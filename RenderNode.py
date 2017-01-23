#!/usr/bin/env python
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pika').setLevel(logging.INFO)
LOG_FORMAT = ('%(levelname) -15s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')

LOGGER = logging.getLogger("Manager")
NODE_LOGGER = logging.getLogger("Process")

import pika
import time
import datetime
import subprocess
import json
import uuid
import socket
import sys
from requests_toolbelt import MultipartEncoder
import requests
import argparse
from node import RenderNode
import os 
import traceback
import socket
import ConfigParser
import importlib
import io
import struct

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            encoded_object = list(obj.timetuple())[0:6]
        else:
            encoded_object =json.JSONEncoder.default(self, obj)
        return encoded_object

class RenderWorker(object):
    
    def __init__(self, comm_host, config ):
        self._comm_host = comm_host
        self._murl = config.get( 'settings', 'manager_url' );

        # These are the communication members
        self._connection = None
        self.channel = None
        self.active_queue = ""
        self.active_queue_tag = None
        self.queue_was_filled = False
        
        # Load Renderer Modules
        self._renderManager = RenderNode();
        for module in config.options('modules'):
            status = config.getboolean( 'modules', module )
            if status:
                print "Loading render module", module, "..."
                mod = importlib.import_module(config.get(module,'module'))
                renderer_args = dict( config.items(module))
                renderer_args["scene_path"] = config.get('settings', 'scene_path' );
                renderer = mod.BuildRenderer( renderer_args )
                self._renderManager.register_renderer(renderer.NodeType(), renderer )

        # periodic checks
        self.last_render_check = datetime.datetime.now()
        self.render_started = False
        self._save_location = config.get( 'settings', 'save_cache_path' );

    def initiate_broker_communications(self, ):
        self._connection = None
        while self._connection == None:
            try:
                LOGGER.info( "Connecting to Broker..." )
                parameters = pika.URLParameters(self._comm_host)
                self._connection = pika.BlockingConnection(parameters)
            except Exception, e:
                LOGGER.info(str(e))
                LOGGER.info(traceback.format_exc())
                time.sleep(5)

        self.channel = self._connection.channel()

        self.channel.queue_declare(queue='log_queue',
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=False)
                                   
        LOGGER.info(' [*] Waiting for messages. To exit press CTRL+C')

        self.channel.basic_qos(prefetch_count=1)


    def spin_up_new_pid(self,config):
        pass

    def kill_pid(self, ):
        pass

    def check_pid_status(self, ):
        pass

    def send_status_update(self, ):
        pass


    def check_render(self, ):
        self.last_render_check = datetime.datetime.now()
        if self.render_started:
            status = self._renderManager.status();
            log_text = self._renderManager.last_log()
            for line in log_text.split("\n"):
                if line:
                    NODE_LOGGER.debug( line )

            if status == "SUCCESS":
                LOGGER.info("Render complete.")
                render_info = self._renderManager.last_render_info()
                self.channel.basic_publish(exchange='', routing_key="log_queue",
                                           body = json.dumps( {"event":"render_finish",
                                                               "frame":render_info["frame"],
                                                               "scene":render_info["scene"],
                                                               "time":datetime.datetime.now(),
                                                               "uuid":render_info["uuid"],
                                                               "type":render_info["type"],
                                                               } , cls=DateTimeEncoder),
                                           properties=pika.BasicProperties(
                                               delivery_mode = 2,
                                               app_id = socket.gethostname(),
                                           )                                           
                                           )
                
                # Save off the render to disk somewhere
                url = self._murl+"/upload_render?uuid="+render_info["uuid"]
                LOGGER.info("Uploading completed render to %s" % url)
                last_render = self._renderManager.last_render()

                all_files_sent = True
                for label in last_render.keys():
                    file_list = {}
                    file_list[label] = ( label+"."+self._renderManager.extension(), last_render[label] )
                    print "Sending image {:s}".format( label+"."+self._renderManager.extension() ) 
                    try:
                        requests.post( url, files=file_list)
                    except Exception, e:
                        LOGGER.warning("Failed to upload the render %s: %s" % (label, str(e)) )
                        all_files_sent = False

                if all_files_sent:
                    self.channel.basic_ack(delivery_tag = self._renderManager.tag)
                else:
                    self.channel.basic_reject(delivery_tag=self._renderManager.tag, requeue=True);

                        
                self.render_started = False
            elif status == "FAILURE":
                LOGGER.info("Render failed.")
                render_info = self._renderManager.last_render_info()
                self.channel.basic_publish(exchange='', routing_key="log_queue",
                                           body = json.dumps( {"event":"render_fail",
                                                               "frame":render_info["frame"],
                                                               "scene":render_info["scene"],
                                                               "time":datetime.datetime.now(),
                                                               "uuid":render_info["uuid"],
                                                               "type":render_info["type"],
                                                               } , cls=DateTimeEncoder),
                                           properties=pika.BasicProperties(
                                               delivery_mode = 2,
                                               app_id = socket.gethostname(),
                                           )
                                           ) 
                self.render_started = False
                self.channel.basic_ack(delivery_tag = self._renderManager.tag)
            else:
                pass
            
    def pull_and_update_queue(self,):       
        url = self._murl+"/available_jobs"
        print url
        try:
            res = requests.get( url )
        except Exception, e:
            LOGGER.warning("Failed to retreieve job priority list from manager: %s", str(e))               
        else:
            if len(res.json()) == 0 and self.active_queue != "":
                LOGGER.info("No jobs available. Disconnecting from the last queue.")
                if self.active_queue_tag != None:
                    self.channel.basic_cancel( None, self.active_queue_tag )
                self.active_queue = ""
                
            for job in res.json():
                if self._renderManager.can_handle_render( job[1] ):
                    if self.active_queue != job[0]:
                        LOGGER.info( "New job has priority. Switching to queue: render_%s" % job[0] )
                        if self.active_queue_tag != None:
                            self.channel.basic_cancel( None, self.active_queue_tag )
                        self.active_queue = job[0]
                        self.active_queue_tag = self.channel.basic_consume(self.callback, queue='render_%s' % self.active_queue )
                    break;
                else:
                    print "Can't handle render jobs of type '%s', skipping to next job in queue..." % job[1]

                
        
            
    def callback(self, ch, method, properties, body):
        response_message = {}
        response_message["status"] = ""

        body_config = dict()
        try:
            body_config = json.loads( body );
        except:
            LOGGER.error("Failed to parse command: %s", str(body))
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return;       

        if body_config["command"] == "render":
            self.check_render();

            if self._renderManager.status() != "RUNNING":
                LOGGER.info("Caught a render job...")
                try:
                    frame = body_config["frame"]
                    scene_file = body_config["scene"]
                    rendertype = body_config["type"]
                    uuid = body_config["uuid"]
                except:
                    LOGGER.error("Render command was malformed. Discarding...")
                    ch.basic_ack(delivery_tag = method.delivery_tag)                
                else:
                    if not self._renderManager.can_handle_render(rendertype):
                        ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True);
                    else:
                        #ch.basic_ack(delivery_tag = method.delivery_tag)
                        LOGGER.info("Rendering frame %s for scene %s of type %s", str(frame), scene_file, rendertype);
                        self._renderManager.render_single_frame_of_type( scene_file, frame, uuid, rendertype );
                        self._renderManager.tag = method.delivery_tag
                        self.render_started = True
                        self.channel.basic_publish(exchange='', routing_key="log_queue",
                                                   body = json.dumps( {"event":"render_start",
                                                                       "frame":frame,
                                                                       "scene":scene_file,
                                                                       "time":datetime.datetime.now(),
                                                                       "uuid":uuid,
                                                                       "type":rendertype,
                                                                       } , cls=DateTimeEncoder),
                                                   properties=pika.BasicProperties(
                                                       delivery_mode = 2,
                                                       app_id = socket.gethostname(),
                                                   )
                                                   )                                           
            else:
                ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True);

            
    def run(self, ):

        self.initiate_broker_communications()
        self.send_status_update();

        while True:
            try:
                self._connection.process_data_events();
            except pika.exceptions.ConnectionClosed, e:
                LOGGER.warning("Lost connection to management, killing any active processes")
                self.kill_pid()
                LOGGER.warning("Connection lost. Reconnecting...")
                self.initiate_broker_communications()
            except KeyboardInterrupt:
                LOGGER.info("Recieved kill command from terminal, shutting down.")
                self.check_render()
                self.kill_pid()
                break;

            self.check_pid_status();
                
            #if (datetime.now() - self.last_update).seconds >= 120:
            #    self.send_status_update();

            
            if (datetime.datetime.now() - self.last_render_check).seconds >= 5:
                self.check_render()
                sys.stdout.flush()
                sys.stderr.flush()
                if not self.render_started:
                    self.pull_and_update_queue()

                
            time.sleep(.5);



def valid_path(path):
    if os.path.isdir( path ):
        return path
    else:
        msg = '"%s" is not a valid path name.' % path 
        raise argparse.ArgumentTypeError(msg)
    
def valid_file(path):
    if os.path.isfile( path ):
        return path
    else:
        msg = '"%s" is not a valid file name.' % path 
        raise argparse.ArgumentTypeError(msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plumage Render Node")

    parser.add_argument("-c", "--conf_file", type=valid_file,
                        help="Specify config file", metavar="FILE")
    
    args, remaining_argv = parser.parse_known_args()
    defaults = {
        "ampq_server" : "http://localhost:5672",
        "ampq_user" : "guest",
        "ampq_password": "guest",
        "save_cache_path":"/tmp",
        "manager_url":"http://localhost:8888",
        "scene_path":"/tmp",
    }
    
    config = ConfigParser.SafeConfigParser()
    if args.conf_file:
        config.read([args.conf_file])
        defaults = dict(config.items("settings"))
        
    parser.set_defaults(**defaults)    
    parser.add_argument('-a','--ampq_server', type=str )
    parser.add_argument('-U','--ampq_user', type=str )
    parser.add_argument('-P','--ampq_password', type=str )
    parser.add_argument('-S','--save_cache_path', type=valid_path)
    parser.add_argument('-m','--manager_url', type=str)
    parser.add_argument('-s','--scene_path', type=valid_path )

    args = parser.parse_args(remaining_argv)
   
    if not config.has_section('settings'):
        config.add_section('settings');
    print vars(args)
    for item, value in vars(args).items():
        config.set( 'settings', item, str(value) );
    
    ampq_url = 'http://%s:%s@%s'%(config.get('settings','ampq_user'),
                                  config.get('settings','ampq_password'),
                                  config.get('settings','ampq_server') );
    
    print "AMPQ:", ampq_url
    
    worker = RenderWorker( ampq_url, config )
    worker.run();
