#!/usr/bin/env python
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('pika').setLevel(logging.INFO)
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


import pika
import time
import datetime
import subprocess
import json
import uuid
import socket
import sys
import requests
import argparse
from node import RenderNode
from node import BlenderNode
import os 
import traceback
import socket

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            encoded_object = list(obj.timetuple())[0:6]
        else:
            encoded_object =json.JSONEncoder.default(self, obj)
        return encoded_object

class RenderWorker(object):
    
    def __init__(self, comm_host, blender_exec, blender_config, save_loc, manager_url, scene_path, **kwargs):
        self._comm_host = comm_host
        self._murl = manager_url

        # These are the communication members
        self._connection = None
        self.channel = None

        # Replace these later
        bn = BlenderNode( blender_exec, blender_config, scene_path, kwargs["timeout"], kwargs["attempts"] )
        self._renderManager = RenderNode();
        self._renderManager.register_renderer("BLENDER", bn );
        self._renderManager.set_active_engine( "BLENDER" )

        # periodic checks
        self.last_render_check = datetime.datetime.now()
        self.render_started = False
        self._save_location = save_loc

    def initiate_broker_communications(self, ):
        self._connection = None
        while self._connection == None:
            try:
                print "Connecting to Broker..."
                parameters = pika.URLParameters(self._comm_host)
                self._connection = pika.BlockingConnection(parameters)
            except Exception, e:
                print e 
                print(traceback.format_exc())
                time.sleep(5)

        self.channel = self._connection.channel()

        self.channel.queue_declare(queue='render_queue',
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=False)
        self.channel.queue_declare(queue='log_queue',
                                   durable=True,
                                   exclusive=False,
                                   auto_delete=False)
                                   
        print ' [*] Waiting for messages. To exit press CTRL+C'

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.callback,
                                   queue='render_queue')
        
#    def QueryJobPriority(self,):
#        
#        try:
#            r = requests.get(self._murl+"/job_priority")
#        except: Exception, e:
#            print "Failed to get a job priority list from the server. Trying later."
#        if r.status_code != requests.codes.ok:
#            print "Failed to get a job priority list from the server. Trying later."
            


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
            print self._renderManager.last_log(),

            if status == "SUCCESS":
                print "Render complete."
                self.channel.basic_publish(exchange='', routing_key="log_queue",
                                           body = json.dumps( {"event":"render_finish",
                                                               "frame":self._renderManager._last_render_info["frame"],
                                                               "scene":self._renderManager._last_render_info["scene"],
                                                               "time":datetime.datetime.now(),
                                                               "uuid":self._renderManager._last_render_info["uuid"],
                                                               "type":"BLENDER",
                                                               } , cls=DateTimeEncoder),
                                           properties=pika.BasicProperties(
                                               delivery_mode = 2,
                                               app_id = socket.gethostname(),
                                           )                                           
                                           )
                
                # Save off the render to disk somewhere
                url = self._murl+"/upload_render?uuid="+self._renderManager._last_render_info["uuid"]
                print "Uploading completed render to %s" % url
                try:
                    requests.post( url, files={ 'render' : ("render.png", self._renderManager.last_render() ) })
                except Exception, e:
                    print "Failed to upload the final render: %s", str(e)
                    self.channel.basic_reject(delivery_tag=self._renderManager.tag, requeue=True);
                else:
                    ##self._renderManager.save_last_render( self._save_location );
                    self.channel.basic_ack(delivery_tag = self._renderManager.tag)
                self.render_started = False
            elif status == "FAILURE":
                print "Render failed."
                self.channel.basic_publish(exchange='', routing_key="log_queue",
                                           body = json.dumps( {"event":"render_fail",
                                                               "frame":self._renderManager._last_render_info["frame"],
                                                               "scene":self._renderManager._last_render_info["scene"],
                                                               "time":datetime.datetime.now(),
                                                               "uuid":self._renderManager._last_render_info["uuid"],
                                                               "type":"BLENDER",
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
                #print status

    def callback(self, ch, method, properties, body):
        response_message = {}
        response_message["status"] = ""

        print "Consuming: ", body

        body_config = dict()
        try:
            body_config = json.loads( body );
        except:
            print "Failed to parse command: "
            print body
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return;       

        if body_config["command"] == "render":
            self.check_render();

            if self._renderManager.status() != "RUNNING":
                print "Caught a render job..."
                try:
                    frame = body_config["frame"]
                    scene_file = body_config["scene"]
                    rendertype = body_config["type"]
                    uuid = body_config["uuid"]
                except:
                    print "Render command was malformed. Discarding..."
                    ch.basic_ack(delivery_tag = method.delivery_tag)                
                else:
                    if rendertype != "BLENDER":
                        ch.basic_reject(delivery_tag = method.delivery_tag, requeue=True);
                    else:
                        #ch.basic_ack(delivery_tag = method.delivery_tag)
                        print "Rendering frame", str(frame), " for scene", scene_file
                        self._renderManager.render_single_frame( scene_file, frame, uuid );
                        self._renderManager.tag = method.delivery_tag
                        self.render_started = True
                        self.channel.basic_publish(exchange='', routing_key="log_queue",
                                                   body = json.dumps( {"event":"render_start",
                                                                       "frame":frame,
                                                                       "scene":scene_file,
                                                                       "time":datetime.datetime.now(),
                                                                       "uuid":uuid,
                                                                       "type":"BLENDER",
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
                print "Lost connection to management, killing any active processes"
                self.kill_pid()
                print "Connection lost. Reconnecting..."
                self.initiate_broker_communications()
            except KeyboardInterrupt:
                print "Recieved kill command from terminal, shutting down."
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



def valid_path(path):
    if os.path.isdir( path ):
        return path
    else:
        msg = '"%s" is not a valid path name.' % path 
        raise argparse.ArgumentTypeError(msg)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plumage Render Node")
    parser.add_argument('-a','--ampq_server', default='localhost:5672', type=str )
    parser.add_argument('-U','--ampq_user', default='guest', type=str )
    parser.add_argument('-P','--ampq_password', default='guest', type=str )
    parser.add_argument('-e','--blender_exec', default='blender', type=str )
    parser.add_argument('-C','--blender_cache', default='/tmp', type=valid_path)
    parser.add_argument('-S','--save_cache_path', default='/tmp', type=valid_path)
    parser.add_argument('-m','--manager_url', default='localhost', type=str)
    parser.add_argument('-s','--scene_path', default='/var/plumage/scenes', type=valid_path )
    parser.add_argument('-t','--timeout', default=3600, help="Number of seconds to wait before retrying a frame.",  type=int )
    parser.add_argument('--attempts', default=3, help="Number of attempts before giving up on a frame.", type=int)

    args = parser.parse_args()

    ampq_url = 'http://%s:%s@%s'%(args.ampq_user,args.ampq_password,args.ampq_server)
    print "AMPQ:", ampq_url
    
    worker = RenderWorker( ampq_url, args.blender_exec, args.blender_cache,  args.save_cache_path, args.manager_url, args.scene_path, timeout=args.timeout, attempts=args.attempts );
    worker.run();
