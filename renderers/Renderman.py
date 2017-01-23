import os.path as osp
import os
import logging

import sys
from subprocess import PIPE, Popen
from threading  import Thread, Lock
from datetime import timedelta, datetime 
import time

import glob
import shutil

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

ON_POSIX = 'posix' in sys.builtin_module_names

def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()


class RenderManNode:
    """A container for a single renderman rendering process."""

    def __init__(self, **kwargs):
        self._exec_binary = kwargs['exec']
        self._config_path = kwargs['config_path']
        self._scene_path = kwargs['scene_path']
        self._timeout = kwargs['timeout']
        self._attempts = int(kwargs['attempts'])
        self._rmantree = kwargs['rmantree']
        self._scene = None
        self._frame = None
        self._process = None
        self._logqueue = None 
        self._logthread = None
        self._lastrt = -1
        self._current_log = ""
        self._lastrender = {}
        self._lastrender_files = []
        
        self._timeoutthread = None
        self._timeoutlock = None
        self._currentattempt = 0
        self._jobstart = None
        self._logger = logging.getLogger("renderman")

    def NodeType(self, ):
        return "RENDERMAN"

    def Extension(self, ):
        return "exr"

    def SetScene(self, scene):
        self._scene = scene

    def SetFrame(self, frame):
        self._frame = frame

    def Status(self, ):
        self.CheckStatus();
        if self._process == None and self._lastrt == -1:
            return "STOPPED"
        if self._process == None and self._lastrt == 0:
            return "SUCCESS"
        if self._process == None and self._lastrt != 0:
            return "FAILURE"
        if self._process != None:
            return "RUNNING"

        return "UNKNOWN"

    def Log(self, ):
        if self._logqueue != None:
            lines = []
            while True:
                line = ""
                try: line = self._logqueue.get_nowait()
                except Empty:
                    break
                else:
                    lines.append( line )
            return "\n".join( lines )
        else:
            return ""
                

    def SanitizeRIB( self, rib_file ):        
        rib_data = []
        renders = []
        extra_display = False;
        for line in rib_file:
            line = line.strip()
            if line.startswith( "Display " ):
                parts = line.strip().split()
                image_filename = osp.basename( parts[1].strip('"') )
                renders.append( image_filename )
                if extra_display:
                    parts[1] = '"+{:s}"'.format( osp.join( "/tmp", image_filename ) )
                else:
                    parts[1] = '"{:s}"'.format( osp.join( "/tmp", image_filename ) )
                    extra_display = True
                new_line = " ".join( parts )
                rib_data.append( new_line )
            else:
                rib_data.append( line )
        return ["\n".join( rib_data ), renders]
                
    def BeginRender(self, ):
        self.StopRender();        
        self._current_log = ""      

        rib_data = ""
        renders = []
        try:
            rib_file = open( osp.join( self._scene_path, self._scene, 'Scene.{:04d}.rib'.format(self._frame)), 'r' )
            rib_data, renders = self.SanitizeRIB( rib_file )
        except:
            pass
        
        self._lastrender_files = renders
        read, write = os.pipe()
        os.write(write, rib_data)
        os.close(write)

        # Remove the image file that we will be producing to eliminate false positives
        self._process = Popen( [self._exec_binary,
                                "-cwd", osp.join( self._scene_path, self._scene),
                                "-Progress",
                                "-loglevel", "4",
                                "-"    
        ],
                               stdin=read,
                               stdout=PIPE, 
                               stderr=PIPE,
                               env = dict( os.environ,
                                           RENDERMAN_USER_CONFIG=self._config_path,
                                           RMANTREE=self._rmantree,
                                           TMP = "/tmp" ),
                           );
        #self._process = Popen( ["/bin/true"], stdout=PIPE, stderr=PIPE )
        
        self._logqueue = Queue()
        self._logthread = Thread( target=enqueue_output, args=(self._process.stdout, self._logqueue ) )
        self._logthread.daemon = True
        self._logthread.start()
        self._jobstart = datetime.now()
        self._currentattempt = self._currentattempt + 1
        
    def RestartRender(self, ):
        self.StopRender();
        self.BeginRender();
                                                      
    def StopRender(self, ):
        if self._process != None:
            self._process.kill()
            self._process.wait()            
            self._lastrt = self._process.returncode
            self._logthread.join()
            self._logthread = None
            self._process = None

    def job_failed(self, ):
        if self._currentattempt < self._attempts:
            self._logger.info(  "Restarting job for attempt {:d}".format(self._currentattempt + 1) );
            self.RestartRender();
        else:
            self._logger.info("Terminating job due to excessive failures." )
            self.StopRender()
            self._currentattempt = 0
            self._lastrt = 1

    def job_success(self, ):
        self._lastrender = {}
        for filename in self._lastrender_files:
            parts = filename.split('.')
            clean_parts = []
            for p in parts:
                if p == "Scene":
                    continue;
                try:
                    test = int(p)
                except:
                    pass
                else:
                    continue
                if p == "exr":
                    continue
                clean_parts.append( p )
            
            label = 'render'            
            if  len(clean_parts) > 0 :
                label = ".".join(clean_parts)
                
            with open( "/tmp/{:s}".format(filename), 'rb' ) as f:
                self._lastrender[label] = f.read()
                
            try:
                os.remove( "/tmp/{:s}".format(filename) );
            except: 
                self._logger.warning( "Failed to remove temporary render result." )               
        
        self._currentattempt = 0
        self._lastrt = 0
        self.StopRender()
        
    def check_file_for_success(self,):
        self.job_failed();                    


    def CheckStatus(self, ):
        if self._process != None:
            self._process.poll()
            self._lastrt = self._process.returncode
            if self._lastrt != None:
                self._logthread.join()
                self._logthread = None
                self._process = None
                if self._lastrt == 0:
                    self.job_success();
                else:
                    self._logger.info(  "Render job failed with code {:d}".format(self._lastrt) )
                    self.check_file_for_success();
            else:
                if self._timeout >= 0:
                    # Check the timeout
                    time_now = datetime.now()
                    time_running = (time_now - self._jobstart).total_seconds();
                    if time_running > self._timeout: # We have exceeded timeout
                        self._logger.info(  "Render job timeout exceeded," );
                        self.check_file_for_success();                           

            
    def LastRender(self, ):
        return self._lastrender


def BuildRenderer(kwargs):
    return RenderManNode(**kwargs)


    
def __selftest__():
    bn = RendermanNode("renderman","/tmp/")
    bn.SetScene( "/tmp/test.blend" )
    bn.SetFrame( 1 )
    bn.BeginRender()
    while True:
        status = bn.Status()
        if status != "RUNNING":
            break;
    print "Render Finished with status: " + bn.Status()
    print "Render Log: "
    print bn.Log()
    if bn.Status() == "SUCCESS":
        output = open( "./render.png", 'wb' )
        output.write( bn.LastRender() )
        output.close()

if __name__ == "__main__":
    __selftest__()
    
