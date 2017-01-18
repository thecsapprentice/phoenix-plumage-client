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

    def __init__(self, renderman_exec, config_path, scene_path, timeout=-1, attempts=1):       
        self._exec_binary = renderman_exec
        self._config_path = config_path
        self._scene_path = scene_path
        self._timeout = timeout
        self._attempts = attempts
        self._scene = None
        self._frame = None
        self._process = None
        self._logqueue = None 
        self._logthread = None
        self._lastrt = -1
        self._current_log = ""
        self._lastrender = bytes([])

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
                
                
    def BeginRender(self, ):
        self.StopRender();        
        self._current_log = ""      

        # Remove the image file that we will be producing to eliminate false positives
        #self._process = Popen( [self._exec_binary,
        #                        osp.join( self._scene_path, self._scene, 'scene.rib' ),
        #                    ],
        #                       stdout=PIPE, 
        #                       stderr=PIPE,
        #                       env = dict( os.environ,
        #                                   RENDERMAN_USER_CONFIG=self._config_path,
        #                                   TMP = "/tmp" ),
        #                   );
        self._process = Popen( ["/bin/true"], stdout=PIPE, stderr=PIPE )
        
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
            self._logger.info(  "Restarting job for attempt", str(self._currentattempt + 1) );
            self.RestartRender();
        else:
            self._logger.info("Terminating job due to excessive failures." )
            self.StopRender()
            self._currentattempt = 0
            self._lastrt = 1

    def job_success(self, ):
        self._currentattempt = 0
        self._lastrt = 0
        self.StopRender()
        
    def check_file_for_success(self,):
        if osp.exists( "/tmp/Renders/render_{:08d}.png".format(self._frame) ) :
            self._logger.info(  "Found rendered image despite renderman failure. Considering job successful." )
            self.job_success();
        else:
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
    
