import os.path as osp
import os

import sys
from subprocess import PIPE, Popen
from threading  import Thread, Lock
from datetime import timedelta, datetime 
import time

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

ON_POSIX = 'posix' in sys.builtin_module_names

def enqueue_output(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()


class BlenderNode:
    """A container for a single blender rendering process."""

    def __init__(self, blender_exec, config_path, scene_path, timeout=-1, attempts=1):       
        self._exec_binary = blender_exec
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

    def NodeType(self, ):
        return "BLENDER"

    def Extension(self, ):
        return "png"

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
	with open("/tmp/seed_script.py",'w') as seedscript:
	    seedscript.writelines(["import bpy\n",
                                   "import time\n",
                                   "seed = int(time.time())\n",
                                   "for scene in bpy.data.scenes:\n",
                                   "    scene.cycles.seed = seed\n" ])

        self._process = Popen( [self._exec_binary,
                                "-b",
                                osp.join( self._scene_path, self._scene, 'scene.blend' ),
                                "-y", "-P", "/tmp/seed_script.py",
                                "-noaudio",
                                "-o", "/tmp/Renders/render_########",
                                "-F", "PNG",
                                "-f", str(self._frame) ],
                               stdout=PIPE, 
                               stderr=PIPE,
                               env = dict( os.environ,
                                           BLENDER_USER_CONFIG=self._config_path,
                                           TMP = "/tmp" ),
                               );
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

    def CheckStatus(self, ):
        if self._process != None:
            self._process.poll()
            self._lastrt = self._process.returncode
            if self._lastrt != None:
                self._logthread.join()
                self._logthread = None
                self._process = None
                if self._lastrt == 0:
                    with open( "/tmp/Renders/render_{:08d}.png".format(self._frame), 'rb' ) as f:
                        self._lastrender = f.read()
                    self._currentattempt = 0
                else:
                    print "Render job failed with code", str(self._lastrt), ",",
                    if self._currentattempt < self._attempts:
                        print "restarting job for attempt", str(self._currentattempt + 1)
                        self.BeginRender();
                    else:
                        print "terminating job due to excessive failures."
                        self._currentattempt = 0
                        self.StopRender()
                    
            else:
                if self._timeout >= 0:
                    # Check the timeout
                    time_now = datetime.now()
                    time_running = (time_now - self._jobstart).total_seconds();
                    if time_running > self._timeout: # We have exceeded timeout
                        print "Render job timeout exceeded,",
                        if self._currentattempt < self._attempts:
                            print "restarting job for attempt", str(self._currentattempt + 1)
                            self.StopRender()
                            self.BeginRender()
                        else:
                            print "terminating job due to excessive failures."
                            self.StopRender()
                            self._currentattempt = 0
                            self._lastrt = 1
                            


            
    def LastRender(self, ):
        return self._lastrender


def __selftest__():
    bn = BlenderNode("blender","/tmp/")
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

