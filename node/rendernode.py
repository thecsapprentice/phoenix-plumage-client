import os.path as osp
import uuid

class RenderNode:
    """A generic container for managing a single instance of a renderer."""

    _current_engine = ""
    _render_engines = dict()
    _last_render_info = dict()


    def __init__(self):
        pass

    def register_renderer(self, key, engine):
        self._render_engines[key] = engine

    def set_active_engine(self, key ):
        self._active_engine = key

    def render_single_frame(self, scene_file, frame_number, uuid ):
        self._render_engines[self._active_engine].SetScene( scene_file )
        self._render_engines[self._active_engine].SetFrame( frame_number )
        self._render_engines[self._active_engine].BeginRender();
        
        self._last_render_info["frame"] = frame_number
        self._last_render_info["scene"] = scene_file
        self._last_render_info["uuid"] = uuid

    def status(self, ):
        s = self._render_engines[self._active_engine].Status();
        return s;
    
    def last_log(self, ):
        log = self._render_engines[self._active_engine].Log();
        return log

    def last_render(self, ):
        return self._render_engines[self._active_engine].LastRender()
                                                                          

    
