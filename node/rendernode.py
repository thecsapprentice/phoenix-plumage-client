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

    def can_handle_render(self, key):
        if key in self._render_engines:
            return True
        else:
            return False

    def render_single_frame( self, scene_file, frame_number, uuid ):
        self.render_single_frame_of_type( scene_file, frame_number, uuid, self._active_engine)
        
    def render_single_frame_of_type(self, scene_file, frame_number, uuid, render_type ):
        self.set_active_engine( render_type )
        self._render_engines[render_type].SetScene( scene_file )
        self._render_engines[render_type].SetFrame( frame_number )
        self._render_engines[render_type].BeginRender();
        
        self._last_render_info["frame"] = frame_number
        self._last_render_info["scene"] = scene_file
        self._last_render_info["uuid"] = uuid
        self._last_render_info["type"] = render_type
        
    def status(self, ):
        s = self._render_engines[self._active_engine].Status();
        return s;
    
    def last_log(self, ):
        log = self._render_engines[self._active_engine].Log();
        return log

    def last_render(self, ):
        return self._render_engines[self._active_engine].LastRender()
                                                                          
    def last_render_info(self):
        return self._last_render_info

    
