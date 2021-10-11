__all__ = ['get_BRAIN', 'set_cur_node', 'get_cur_node',
           'set_storage', 'get_storage', 'set_logger', 'get_logger',
           'set_llogger', 'get_llogger']

import ray


class Frame(object):
    def __init__(self):
        self.BRAIN = None
        self.cur_node = None
        self.storage = None
        self.logger = None
        self.llogger = None
        self.ps = None
        self.proxies = {}
        self.nodes = {}


frame = Frame()


def get_BRAIN():
    if frame.BRAIN is None:
        from .brain import Brain
        import raylink.util.util as u
        from .node import _reconstruct_offline
        ah = ray.get_actor('BRAIN')
        wrapped_class = u.wrap_class(Brain)
        wrapper = wrapped_class(ah)
        offline = wrapper.update_offline()
        wrapper = _reconstruct_offline(wrapper, offline)
        wrapper._BRAIN = wrapper
        frame.BRAIN = wrapper
    return frame.BRAIN


def set_cur_node(node):
    frame.cur_node = node
    if hasattr(node, '_storage'):
        frame.storage = node._storage
    if hasattr(node, '_logger'):
        frame.logger = node._logger
    if hasattr(node, '_llogger'):
        frame.llogger = node._llogger


def get_cur_node():
    return frame.cur_node


def set_storage(node):
    frame.storage = node


def get_storage():
    return frame.storage


def set_logger(node):
    frame.logger = node


def get_logger():
    return frame.logger


def set_llogger(node):
    frame.llogger = node


def get_llogger():
    return frame.llogger


def _set_ps(node):
    frame.ps = node


def _get_proxy(actor):
    if id(actor) not in frame.proxies:
        proxy = ray.get(actor.get_tunnel.remote())
        frame.proxies[id(actor)] = proxy
    return frame.proxies[id(actor)]


def _set_node(key, node):
    frame.nodes[key] = node
