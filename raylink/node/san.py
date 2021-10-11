__all__ = ['offlinemethod', 'SelfAwareNode']

import pickle
import copy
import sys

import raylink.util.util as u
import raylink.constants as kc


class offlinemethod(object):
    def __init__(self, method):
        self.method = method
        self.__name__ = method.__name__
        self.__doc__ = method.__doc__

    def __set_name__(self, owner, name):
        _offline_methods = copy.deepcopy(owner._offline_methods)
        _offline_methods[self.method.__name__] = self.method
        owner._offline_methods = _offline_methods
        setattr(owner, name, self.method)


class SelfAwareNode(object):
    TYPE = 'SAN'
    _OFFLINE_ATTR = {'_nid', '_name', '_index', '_path', '_parent_path',
                     '_ip', '_pid', '_node_cfg', '_node_info',
                     '_ah', '_offline_methods'}
    _offline_methods = {}

    def __init__(self, node_cfg):
        self._node_cfg = node_cfg
        self._setup_offline()

    def _setup_offline(self):
        self._ip = u.get_ip()
        self._pid = u.get_pid()
        self._node_info = {'ip': self._ip, 'pid': self._pid}

    def update_offline(self):
        """Offline attribute and method can not be changed after setup."""
        p = {}
        for _p in list(self._OFFLINE_ATTR):
            if hasattr(self, _p):
                p[_p] = getattr(self, _p)
            else:
                p[_p] = None
                print(f'Can not find {_p} in {self.TYPE}', file=sys.stderr)
        self._BRAIN.set_node_meta(self._nid, kc.META_OFFLINE, pickle.dumps(p))
        return p

    def add_offline_attr(self, name):
        self._OFFLINE_ATTR.add(name)

    def setup(self, **kwargs):
        pass

    def _reconstruct(self, data):
        from .node import _reconstruct_offline
        wrapper, offline = data
        h = _reconstruct_offline(pickle.loads(wrapper), pickle.loads(offline))
        return h

    @offlinemethod
    def name_(self):
        """Get the name of this node, e.g. 'BRAIN', 'learner-0', etc.

        Returns:
            str: self._name
        """
        return self._name

    @offlinemethod
    def path_(self):
        """Get the path

        Returns:
            str: self._path
        """
        return self._path

    @offlinemethod
    def index_(self):
        """Get the index

        Returns:
            int: self._index
        """
        return self._index

    @offlinemethod
    def parent_path_(self):
        """Get the path of the parent.

        Returns:
            str: The path of self's parent
        """
        return self._parent_path

    @offlinemethod
    def info_(self):
        """Get IP and PID.

        Returns:
            dict: A dict contains 'ip' and 'pid'
        """
        return self._info

    @offlinemethod
    def ip_(self):
        """Get IP address.

        Returns:
            str: IP address
        """
        return self._ip

    @offlinemethod
    def pid_(self):
        """Get PID.

        Returns:
            str: PID
        """
        return self._pid

    @offlinemethod
    def nid_(self):
        """
        Get node id

        Returns:
            node id

        """
        return self._nid

    def parent(self):
        return self._parent

    def ping(self):
        """
        This function can be used to check latency,
        and it also can be used to check whether a node is idling.
        """
        return 'pong'

    def set_ah(self, ah):
        """
        Set actor handler, created by ray.

        Args:
            ah: actor handler

        """
        self._ah = ah

    @offlinemethod
    def ah_(self):
        """
        Get actor handler

        Returns:
            actor handler

        """
        return self._ah

    @offlinemethod
    def wrapper_(self, update=False):
        """
        Get wrapper object

        Returns:
            wrapper object

        """
        if not hasattr(self, '_wrapper') or update:
            self._wrapper = self._reconstruct(
                self._BRAIN.get_node_wrapper(self._nid))
        return self._wrapper

    @offlinemethod
    def node_cfg_(self):
        return self._node_cfg

    def set_BRAIN(self, BRAIN):
        self._BRAIN = BRAIN

    def get_BRAIN(self):
        return self._BRAIN

    def set_attr(self, attr, value):
        """Reserve method to set attribute remotely"""
        setattr(self, attr, value)

    def call_methods(self, kwargs: dict):
        """Reserve method to call methods remotely"""
        returns = {}
        for method, kwarg in kwargs.items():
            r = getattr(self, method)(**kwarg)
            returns[method] = r
        return returns

    def get_cache(self, key):
        if not hasattr(self, '_cache'): self._cache = {}
        return self._cache.get(key, None)

    def set_cache(self, key, value):
        if not hasattr(self, '_cache'): self._cache = {}
        self._cache[key] = value
