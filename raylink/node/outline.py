import threading
import time
import os

from .san import *
from .frame import *

__all__ = ['OutlineNode']


class OutlineNode(SelfAwareNode):
    TYPE = 'outline'

    def __init__(self, info, parent, node_cfg):
        SelfAwareNode.__init__(self, node_cfg)
        self._info = info
        self._parent = parent
        self._nid = info['nid']
        self._name = info['name']
        self._index = info['index']
        self._path = info['path']
        self._parent_path = os.path.dirname(self._path)
        self._tunnels = {}
        self._pickler = None
        self.__lock = threading.Lock()
        self.__set_global()
        self._setup_tunnel()
        if hasattr(self, '_logger'):
            self._logger.debug(
                f'{self.TYPE}-{self._index} setup complete, '
                f'node info: {self._node_info}, node cfg {self._node_cfg}')

    def _setup_logger(self):
        import raylink.util.log as ul
        logger = self.find_path('logger-0')
        self._logger = ul.RemoteLogAPI(self.name_(), self.path_(), logger)
        self._llogger = ul.LocalLogAPI(self.name_(), self.path_(), logger)

    def __set_global(self):
        BRAIN = get_BRAIN()
        self.set_BRAIN(BRAIN)
        if self._BRAIN.check_setup('logger-0'):
            self._setup_logger()
        if self._BRAIN.check_setup('storage-0'):
            from raylink.data.storage import Storage
            self._storage: Storage = self.find_path('storage-0')
        set_cur_node(self)

    @property
    def _ps(self):
        if hasattr(self, '_ps_api'):
            return self._ps_api
        from raylink.data.ps.ps_api import PS
        from raylink.util.resource import get_identity
        ps_captain = self.find_path('ps-0', f'{self._name}')
        ps_officer_nid = ps_captain.get_officer_t(self.node_cfg_())
        ps_officer = self.find_nid(ps_officer_nid)
        self._logger.debug(f'{get_identity(self.node_cfg_())} '
                           f'connect to {get_identity(ps_officer.node_cfg_())}')
        self._ps_api = PS(self, ps_captain, ps_officer)
        return self._ps_api

    @property
    def children(self):
        return [self._reconstruct(c)
                for c in self._BRAIN.get_children(self._nid)]

    def _lock(self):
        self.__lock.acquire()

    def _unlock(self):
        self.__lock.release()

    def _is_locked(self):
        return self.__lock.locked()

    def join(self):
        """
        This function is used to join actor(s).
        """
        self._lock()
        self._unlock()

    def wrapped_setup(self, **kwargs):
        self._lock()
        self.setup(**kwargs)
        self._unlock()

    def get_path(self, nid):
        """Get the path of a node by its nid

        Args:
            nid (str): The nid of the node

        Returns:
            str: The path of the node
        """
        return self._BRAIN.get_path(nid)

    def find_path(self, path, debug=None):
        """Find the node wrapper by its path.

        Args:
            path (str): Path of the node

        Returns:
            Wrapper: Node wrapper created by `raylink.util.util.wrap_class`
        """
        return self._reconstruct(self._BRAIN.find_path(path, debug))

    def find_alias(self, alias):
        """
        Use alias to find actual wrapper, which contain actor handler.

        Args:
            alias: the alias used to get the wrapper

        Returns:
            the wrapper object of the alias
        """
        return self._reconstruct(self._BRAIN.find_alias(alias))

    def find_nid(self, nid):
        """Find the node wrapper by its nid.

        Args:
            nid (str): Nid of the node

        Returns:
            Wrapper: Node wrapper created by `raylink.util.util.wrap_class`
        """
        return self._reconstruct(self._BRAIN.find_nid(nid))

    def _setup_tunnel(self, tag='common', debug=False):
        from ..data import TunnelServer
        ts = TunnelServer(self, self._pickler, debug)
        ts.start()
        time.sleep(0.1)
        conn_flag = False
        max_retry = 20
        while not conn_flag and max_retry > 0:
            try:
                tunnel_info = ts._get_tunnel()
                proxy = self._get_tunnel_proxy(tunnel_info, local=False)
                conn_flag = True
            except:
                max_retry -= 1
                time.sleep(0.01)
        if not conn_flag:
            import traceback
            traceback.print_exc()
            return
        del proxy
        ts._unlock()
        self._tunnels[tag] = ts
        return tunnel_info

    def get_tunnel(self, tag='common', debug=False):
        """Get or create a tunnel by its name tag.


        Args:
            tag (str): Tag of the tunnel
            debug (bool): Enable debug mode

        Returns:
            TunnelInfo: Info of the tag tunnel
        """
        self._lock()
        if tag in self._tunnels:
            tunnel_info = self._tunnels[tag].get_tunnel()
            self._llogger.debug(f'use exist tunnel for {tag}')
            self._unlock()
            return self._get_tunnel_proxy(tunnel_info)
        tunnel_info = self._setup_tunnel(tag, debug)
        if not tunnel_info:
            self._llogger.debug(f'setup tunnel failed for {tag}')
            self._unlock()
            return
        self._llogger.debug(f'setup tunnel for {tag}')
        self._unlock()
        return self._get_tunnel_proxy(tunnel_info)

    def _get_tunnel_proxy(self, tunnel_info, local=True, debug=False):
        from ..data import TunnelProxy
        return TunnelProxy(tunnel_info, local=local, debug=debug)

    def get_tunnel_info(self, tag='common'):
        """Get tunnel info by its name tag.

        Args:
            tag (str): Name tag of the tunnel

        Returns:
            TunnelInfo: The corresponding tunnel info with respect to the tag
        """
        return self._tunnels[tag].get_tunnel()

    def get_tunnel_stats(self, tag):
        """Get tunnel statistics by the name tag.

        Args:
            tag (str): Name tag of the tunnel

        Returns:
            dict: The corresponding tunnel statistics (e.g., `conn_num`) with respect to the tag
        """
        return self._tunnels[tag].get_tunnel_stats()
