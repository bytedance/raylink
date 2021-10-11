from .san import SelfAwareNode
from .frame import get_logger, set_cur_node
import raylink.constants as kc
import pickle
import uuid


class _TreeNode(object):
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name
        self.nid = str(uuid.uuid4())
        self.alias = ''
        self.meta = {}
        self._children = {}
        self._name_dict = {}
        if self.parent is None:
            self._path = [name]
        else:
            self._path = parent.path().split('/')
            self._path.append(self.name)
            self.parent.add_child(self)

    @staticmethod
    def should_pickle(key):
        if key == 'meta':
            return False
        return True

    def get_name_idx(self, name):
        if name not in self._name_dict:
            self._name_dict[name] = 0
        return f"{name}-{self._name_dict[name]}", self._name_dict[name]

    def add_child(self, child):
        """Add a child node.

        Args:
            child (_TreeNode): The child node to be added
        """
        name = child.name
        self._children[name] = child
        self._name_dict[name.split('-')[0]] += 1

    def get_child(self, name):
        """Get a child node by its name

        Args:
            name (str): Name of the child node

        Returns:
            Tuple(str, _TreeNode): Name and the corresponding child node
        """
        return name, self._children[name]

    def get_children(self):
        return self._children

    def path(self):
        return '/'.join(self._path)

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __getstate__(self):
        return dict((k, v) for (k, v) in self.__dict__.items()
                    if self.should_pickle(k))


class Brain(SelfAwareNode):
    """
    THE BRAIN, connecting all nodes
    """
    TYPE = 'BRAIN'

    def __init__(self, node_cfg):
        SelfAwareNode.__init__(self, node_cfg)
        self.nid_node_map = {}
        self.nid_path_map = {}
        self.path_nid_map = {}
        self.alias_path_map = {}
        self._root = _TreeNode(None, 'BRAIN')
        self.nid_node_map[self._root.nid] = self._root
        self.nid_path_map[self._root.nid] = 'BRAIN'
        self.path_nid_map['BRAIN'] = self._root.nid
        self._name = 'BRAIN'
        self._path = 'BRAIN'
        self._parent_path = ''
        self._BRAIN = self
        self._index = 0
        self._parent = None
        self._nid = self._root.nid

    def _setup_logger(self):
        import raylink.util.log as ul
        logger = self._reconstruct(self.find_path('logger-0'))
        self._logger = ul.LocalLogAPI('BRAIN', 'BRAIN', logger)

    def setup_global(self):
        if self._BRAIN.check_setup('logger-0'):
            self._setup_logger()
        if self._BRAIN.check_setup('storage-0'):
            self._storage = self._reconstruct(self.find_path('storage-0'))
        set_cur_node(self)

    def check_nid(self, nid, exception=False):
        try:
            self.nid_node_map[nid]
            return True
        except:
            if exception:
                raise Exception(f'Invalid node id. nid: {nid}')
            else:
                return False

    def check_alias(self, alias, exception=False):
        try:
            self.alias_path_map[alias]
            return True
        except:
            if exception:
                raise Exception(f'Invalid alias. alias: {alias}, available: '
                                f'{list(self.alias_path_map.keys())}')
            else:
                return False

    def check_path(self, path, exception=False):
        path = self._absolute_path(path)
        try:
            self.path_nid_map[path]
            return True
        except:
            if exception:
                raise Exception(f'Invalid node path. path: {path}, available: '
                                f'{list(self.path_nid_map.keys())}')
            else:
                return False

    def check_setup(self, path, exception=False):
        path = self._absolute_path(path)
        try:
            return self.nid_node_map[self.path_nid_map[path]]. \
                meta[kc.META_SETUP]
        except:
            if exception:
                raise Exception(f'Invalid node path. path: {path}, available: '
                                f'{list(self.path_nid_map.keys())}')
            else:
                return False

    def set_alias(self, path, alias):
        self.alias_path_map[alias] = path

    def set_node_meta(self, nid, key, value):
        self.check_nid(nid, True)
        self.nid_node_map[nid].meta[key] = value

    def _get_node_meta(self, nid, key):
        self.check_nid(nid, True)
        return self.nid_node_map[nid].meta[key]

    def get_node_wrapper(self, nid):
        if hasattr(self, '_logger'):
            self._logger.debug(nid)
        self.check_nid(nid, True)
        wrapper = self._get_node_meta(nid, kc.META_WRAPPER)
        offline = self._get_node_meta(nid, kc.META_OFFLINE)
        return wrapper, offline

    def get_path(self, nid):
        if hasattr(self, '_logger'):
            self._logger.debug(nid)
        self.check_nid(nid, True)
        return self._relative_path(self.nid_path_map[nid])

    def _relative_path(self, path):
        if 'BRAIN/' in path:
            path = path.replace('BRAIN/', '')
        return path

    def _absolute_path(self, path):
        if 'BRAIN/' not in path:
            path = 'BRAIN/' + path
        return path

    def _add_one_child(self, p, name):
        child_name, index = p.get_name_idx(name)
        node = _TreeNode(p, child_name)
        node.meta['setup'] = False
        self.nid_node_map[node.nid] = node
        self.nid_path_map[node.nid] = node.path()
        self.path_nid_map[node.path()] = node.nid
        ret = {
            'nid': node.nid, 'name': node.name, 'index': index,
            'path': self._relative_path(node.path())
        }
        return ret

    def add_children(self, p_nid, name, num=1):
        if hasattr(self, '_logger'):
            self._logger.debug(f'{name}, {num}')
        self.check_nid(p_nid, True)
        p = self.nid_node_map[p_nid]
        rets = [self._add_one_child(p, name) for _ in range(num)]
        return rets

    def _sim_add_one_child(self, p, name):
        child_name, index = p.get_name_idx(name)
        node = _TreeNode(p, child_name)
        ret = {
            'nid': node.nid, 'name': node.name, 'index': index,
            'path': self._relative_path(node.path())
        }
        return ret

    def sim_add_children(self, p_nid, name, num=1):
        if hasattr(self, '_logger'):
            self._logger.debug(f'{name}, {num}')
        self.check_nid(p_nid, True)
        sim_nid_node_map = pickle.loads(pickle.dumps(self.nid_node_map))
        p = sim_nid_node_map[p_nid]
        rets = [self._sim_add_one_child(p, name) for _ in range(num)]
        return rets

    def get_children(self, p_nid):
        self.check_nid(p_nid, True)
        p = self.nid_node_map[p_nid]
        children_map = p.get_children()
        children = [self.get_node_wrapper(c.nid)
                    for c in children_map.values()]
        return children

    def find_path(self, path, debug=None):
        """
        Use path to find actual wrapper, which contain actor handler.

        Args:
            path: the path used to get the wrapper

        Returns:
            the wrapper object of the path
        """
        if hasattr(self, '_logger'):
            self._logger.debug(f'find {path} from {debug}')
        path = self._absolute_path(path)
        self.check_path(path, True)
        nid = self.path_nid_map[path]
        return self.get_node_wrapper(nid)

    def find_alias(self, alias):
        """
        Use alias to find actual wrapper, which contain actor handler.

        Args:
            alias: the alias used to get the wrapper

        Returns:
            the wrapper object of the alias
        """
        if hasattr(self, '_logger'):
            self._logger.debug(alias)
        self.check_alias(alias, True)
        path = self.alias_path_map[alias]
        path = self._absolute_path(path)
        self.check_path(path, True)
        nid = self.path_nid_map[path]
        return self.get_node_wrapper(nid)

    def find_nid(self, nid):
        """alias of get_node_wrapper."""
        if hasattr(self, '_logger'):
            self._logger.debug(nid)
        self.check_nid(nid, True)
        return self.get_node_wrapper(nid)

    def set_res_mgr(self, rm):
        self.rm = rm

    def get_res_mgr(self):
        return self.rm

    def _alloc_node_cfg(self, node_type, parent_node_cfg=None):
        if parent_node_cfg is not None:
            node_cfg = self.rm.allocate(node_type, parent_node_cfg)
            return node_cfg
        if node_type in self.rm.node_cfg:
            node_cfg = self.rm.allocate(node_type)
            return node_cfg
        return None

    def alloc_node_cfgs(self, node_type, parent_node_cfg=None, num=1):
        if hasattr(self, '_logger'):
            self._logger.debug(f'{node_type}, {num}')
        node_cfgs = [self._alloc_node_cfg(node_type, parent_node_cfg)
                     for _ in range(num)]
        logger = get_logger()
        if logger is not None:
            logger.debug(f"[rm.allocate('{node_type}', {parent_node_cfg}) "
                         f"for _ in range({num})]"
                         f" -> {node_cfgs}")
        return node_cfgs
