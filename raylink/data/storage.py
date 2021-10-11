__all__ = ['Storage']

from raylink.util.path import get_path
from urllib import parse as up
import raylink.constants as kc
import pickle
import shutil
import raylink
import uuid
import os


def get_vars_path(base_dir):
    return os.path.join(base_dir, 'vars')


class Storage(raylink.OutlineNode):
    TYPE = 'storage'

    def setup(self):
        """Setup storage."""

        self._storage = {}
        self._setup_logger()
        self.vars_dir = get_path(get_vars_path(self._logger.base_dir))

    def _gen_id(self):
        return str(uuid.uuid4())

    def has_key(self, key):
        self._llogger.debug(f'has_key {key}')
        return key in self._storage

    def put(self, key, value):
        self._llogger.debug(f'put {key} {value}')
        _id = self._gen_id()
        self._storage[key] = {'id': _id, 'v': value}
        return _id, value

    def get(self, key, default=None):
        self._llogger.debug(f'get {key}')
        if key not in self._storage:
            return default
        return self._storage[key]['v']

    def get_id(self, key):
        self._llogger.debug('get_id')
        return self._storage[key]['id']

    def incr(self, key, value, init=0):
        """Increase the value of the key by a number."""
        self._llogger.debug('incr')
        _id = self._gen_id()
        if key not in self._storage:
            self._storage[key] = {'v': init}
        self._storage[key]['id'] = _id
        self._storage[key]['v'] += value
        return self._storage[key]['v']

    def decr(self, key, value, init=0):
        """Decrease the value of the key by a number."""
        self._llogger.debug('decr')
        _id = self._gen_id()
        if key not in self._storage:
            self._storage[key] = {'v': init}
        self._storage[key]['id'] = _id
        self._storage[key]['v'] -= value
        return self._storage[key]['v']

    def func(self, key, func, inplace=False):
        """Execute a function on the value of the key."""
        self._llogger.debug('func')
        _id = self._gen_id()
        self._storage[key]['id'] = _id
        res = func(self._storage[key]['v'])
        if not inplace:
            self._storage[key]['v'] = res
        return _id, self._storage[key]['v']

    def save(self, keys: list, path=None):
        """Serialize values of keys to disk.

        Args:
            keys (list): list of keys
            path (str): path to save, can be None
        """
        if not isinstance(keys, list):
            raise Exception('Keys must be list')
        if path is None:
            path = self.vars_dir
        path = get_path(path)
        shutil.rmtree(path, ignore_errors=True)
        tmp_dir = get_path(path + '.tmp')
        for key in keys:
            fn = tmp_dir + '/' + up.quote(key, safe='') + '.pkl'
            with open(fn, 'wb') as f:
                pickle.dump(self._storage[key], f)
        os.rename(tmp_dir, path)

    def load(self, path=kc.SAVED_VARS):
        """Load variables from disk and replace the value if the key exists.

        Args:
            path: path to load, default is

        Returns:
            list: loaded keys
        """
        path = get_path(path)
        loaded_vars = []
        for root, dirs, files in os.walk(path, topdown=True):
            for name in files:
                fn = os.path.join(root, name)
                raylink.sprint(fn)
                key = up.unquote(name.replace('.pkl', ''))
                with open(fn, 'rb') as f:
                    value = pickle.load(f)
                self._storage[key] = value
                loaded_vars.append(key)
        self._llogger.debug(f'Load vars: {loaded_vars}')
        return loaded_vars
