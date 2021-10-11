__all__ = ['WriteHead', 'ReadHead', 'ShmReplay']

from raylink.data.shm import ShM
import numpy as np
import time
import sys
import raylink
from .pickler import ReadHeadPickler
from tabulate import tabulate


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


class Area(object):
    """Auxiliary class representing an area with a start and an end.
    """

    def __init__(self, start, end):
        self.start = start
        self.end = end
        self.length = end - start + 1

    def __len__(self):
        return self.length


class SafeArea(object):
    def __init__(self):
        self.areas = []
        self.length = 0

    def add_area(self, start, end):
        """Add an available area.

        Args:
            start (int): The start of the area
            end (int): The end of the area
        """
        if end < start:
            return
        a = Area(start, end)
        self.areas.append(a)
        self.length += a.length

    def get_last(self, size):
        """Get the newest added areas with total length as size.

        Args:
            size (int): Size of the areas

        Returns:
            SafeArea: A group of areas with the total length as `size`
        """
        _size = size
        sa = SafeArea()
        for a in self.areas[::-1]:
            if a.length >= _size:
                sa.add_area(a.end - _size + 1, a.end)
                assert sa.length == size
                return sa
            else:
                sa.add_area(a.start, a.end)
                _size -= a.length

    def to_list(self):
        """Convert areas into a list

        Returns:
            list: A list contains every index in `self.areas`
        """
        _list = []
        for a in self.areas:
            _list.extend(list(range(a.start, a.end + 1)))
        return _list

    def __len__(self):
        return self.length

    def __contains__(self, item):
        for a in self.areas:
            if a.start <= item <= a.end:
                return True
        return False


class NPSharedMemory(object):
    def __init__(self, arr, name=None):
        self.arr_shape = arr.shape
        self.arr_dtype = arr.dtype
        if name:
            self.attach(name)
        else:
            self.create(arr)

    def create(self, arr):
        self.shm = ShM(create=True, size=arr.nbytes)
        self.shm_name = self.shm.name
        self.array = np.ndarray(
            self.arr_shape, dtype=self.arr_dtype, buffer=self.shm.buf)
        np.copyto(self.array, arr)

    def attach(self, name=None):
        if name is None:
            name = self.shm_name
        self.shm = ShM(name=name)
        self.shm_name = name
        self.array = np.ndarray(
            self.arr_shape, dtype=self.arr_dtype, buffer=self.shm.buf)

    def __getstate__(self):
        return {
            'shm_name': self.shm_name,
            'arr_shape': self.arr_shape,
            'arr_dtype': self.arr_dtype
        }

    def __setstate__(self, state):
        self.__dict__.update(state)


class WriteHead(raylink.OutlineNode):
    TYPE = 'head'

    def setup(self, shms: dict):
        self._shms = shms
        for shm in shms.values():
            shm.attach()

    def write(self, keys, cursors, samples):
        for key in keys:
            try:
                self._shms[key].array[cursors[key]] = samples[key]
            except Exception as e:
                print(f'ERROR: Unable to write key, {key}', file=sys.stderr)
                raise e

    def write_inc(self, samples):
        st = time.time()
        res, cid, cursor = self._parent.register_cursor(1)
        if res == -1:
            print('WARN: Unable to increase cursor. Dropping.', file=sys.stderr)
            return cursor
        for key in list(samples.keys()):
            try:
                array = self._shms[key].array
                sample = np.array(samples.pop(key), array.dtype)
                np.copyto(array[cursor[0], ...], sample)
            except Exception as e:
                print(f'Error: Unable to write key: {key}', file=sys.stderr)
                raise e
        self._shms['access_count'].array[cursor[0]] = 0
        self._parent.unregister_cursor(cid)
        self._llogger.debug(f'write_inc takes {time.time() - st}')
        return cursor

    def write_multi_inc(self, samples):
        st = time.time()
        num = len(list(samples.values())[0])
        res, cid, cursor = self._parent.register_cursor(num)
        if res == -1:
            print('WARN: Unable to increase cursor. Dropping.', file=sys.stderr)
            return cursor
        for key in list(samples.keys()):
            try:
                array = self._shms[key].array
                sample = np.array(samples.pop(key), array.dtype)
                array[cursor, ...] = sample
            except Exception as e:
                print(f'Error: Unable to write key: {key}', file=sys.stderr)
                raise e
        self._shms['access_count'].array[cursor] = 0
        self._parent.unregister_cursor(cid)
        self._llogger.debug(f'write_multi_inc takes {time.time() - st}')
        return cursor


class ReadHead(raylink.OutlineNode):
    TYPE = 'head'

    def setup(self, shms: dict):
        self._shms = shms
        for shm in shms.values():
            shm.attach()
        self._pickler = ReadHeadPickler

    def read(self, keys, cursors, count=False):
        st = time.time()
        batch_sample = {}
        for key in keys:
            batch_sample[key] = self._shms[key].array[cursors[key]]
        if count:
            # Do not assign value directly
            # because advanced index will make a copy
            for cursor in cursors[keys[0]]:
                self._shms['access_count'].array[cursor] += 1

        self._llogger.debug(f'read takes {time.time() - st}')
        return batch_sample


class ShmReplay(raylink.OutlineNode):
    TYPE = 'replay'

    def setup(self, cfg):
        self._config = cfg
        self._capacity = self._config.capacity
        self.create_storage()
        self.create_head()
        self._wh_cursors = {}
        self._time_wh = {}
        self._wh_time = {}
        self._nid_safe_area = {}
        self._safe_area_left = self._capacity
        self._safe_area_right = 0
        self._log_delta = 1000
        self._ma_sp = 0
        self._ma_sp_lambda = 0.5
        self._actions = []
        self._last_action_count = 0
        self._logger.debug(
            f'Replay-{self._index} setup complete, '
            f'ip: {self.ip_()}, node cfg {self.node_cfg_()}')

    def create_storage(self):
        self._num = 0
        self._loop_num = 0
        self._next_cap = self._capacity
        self._keys = []
        self._shms = {}

        mem_sum = []
        total_bytes = 0
        for key, (shape, dtype, init_func) in self._config.structure.items():
            struct = [self._capacity] + list(shape)
            nbytes = np.prod(struct) * np.dtype(dtype).itemsize
            total_bytes += nbytes
            mem_sum.append([key, sizeof_fmt(nbytes)])
        mem_sum = tabulate(mem_sum, headers=['Key', 'Size'], tablefmt='pretty',
                           colalign=['left', 'right'])
        mem_sum = '\nReplay Memory Summary:\n' + \
                  mem_sum + \
                  f'\nTotal Size: {sizeof_fmt(total_bytes)}'
        self._logger.info(mem_sum)

        def empty(struct, dtype):
            return np.empty(struct, dtype=dtype)

        for key, (shape, dtype, init_func) in self._config.structure.items():
            if init_func is None:
                init_func = empty
            struct = [self._capacity] + list(shape)
            _storage = init_func(struct, dtype)
            self._logger.info(
                f'creating "{key}" memory, {_storage.shape}, {dtype}')
            self._shms[key] = NPSharedMemory(_storage)
            self._keys.append(key)
        _storage = np.zeros((self._capacity,), dtype=np.uint64)
        self._shms['access_count'] = NPSharedMemory(_storage)

    def create_head(self):
        self.write_heads = raylink.batch_create(
            WriteHead, self, bind=True, async_=True,
            num=self._config.num_write_head, shms=self._shms)
        self.write_heads = raylink.get(self.write_heads)
        self.write_heads_path = []
        for head in self.write_heads:
            self.write_heads_path.append(head.path_())
        self.read_heads = []
        self.read_heads = raylink.batch_create(
            ReadHead, self, bind=True, async_=True,
            num=self._config.num_read_head, shms=self._shms)
        self.read_heads = raylink.get(self.read_heads)
        self.read_heads_path = []
        for head in self.read_heads:
            self.read_heads_path.append(head.path_())

    def get_read_heads_path(self):
        return self.read_heads_path

    def get_write_heads_path(self):
        return self.write_heads_path

    def get_keys(self):
        return self._keys

    def cursor(self):
        return self._num % self._capacity

    def loop_num(self):
        return self._loop_num

    def _validate_safe_area(self):
        valid_safe_area = SafeArea()
        if not self._wh_time:
            if self.is_full():
                if self.cursor() > 0:
                    valid_safe_area.add_area(self.cursor(), self._capacity - 1)
                    valid_safe_area.add_area(0, self.cursor() - 1)
                else:
                    valid_safe_area.add_area(self.cursor(), self._capacity - 1)
            else:
                valid_safe_area.add_area(0, self.cur_size() - 1)
            return valid_safe_area
        if len(self._wh_cursors) == 1:
            cursor = list(self._wh_cursors.values())[0][0]
            if self.is_full():
                if cursor < self.cursor():
                    valid_safe_area.add_area(self.cursor(), self._capacity - 1)
                    valid_safe_area.add_area(0, cursor - 1)
                    valid_safe_area.add_area(cursor + 1, self.cursor() - 1)
                else:
                    valid_safe_area.add_area(cursor + 1, self._capacity - 1)
                    valid_safe_area.add_area(0, self.cursor())
                    valid_safe_area.add_area(self.cursor() + 1, cursor - 1)
            else:
                valid_safe_area.add_area(0, cursor - 1)
                valid_safe_area.add_area(cursor + 1, self.cur_size() - 1)
            return valid_safe_area
        _old_t = min(self._time_wh.keys())
        _old_cursor, _old_loop_num = self._wh_cursors[self._time_wh[_old_t]]
        _new_t = max(self._time_wh.keys())
        _new_cursor, _new_loop_num = self._wh_cursors[self._time_wh[_new_t]]
        if _old_cursor > _new_cursor:
            valid_safe_area.add_area(_new_cursor + 1, _old_cursor - 1)
            return valid_safe_area
        if self.is_full():
            valid_safe_area.add_area(_new_cursor + 1, self._capacity - 1)
            valid_safe_area.add_area(0, _old_cursor - 1)
            return valid_safe_area
        valid_safe_area.add_area(0, _old_cursor - 1)
        return valid_safe_area

    def acquire_safe_area(self, nid, size, keys):
        self._lock()
        self._actions.append(f'acquire_safe_area({repr(nid)}, {repr(size)}, {repr(keys)}')
        valid_safe_area = self._validate_safe_area()
        if valid_safe_area.length < size:
            self._unlock()
            return [], None

        valid_safe_area = valid_safe_area.get_last(size)
        self._nid_safe_area[nid] = valid_safe_area
        self._unlock()
        area_list = valid_safe_area.to_list()
        batch_sample = {}
        for key in keys:
            batch_sample[key] = self._shms[key].array[area_list]
        return area_list, batch_sample

    def release_safe_area(self, nid):
        self._lock()
        self._actions.append(f'release_safe_area({repr(nid)})')
        self._nid_safe_area.pop(nid, None)
        self._unlock()

    def is_full(self):
        return self._num > self._capacity

    def cur_size(self):
        if self.is_full():
            return self._capacity
        return self._num

    def update_stat(self):
        if self._num % self._log_delta != 0:
            return
        if hasattr(self, '_tick_time'):
            delta_time = time.time() - self._tick_time
            sp = self._log_delta / delta_time
            if self._ma_sp == 0:
                self._ma_sp = sp
            else:
                self._ma_sp = self._ma_sp * self._ma_sp_lambda + sp * (1 - self._ma_sp_lambda)
            self._llogger.debug(f'sample speed {round(sp, 2)}it/s')
            self._llogger.debug(f'sample speed MA {round(self._ma_sp, 2)}it/s')
            if self._ma_sp > self._log_delta:
                self._log_delta = int(self._log_delta * 10)
            if self._ma_sp < self._log_delta // 10:
                self._log_delta = self._log_delta // 10
        self._tick_time = time.time()
        if 'version' in self._shms:
            a = self._shms['version'].array
            unique, counts = np.unique(a, return_counts=True)
            ver = dict(sorted(dict(zip(unique, counts)).items(), key=lambda x: x[0]))
            self._llogger.debug(f'version {ver}')
        self._llogger.debug('\n' + '\n'.join(self._actions))
        self._actions = []

    def validate_cursor(self):
        _cursor = self.cursor()
        for safe_area in self._nid_safe_area.values():
            if _cursor in safe_area:
                return False
        if len(self._wh_cursors) == 0:
            return True
        _old_t = min(self._time_wh.keys())
        _old_cursor, _old_loop_num = self._wh_cursors[self._time_wh[_old_t]]
        # loop for one capacity
        if self.loop_num() > _old_loop_num and _old_cursor <= _cursor:
            return False
        return True

    def _register_cursor(self):
        self._lock()
        if self._actions and '_register_cursor()' in self._actions[-1]:
            self._last_action_count += 1
            self._actions[-1] = f'_register_cursor() +{self._last_action_count}'
        else:
            self._last_action_count = 1
            self._actions.append(f'_register_cursor() +{self._last_action_count}')
        cid = str(time.time()) + '-' + self._nid
        if not self.validate_cursor():
            return -1, 0, 0
        _cursor = self.cursor()
        self._wh_cursors[cid] = _cursor, self.loop_num()
        c_time = time.time()
        self._time_wh[c_time] = cid
        self._wh_time[cid] = c_time
        self._num += 1
        if self._num == self._next_cap:
            self._loop_num += 1
            self._next_cap += self._capacity
        self._unlock()
        self.update_stat()
        return 0, cid, _cursor

    def register_cursor(self, num=1):
        cids, cursors = [], []
        for _ in range(num):
            res, cid, cursor = self._register_cursor()
            if res == -1:
                return -1, cids, np.array(cursors, dtype=np.uint64)
            cids.append(cid)
            cursors.append(cursor)
        return 0, cids, np.array(cursors, dtype=np.uint64)

    def _unregister_cursor(self, cid):
        self._lock()
        if self._actions and '_unregister_cursor()' in self._actions[-1]:
            self._last_action_count += 1
            self._actions[-1] = f'_unregister_cursor() +{self._last_action_count}'
        else:
            self._last_action_count = 1
            self._actions.append(f'_unregister_cursor() +{self._last_action_count}')
        self._wh_cursors.pop(cid)
        c_time = self._wh_time.pop(cid)
        self._time_wh.pop(c_time)
        self._unlock()

    def unregister_cursor(self, cids):
        for cid in cids:
            self._unregister_cursor(cid)
