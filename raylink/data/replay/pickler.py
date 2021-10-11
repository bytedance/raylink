__all__ = ['WriteHeadPickler', 'ReadHeadPickler']

import pickle
import marshal
import numpy as np
from raylink.data.tunnel.pickler import Pickler


class WriteHeadPickler(Pickler):
    funcs = ['write', 'write_inc']

    @classmethod
    def _custom_c2s_dumps(cls, func, *args, **kwargs):
        keys, cursors, samples = args
        new_args = {b'keys': marshal.dumps(keys),
                    b'cursors': pickle.dumps(cursors)}
        for k in samples:
            _k = k.encode()
            array = samples[k]
            new_args[_k + b'_bytes'] = array.tobytes()
            array_info = array.shape, array.dtype
            new_args[_k + b'_info'] = pickle.dumps(array_info)
        return new_args

    @classmethod
    def _custom_c2s_loads(cls, func, args):
        keys = marshal.loads(args[b'keys'])
        cursors = marshal.loads(args[b'cursors'])
        samples = {}
        for k in keys:
            _k = k.encode()
            shape, dtype = pickle.loads(args[_k + b'_info'])
            array = np.frombuffer(args[_k + b'_bytes'], dtype=dtype)
            array = array.reshape(shape)
            samples[k] = array
        args = keys, cursors, samples
        return args, {}


class ReadHeadPickler(Pickler):
    funcs = ['read']

    @classmethod
    def _custom_s2c_dumps(cls, func, returns):
        new_returns = {b'keys': marshal.dumps(list(returns.keys()))}
        for k in returns:
            _k = k.encode()
            array = returns[k]
            new_returns[_k + b'_bytes'] = array.tobytes()
            array_info = array.shape, array.dtype
            new_returns[_k + b'_info'] = pickle.dumps(array_info)
        return new_returns

    @classmethod
    def _custom_s2c_loads(cls, func, returns):
        keys = marshal.loads(returns[b'keys'])
        samples = {}
        for k in keys:
            _k = k.encode()
            shape, dtype = pickle.loads(returns[_k + b'_info'])
            array = np.frombuffer(returns[_k + b'_bytes'], dtype=dtype)
            array = array.reshape(shape)
            samples[k] = array
        return samples
