__all__ = ['Pickler']

import pickle


class Pickler(object):
    funcs = []

    @classmethod
    def s2c_dumps(cls, func, returns):
        if func in cls.funcs:
            return cls._custom_s2c_dumps(func, returns)
        return cls._default_s2c_dumps(func, returns)

    @classmethod
    def s2c_loads(cls, func, returns):
        if func in cls.funcs:
            return cls._custom_s2c_loads(func, returns)
        return cls._default_s2c_loads(func, returns)

    @classmethod
    def c2s_dumps(cls, func, *args, **kwargs):
        if func in cls.funcs:
            return cls._custom_c2s_dumps(func, *args, **kwargs)
        return cls._default_c2s_dumps(func, *args, **kwargs)

    @classmethod
    def c2s_loads(cls, func, args):
        if func in cls.funcs:
            return cls._custom_c2s_loads(func, args)
        return cls._default_c2s_loads(func, args)

    @staticmethod
    def _default_s2c_dumps(func, returns):
        return {
            b'returns': pickle.dumps(returns)
        }

    @staticmethod
    def _default_s2c_loads(func, returns):
        return pickle.loads(returns[b'returns'])

    @staticmethod
    def _default_c2s_dumps(func, *args, **kwargs):
        return {
            b'args': pickle.dumps(args),
            b'kwargs': pickle.dumps(kwargs)
        }

    @staticmethod
    def _default_c2s_loads(func, args):
        args, kwargs = \
            pickle.loads(args[b'args']), pickle.loads(args[b'kwargs'])
        return args, kwargs

    @staticmethod
    def _custom_s2c_dumps(func, returns):
        return {
            b'returns': pickle.dumps(returns)
        }

    @staticmethod
    def _custom_s2c_loads(func, returns):
        return pickle.loads(returns[b'returns'])

    @staticmethod
    def _custom_c2s_dumps(func, *args, **kwargs):
        return {
            b'args': pickle.dumps(args),
            b'kwargs': pickle.dumps(kwargs)
        }

    @staticmethod
    def _custom_c2s_loads(func, args):
        args, kwargs = \
            pickle.loads(args[b'args']), pickle.loads(args[b'kwargs'])
        return args, kwargs
