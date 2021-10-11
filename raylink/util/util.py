import functools
import inspect
import ray

from raylink.node.frame import _get_proxy


def remote_dec(cls, node_cfg=None, option=None):
    if node_cfg:
        remote_cls = ray.remote(**node_cfg)(cls)
    else:
        remote_cls = ray.remote(cls)
    if option:
        remote_cls = remote_cls.options(**option)
    return remote_cls


def wrap_class(cls):
    def constructor(self, obj):
        self.obj = obj

    def wrapper_method(n):
        @functools.wraps(n)
        def method(self, *args, **kwargs):
            return ray.get(getattr(self.obj, n).remote(*args, **kwargs))

        return method

    def wrapper_method_async(n):
        @functools.wraps(n)
        def method(self, *args, **kwargs):
            return getattr(self.obj, n).remote(*args, **kwargs)

        return method

    def wrapper_method_t(n):
        @functools.wraps(n)
        def method(self, *args, **kwargs):
            return getattr(_get_proxy(self.obj), n)(*args, **kwargs)

        return method

    def wrapper_method_t_async(n):
        @functools.wraps(n)
        def method(self, *args, **kwargs):
            return getattr(_get_proxy(self.obj), n + '_async')(*args, **kwargs)

        return method

    new_class_dict = {'__init__': constructor}
    methods = inspect.getmembers(cls, predicate=inspect.isfunction)
    methods = [(n, f) for n, f in methods if n != '__init__']
    wrapper_methods = {n: wrapper_method(n) for n, _ in methods}
    new_class_dict.update(wrapper_methods)
    wrapper_methods_async = {n + '_async': wrapper_method_async(n)
                             for n, _ in methods}
    new_class_dict.update(wrapper_methods_async)
    wrapper_methods_t = {n + '_t': wrapper_method_t(n)
                         for n, _ in methods}
    new_class_dict.update(wrapper_methods_t)
    wrapper_methods_t_async = {n + '_t_async': wrapper_method_t_async(n)
                               for n, _ in methods}
    new_class_dict.update(wrapper_methods_t_async)
    new_class = type(cls.__name__ + '_', (object,), new_class_dict)
    return new_class


def get_pid():
    import os
    return os.getpid()


def get_ip(index=-1):
    """
    Get the local ip from network interfaces.
    If there is two Ethernet network interface controller,
    this method will get the ip of last Ethernet network by default.

    Returns: ip address

    """
    import netifaces as ni
    interfaces = ni.interfaces()
    available = {i: ni.ifaddresses(i)[ni.AF_INET][0]['addr']
                 for i in interfaces
                 if 'docker' not in i and ni.AF_INET in ni.ifaddresses(i)}
    eth = [k for k in available.keys() if k.startswith('e')]
    if eth:
        return available[sorted(eth)[index]]
    return '127.0.0.1'
