__all__ = ['ccon', 'create', 'batch_create', 'init']

import cloudpickle as pickle
from pprint import pprint
import types
from typing import Union, Type, TypeVar, List, Dict, Optional
import ray
import io

from .frame import *
from .frame import _set_ps, _set_node
from .brain import Brain
import raylink.constants as kc
from raylink.util.config import Config

T = TypeVar('T')
"""
There are three type node. The basic node is `SelfwareNode`,
which will be given its own actor handler when created. 
Every node has node id, which is provide by BRAIN,
and saved in both its actor handler and its wrapper.
The `OutlineNode` is inherited by users to create new node in raylink.

Notice
You can not call any function of one node while the node
is processing another function. For example, you can not call
a function of parent node, `parent.set_tag()`, in the `setup()` of a
child node while parent node is using `ccon` to initialize
the child node.
"""


def _reconstruct_offline(wrapper, offline):
    [setattr(wrapper, k, v) for k, v in offline.items()]
    offline_methods = offline['_offline_methods']
    [setattr(wrapper, k, v) for k, v in offline_methods.items()]
    for k, v in offline_methods.items():
        func_type = types.MethodType
        setattr(wrapper, k, func_type(v, wrapper))
    wrapper._wrapper = wrapper
    return wrapper


def csn(cls, node_cfg=None, option=None, **kwargs):
    """
    Create selfware node

    Args:
        cls: SelfwareNode class
        node_cfg (dict): ray config for actor class
        option (dict): ray option for actor class
        **kwargs: key word arguments for ray.remote

    Returns: class object

    """
    import raylink.util.util as u
    # decorate class using ray
    remote_cls = u.remote_dec(cls, node_cfg, option)
    # create actor handler
    ah = remote_cls.remote(**kwargs, node_cfg=node_cfg)
    # create wrapped class based on cls
    # it will remove `ray.remote` from code
    wrapped_class = u.wrap_class(cls)
    # create local wrapper contain the actor handler
    wrapper = wrapped_class(ah)
    # set actor handler to self
    wrapper.set_ah(ah)
    # setup offline attribute
    offline = wrapper.update_offline()
    # reconstruct offline attributes and offline methods in wrapper
    # so that you don't need to access actor to get these attributes
    wrapper = _reconstruct_offline(wrapper, offline)
    return wrapper, {'offline': offline}


@ray.remote
def _ccon(cls, info, node_cfg, alias='', setup=True, **kwargs):
    BRAIN = get_BRAIN()
    parent = info['p_wrapper']
    child, detail = csn(cls, node_cfg=node_cfg, info=info, parent=parent)
    BRAIN.set_node_meta(child.nid_(), kc.META_WRAPPER, pickle.dumps(child))
    BRAIN.set_node_meta(child.nid_(), kc.META_OFFLINE,
                        pickle.dumps(detail['offline']))
    if alias != '':
        BRAIN.set_alias(child.path_(), alias)
    if setup:
        child.wrapped_setup(**kwargs)
    BRAIN.set_node_meta(child.nid_(), kc.META_SETUP, pickle.dumps(True))
    child.join()
    return child


def check_alias(num, alias):
    if alias == '':
        return [alias for _ in range(num)]
    if num == 1:
        assert isinstance(alias, str), 'Alias must be str.'
        return [alias]
    assert all([isinstance(a, str) for a in alias]), \
        'Alias must be list of str when num > 1.'
    return alias


def wait_setup(objs, kwargs):
    futures = []
    for o in objs:
        f = o.wrapped_setup_async(**kwargs)
        futures.append(f)
    ray.get(futures)
    for o in objs:
        o.join()
    if len(objs) == 1:
        objs = objs[0]
    return objs


def add_log(log):
    from raylink import get_logger
    if get_logger():
        get_logger().debug(log)


def ccon(
        cls: Type[T],
        parent: object = None,
        bind: bool = False,
        node_cfg: Optional[Dict[str, int]] = None,
        async_: bool = False,
        num: int = 1,
        alias: Union[str, List[str]] = '',
        setup: bool = True,
        **kwargs
):
    """
    Internal function to create child OutlineNode

    Args:
        cls: OutlineNode class
        parent: parent OutlineNode object
        bind: whether stay on the same machine with parent
        node_cfg: resource config used by ray
        async_: whether need to wait for setup complete
        num: number of children to be initialized
        alias: alias name for each child
        setup: whether need to run setup
        **kwargs: keyword arguments for setup

    Returns:
        OutlineNode instance: the created node

    Example::

        >>> import raylink
        >>> from raylink.data.replay import ShmReplay
        >>> raylink.ccon(ShmReplay)  # Create a replay buffer based on shared memory

    """
    import time
    assert num >= 1 and isinstance(num, int)
    alias = check_alias(num, alias)
    BRAIN = get_BRAIN()
    if parent is None:
        parent = BRAIN
    info = parent.get_cache('ccon_info')
    if info is None:
        rm = BRAIN.get_res_mgr()
        info = {
            'cluster_mode': rm.cluster_mode,
            'p_nid': parent.nid_(),
            'p_node_cfg': parent.node_cfg_(),
        }
        parent.set_cache('ccon_info', info)
    info['p_wrapper'] = parent.wrapper_(update=True)
    alloc_start = time.time()
    rets = BRAIN.add_children(info['p_nid'], cls.TYPE, num=num)
    if node_cfg is None:
        if bind and info['cluster_mode']:
            assert info['p_node_cfg'] is not None
        else:
            info['p_node_cfg'] = None
        node_cfgs = BRAIN.alloc_node_cfgs(cls.TYPE, info['p_node_cfg'], num=num)
    else:
        node_cfgs = [node_cfg for _ in range(num)]
    alloc_time = round(time.time() - alloc_start, 3)
    log = f'Allocate {num} {cls.TYPE} takes {alloc_time}s'
    add_log(log)
    futures = []
    alloc_start = time.time()
    for i in range(num):
        info.update(rets[i])
        node_cfg = node_cfgs[i]
        f = _ccon.remote(
            cls, info, node_cfg, alias=alias[i], setup=False, **kwargs)
        futures.append(f)
    objs = ray.get(futures)
    alloc_time = round(time.time() - alloc_start, 3)
    log = f'Ray allocate {num} {cls.TYPE} take {alloc_time}s'
    add_log(log)
    if setup:
        if async_:
            return ray.remote(wait_setup).remote(objs, kwargs)
        else:
            alloc_start = time.time()
            objs = wait_setup(objs, kwargs)
            alloc_time = round(time.time() - alloc_start, 3)
            log = f'Wait {num} {cls.TYPE} setup take {alloc_time}s'
            add_log(log)
            return objs
    if len(objs) == 1:
        objs = objs[0]
    return objs


def create(
        cls: Type[T],
        parent: object = None,
        bind: bool = False,
        node_cfg: Optional[Dict[str, int]] = None,
        async_: bool = False,
        num: int = 1,
        alias: Union[str, List[str]] = '',
        setup: bool = True,
        **kwargs
):
    if num > 1:
        raise Exception('Use batch create!')
    return ccon(cls, parent=parent, bind=bind, node_cfg=node_cfg,
                async_=async_, num=num, alias=alias, setup=setup, **kwargs)


def batch_create(
        cls: Type[T],
        parent: object = None,
        bind: bool = False,
        node_cfg: Optional[Dict[str, int]] = None,
        async_: bool = False,
        num: int = 1,
        alias: Union[str, List[str]] = '',
        setup: bool = True,
        **kwargs
):
    objs = ccon(cls, parent=parent, bind=bind, node_cfg=node_cfg,
                async_=async_, num=num, alias=alias, setup=setup, **kwargs)
    if num == 1:
        return [objs]
    return objs


def init(
        init_cfg: Optional[Dict] = None,
        cluster_cfg: Optional[Dict] = None,
        node_cfg: Optional[Dict] = None,
        user_cfg: Config = None
):
    """
    `init` function first initialize ray. Then it will create a global
    hidden node, named BRAIN, which will connect all nodes.

    Args:
        init_cfg (dict): ray init keyword arguments
        cluster_cfg (dict): resources in the cluster
        node_cfg (dict): resources for every type of OutlineNode
        user_cfg (:py:class:`~raylink.util.config.Config`): user configuration
    """
    import raylink.util.log as ul
    import raylink.data.storage as s
    import raylink.data.ps.ps_captain as p
    import raylink.util.resource as ur
    import raylink.util.config as uc
    import raylink.util.monitor as um
    if node_cfg is None:
        node_cfg = {}
    if cluster_cfg is None:
        cluster_cfg = {}
    if init_cfg is None:
        init_cfg = {}
    if user_cfg is None:
        user_cfg = uc.BasicConfig()
        user_cfg.setup()
    ray.init(**init_cfg)
    rm = ur.ResourceManager(cluster_cfg, node_cfg)
    brain_cfg = rm.allocate('BRAIN')
    # BRAIN_option = {'name': 'BRAIN', 'lifetime': 'detached'}
    BRAIN_option = {'name': 'BRAIN'}
    BRAIN, detail = csn(Brain, node_cfg=brain_cfg, option=BRAIN_option)
    BRAIN.set_node_meta(BRAIN.nid_(), kc.META_WRAPPER, pickle.dumps(BRAIN))
    BRAIN.set_node_meta(BRAIN.nid_(), kc.META_OFFLINE,
                        pickle.dumps(detail['offline']))
    BRAIN.set_res_mgr(rm)
    logger = ccon(ul.Logger, bind=True, log_cfg=user_cfg.logger)
    storage = ccon(s.Storage, bind=True)
    storage.put('config', user_cfg)
    log_info = logger.get_log_info()
    _logger = ul.LocalLogAPI('BRAIN', 'BRAIN', logger)
    set_storage(storage)
    set_logger(_logger)
    ps = ccon(p.PSCaptain, bind=True)
    _set_ps(ps)
    old_bro = ccon(um.OldBro, bind=True)
    _set_node('old_bro', old_bro)
    _logger.info(f'BRAIN run path: {log_info["base_dir"]}')
    buffer = io.StringIO()
    pprint(init_cfg, buffer)
    _logger.info(f'ray init config:\n{buffer.getvalue()}')
    buffer = io.StringIO()
    pprint(cluster_cfg, buffer)
    _logger.info(f'cluster config:\n{buffer.getvalue()}')
    buffer = io.StringIO()
    pprint(node_cfg, buffer)
    _logger.info(f'node config:\n{buffer.getvalue()}')
    _logger.info(f'user config:\n{user_cfg}')
    BRAIN.setup_global()
