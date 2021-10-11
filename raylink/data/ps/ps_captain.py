__all__ = ['PSCaptain']

from raylink.util.path import get_path
import raylink.constants as kc
import threading
import raylink
import uuid
import json
import os


def get_models_path(base_dir):
    import os
    return os.path.join(base_dir, 'models')


def cal_single_nid_route_map(nids):
    nid_route_map = {}
    if len(nids) == 0:
        return nid_route_map
    if len(nids) <= kc.PS_TREE_K:
        for nid in nids:
            nid_route_map[nid] = []
        return nid_route_map
    remain_nids = nids[kc.PS_TREE_K:]
    for i in range(kc.PS_TREE_K):
        nid = nids[i]
        nid_route_map[nid] = remain_nids[i::kc.PS_TREE_K]
    return nid_route_map


def cal_nid_route_map(nids):
    route_map = cal_single_nid_route_map(nids)
    for nid in route_map:
        route_map[nid] = cal_nid_route_map(route_map[nid])
    return route_map


def set_route_map(self, route_map, get_node_func):
    self.route_map = route_map
    self._llogger.debug(f'route map: {route_map}')
    self.officer_nodes = []
    for nid, route_map in route_map.items():
        o = get_node_func(nid)
        self.officer_nodes.append(o)
        self._llogger.debug(f'set route for {nid}, {o.node_cfg_()}')
        o.set_route_map(route_map)


class PSCaptain(raylink.SuperVillain):
    TYPE = 'ps'

    def setup(self):
        """Setup ps.
        Acquire storage and config. Create ps officers and build tunnels"""
        from .ps_officer import PSOfficer
        self.models_dir = get_path(get_models_path(self._llogger.base_dir))
        self._params = {}
        self._futures = None
        self._locks = {}
        raylink.SuperVillain.setup(self, PSOfficer)
        self.officers = self.minions
        self.node_cfg_officers_nid_map = self.node_cfg_minions_nid_map
        self.nid_officers_map = self.nid_minions_map
        self._setup_route_map()

    def _setup_route_map(self):
        route_map = cal_nid_route_map(list(self.nid_officers_map.keys()))
        set_route_map(self, route_map, lambda nid: self.nid_officers_map[nid])

    def get_officer(self, node_cfg):
        """return nid of the officer.

        Args:
            node_cfg (str): Node's node cfg.

        Returns:
            str: NID of the officer.
        """

        return self.get_minion(node_cfg)

    def _gen_id(self):
        """Generate NID.

        Returns:
            str: NID
        """

        return str(uuid.uuid4())

    def push(self, tag, params, info, wait=False,
             _id=None, _time=None, _count=None):
        """Push parameters to all officers.

        Args:
            tag (str): Tag of the parameters
            params (bytes): Parameters to push
            info (dict): Parameter info
            wait: Whether wait until all officers' push is completed
            _id (str): Override id of the parameters
            _time: Override push time of the parameters
            _count: Override push count of the parameters

        Returns:
            str: The NID of ps captain

        Examples::

            >>> ps.push('hrl/policy-0', b'byte_weights', {'learn_step': 1})
        """
        self._llogger.debug(f'push tag {tag}')
        if tag not in self._locks:
            self._locks[tag] = threading.Lock()
        self._locks[tag].acquire()
        if _id is None:
            _id = self._gen_id()
        if _time is None:
            import time
            _time = time.time()
        if _count is None:
            if tag not in self._params:
                _count = 1
            else:
                _count = self._params[tag]['meta']['count'] + 1
        meta_info = {'id': _id, 'info': info, 'time': _time, 'count': _count}
        self._params[tag] = {'meta': meta_info, 'p': params}
        kwargs = {'tag': tag, 'params_info': self._params[tag]}
        self._llogger.debug(f'broadcast push tag {tag}')
        self.broadcast('_process_info', tag, kwargs, wait)
        self._llogger.debug(f'broadcast push tag {tag} end')
        self._locks[tag].release()
        return _id

    def duplicate(self, old_tag, new_tag, info=None,
                  _id=None, _time=None, _count=None, wait=True):
        """Duplicate a tag to a new tag.

        Args:
            old_tag: tag to be duplicated
            new_tag: new tag
            wait: whether wait for broadcasting to officers
        """
        self.tag_check(old_tag)
        self._params[new_tag] = self._params[old_tag]
        om = self._params[new_tag]['meta']
        meta_info = {
            'id': _id if _id else om['id'],
            'info': info if info else om['info'],
            'time': _time if _time else om['time'],
            'count': _count if _count else om['count']
        }
        self._params[new_tag]['meta'] = meta_info
        kwargs = {'tag': new_tag, 'params_info': self._params[new_tag]}
        self._llogger.debug(f'broadcast duplicate tag {new_tag}')
        self.broadcast('_process_info', new_tag, kwargs, wait)
        self._llogger.debug(f'broadcast duplicate tag {new_tag} end')
        return self._params[new_tag]['meta']['id']

    def delete(self, tag, wait=True):
        """Delete a tag and its model

        Args:
            tag (str): Parameter tag
            wait: whether wait for broadcasting to officers
        """
        if tag not in self._params:
            return
        self._params.pop(tag)
        self._llogger.debug(f'broadcast delete tag {tag}')
        self.broadcast('delete', tag, {'tag': tag}, wait)
        self._llogger.debug(f'broadcast delete tag {tag} end')

    def broadcast(self, func, tag, kwargs, wait):
        """Broadcast function to all officers"""
        # issue https://github.com/ray-project/ray/issues/9432
        import pickle
        import time
        st = time.time()
        data = pickle.dumps(kwargs)
        self._llogger.debug(f'put takes {time.time() - st}')
        st = time.time()
        rs = []
        for o in self.officer_nodes:
            r = o.broadcast_async(func, tag, data, wait)
            rs.append(r)
        self._llogger.debug(f'loop takes {time.time() - st}')
        if rs and wait:
            raylink.get(rs)

    def save_model(self, tag, path):
        self.tag_check(tag)
        model_info = self._params[tag]
        tag_dir = get_path(path)
        model_name = f'model-{model_info["meta"]["id"]}.pt'
        model_path = os.path.join(tag_dir, model_name)
        with open(model_path, 'wb') as f:
            f.write(model_info['p'])
        meta_info = model_info['meta']
        with open(model_path + '.meta', 'w') as f:
            json.dump(meta_info, f)
        self._llogger.debug(f'Save model {tag} to {model_path}')

    def save_models(self, step):
        if len(self._params.keys()) == 0:
            return
        models_dir = os.path.join(self.models_dir, f'models-{step}')
        tmp_dir = models_dir + '.tmp'
        for tag in self._params.keys():
            tag_dir = os.path.join(tmp_dir, tag)
            self.save_model(tag, tag_dir)
        os.rename(tmp_dir, models_dir)
        self._llogger.debug(f'Save models to {models_dir}')

    def load_models(self, model_path=kc.SAVED_MODEL):
        model_path = get_path(model_path)
        loaded_model = {}

        def _load(root, name):
            tag = root.replace(model_path, '')[1:]
            with open(os.path.join(root, name + '.meta')) as f:
                meta_info = json.load(f)
            with open(os.path.join(root, name), 'rb') as f:
                params = f.read()
            self.push(
                tag, params, meta_info['info'], _id=meta_info['id'],
                _time=meta_info['time'], _count=meta_info['count'], wait=True)
            return tag, meta_info

        for root, dirs, files in os.walk(model_path, topdown=True):
            for name in files:
                if not (name.endswith('.pt')):
                    continue
                tag, meta_info = _load(root, name)
                loaded_model[tag] = meta_info
        self._llogger.debug(f'Load models: {loaded_model}')
        return loaded_model

    def tag_check(self, tag):
        if tag not in self._params:
            raise Exception("Tag not exists")
