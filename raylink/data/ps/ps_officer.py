__all__ = ['PSOfficer']

from raylink.data.shm import ShM
from raylink.constants import PS_SAFE_RATIO, PS_CACHE_SIZE
import raylink
import threading
from raylink.util.monitor import TM, ProcMem


class PSOfficer(raylink.OutlineNode):
    TYPE = 'ps_officer'

    def setup(self):
        """Setup this ps officer."""

        self.config = self._storage.get('config')
        try:
            self.safe_ratio = self.config.ps.safe_ratio
        except:
            self.safe_ratio = PS_SAFE_RATIO
        try:
            self._cache_size = self.config.ps.cache_size
        except:
            self._cache_size = PS_CACHE_SIZE
        self._params = {}
        self._params_shm = {}
        self._futures = {}
        self._locks = {}
        self.proc_mem = {}
        self.proc_count = {}
        self.start_tm()

    def start_tm(self):
        import schedule
        import threading
        import time

        self.tm = TM()
        self.tm.start()

        def stat():
            self._llogger.debug('\n' + self.tm.stat())

        schedule.every(1).minutes.do(stat)

        def run():
            while True:
                schedule.run_pending()
                time.sleep(1)

        job_thread = threading.Thread(target=run, daemon=True)
        job_thread.start()

    def set_route_map(self, route_map):
        from .ps_captain import set_route_map
        set_route_map(self, route_map, lambda nid: self.find_nid(nid))

    def _process_info(self, tag, params_info):
        """Update parameters corresponding to the tag from the captain.

        Args:
            tag (str): Parameter tag
            params_info (dict): Parameter Info
        """
        self._llogger.debug(f'processing tag {tag}')
        if tag not in self._locks:
            self._locks[tag] = threading.Lock()
        self._locks[tag].acquire()
        if tag not in self.proc_mem:
            self.proc_mem[tag] = ProcMem()
            self.proc_count[tag] = 0
        self.proc_mem[tag].snapshot(True)
        p = params_info['p']
        p_size = len(p)
        if tag not in self._params_shm:
            self._params_shm[tag] = {
                'shms': [None for _ in range(self._cache_size)],
                'cur': -1,
            }
        _tmp_params_shm = self._params_shm[tag]
        self.proc_mem[tag].snapshot()
        _next_idx = (_tmp_params_shm['cur'] + 1) % self._cache_size
        _next_shm = _tmp_params_shm['shms'][_next_idx]
        self.proc_mem[tag].snapshot()
        if _next_shm is None or _next_shm['size'] < p_size:
            # if shm not exist or size of params grows bigger
            if _next_shm is not None:
                # release old shm
                self._llogger.debug(f'releasing shared memory '
                                    f'{_next_shm["shm"].name} for {tag}')
                _next_shm['shm'].close()
                _next_shm['shm'].unlink()
                self._llogger.debug(f'released shared memory for {tag}')
            # create new shm
            _tmp_shm = self._create_shm(tag, p_size)
        else:
            _tmp_shm = _tmp_params_shm['shms'][_next_idx]
            _tmp_shm['size'] = p_size
        self.proc_mem[tag].snapshot()
        self._llogger.debug(f'replacing shared memory {_tmp_shm["shm"].name}')
        _tmp_shm['shm'].buf[:p_size] = p
        _tmp_params_shm['shms'][_next_idx] = _tmp_shm
        self._params[tag] = params_info
        self._params_shm[tag]['cur'] = _next_idx
        self.proc_mem[tag].snapshot()
        self.proc_count[tag] += 1
        if self.proc_count[tag] % 100 == 0:
            self._llogger.debug(f'tag {tag}\n' + self.proc_mem[tag].stat())
        self._locks[tag].release()
        self._llogger.debug(f'process tag {tag} finished')

    def delete(self, tag):
        """Delete a tag and its model

        Args:
            tag (str): Parameter tag
            wait: whether wait for broadcasting to officers
        """
        self._llogger.debug(f'deleting tag {tag}')
        self._locks[tag].acquire()
        if tag not in self._params:
            self._locks[tag].release()
            return
        self._params.pop(tag)
        for i in range(self._cache_size):
            shm = self._params_shm[tag]['shms'][i]
            if shm is None:
                continue
            shm['shm'].close()
            shm['shm'].unlink()
        self._params_shm.pop(tag)
        self._locks[tag].release()
        self._llogger.debug(f'delete tag {tag} finished')

    def list_tags(self):
        return list(self._params.keys())

    def tags_info(self):
        return dict([(tag, self._params[tag]['meta']['info'])
                     for tag in self._params])

    def _broadcast(self, func, data):
        import pickle
        self._llogger.debug(f'get kwargs for {func}')
        kwargs = pickle.loads(data)
        self._llogger.debug(f'get kwargs end for {func}')
        getattr(self, func)(**kwargs)
        self._llogger.debug(f'run {func} end')

    def broadcast(self, func, tag, data, wait):
        """Broadcast function to all officers"""
        self._llogger.debug(f'enter broadcast for {func}')
        if tag in self._futures:
            t = self._futures.pop(tag)
            t.join()
        t = threading.Thread(
            target=self._broadcast, args=(func, data), daemon=True)
        t.start()
        self._futures[tag] = t
        self._llogger.debug(f'thread start for {func}')
        rs = []
        for o in self.officer_nodes:
            r = o.broadcast_async(func, tag, data, wait)
            rs.append(r)
        self._llogger.debug(f'loop end for {func}')
        if wait:
            t = self._futures.pop(tag)
            t.join()
            if rs:
                raylink.get(rs)
            self._llogger.debug(f'wait end for {func}')

    def get_id(self, tag):
        """Get parameter id corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            str: Parameter ID
        """
        self.tag_check(tag)
        ret = self._params[tag]['meta']['id']
        return ret

    def get_ids(self, tags):
        """Get parameter id corresponding to the tag.

        Args:
            tags (str): Parameter tag

        Returns:
            str: Parameter ID
        """
        rets = []
        for tag in tags:
            rets.append(self.get_id(tag))
        return rets

    def get_p_info(self, tag):
        """Get the parameter info corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            : Parameter Info
        """
        self.tag_check(tag)
        ret = self._params[tag]['meta']['info']
        return ret

    def get_p_infos(self, tags):
        """Get the parameter info corresponding to the tag.

        Args:
            tags (str): Parameter tag

        Returns:
            : Parameter Info
        """
        rets = []
        for tag in tags:
            rets.append(self.get_p_info(tag))
        return rets

    def get_meta_info(self, tag):
        """Get the parameter meta info corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            : Parameter meta info
        """
        self.tag_check(tag)
        ret = self._params[tag]['meta']
        return ret

    def get_p(self, tag):
        """Get the parameters corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            : Parameters
        """
        self.tag_check(tag)
        ret = self._params[tag]['p']
        return ret

    def get_shm_p(self, tag):
        self.tag_check(tag)
        self._llogger.debug(f'get shm p tag {tag}')
        self._locks[tag].acquire()
        _tmp_params_shm = self._params_shm[tag]
        _cur_idx = _tmp_params_shm['cur']
        _tmp_shm = _tmp_params_shm['shms'][_cur_idx]
        self._locks[tag].release()
        self._llogger.debug(f'get shm p tag {tag} '
                            f'with {_tmp_shm["shm"].name} finished')
        return {
            'name': _tmp_shm['shm'].name,
            'size': _tmp_shm['size'],
        }

    def pull_tag(self, tag: str):
        self.tag_check(tag)
        return self.get_shm_p(tag), self.get_p_info(tag)

    def pull_tags(self, tags: list):
        data = []
        for tag in tags:
            self.tag_check(tag)
            data.append([self.get_shm_p(tag), self.get_p_info(tag)])
        return data

    def tag_check(self, tag):
        if tag not in self._params:
            raise Exception(f"Tag not exists. expect: {tag}, has: {self._params.keys()}")

    def _create_shm(self, tag, p_size):
        self._llogger.debug('creating shared memory')
        _size = int(p_size * self.safe_ratio)
        shm = ShM(create=True, size=_size)
        _tmp_shm = {'shm': shm, 'size': p_size}
        self._llogger.debug(f'created shared memory {shm.name} for {tag}')
        return _tmp_shm
