__all__ = ['PS']

from raylink.data.shm import ShM
import raylink.constants as kc
import raylink


class PS(object):
    def __init__(self, node, ps_captain, ps_officer):
        self.node = node
        self.ps_captain = ps_captain
        self.ps_officer = ps_officer
        self._future = None

    def _access_shm(self, info):
        name, size = info['name'], info['size']
        shm = ShM(name=name)
        data = bytes(shm.buf[:size])
        shm.close()
        return data

    def list_tags(self):
        return self.ps_officer.list_tags_t()

    def tags_info(self):
        return self.ps_officer.tags_info_t()

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

            >>> self._ps.push('hrl/policy-0', b'byte_weights', {'learn_step': 1})
        """
        if wait:
            self.ps_captain.push_t(tag, params, info, wait=wait,
                                   _id=_id, _time=_time, _count=_count)
            return
        self.ps_captain.push_t_async(tag, params, info, wait=wait,
                                     _id=_id, _time=_time, _count=_count)

    def pull_tag(self, tag):
        """Get the parameters and info corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            tuple(bytes, dict): Parameters and info
        """
        shm_info, info = self.ps_officer.pull_tag_t(tag)
        return self._access_shm(shm_info), info

    def pull_tags(self, tags):
        """Get the parameters and info corresponding to the tag.

        Args:
            tags (str): Parameter tag

        Returns:
            tuple(bytes, dict): Parameters and info
        """
        data = self.ps_officer.pull_tags_t(tags)
        for idx in range(len(data)):
            shm_info = data[idx][0]
            data[idx][0] = self._access_shm(shm_info)
        return data

    def duplicate(self, old_tag, new_tag, info=None,
                  _id=None, _time=None, _count=None, wait=True):
        """Duplicate a tag to a new tag.

        Args:
            old_tag: tag to be duplicated
            new_tag: new tag
            wait: whether wait for broadcasting to officers
        """
        self.ps_captain.duplicate(
            old_tag, new_tag, info, _id, _time, _count, wait)

    def delete(self, tag, wait=True):
        """Delete a tag and its model

        Args:
            tag (str): Parameter tag
            wait: whether wait for broadcasting to officers
        """
        self.ps_captain.delete(tag, wait)

    def get_id(self, tag):
        """Get parameter id corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            str: Parameter ID
        """
        return self.ps_officer.get_id_t(tag)

    def get_ids(self, tags):
        """Get parameter id corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            str: Parameter ID
        """
        return self.ps_officer.get_ids_t(tags)

    def get_p_info(self, tag):
        """Get the parameter info corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            : Parameter Info
        """
        return self.ps_officer.get_p_info_t(tag)

    def get_p_infos(self, tags):
        """Get the parameter info corresponding to the tag.

        Args:
            tags (str): Parameter tag

        Returns:
            : Parameter Info
        """
        return self.ps_officer.get_p_infos_t(tags)

    def get_meta_info(self, tag):
        """Get the parameter meta info corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            : Parameter meta info
        """
        return self.ps_officer.get_meta_info_t(tag)

    def get_p(self, tag):
        """Get the parameters corresponding to the tag.

        Args:
            tag (str): Parameter tag

        Returns:
            : Parameters
        """
        shm_info = self.ps_officer.get_shm_p_t(tag)
        try:
            p = self._access_shm(shm_info)
            return p
        except:
            import sys
            import traceback
            print(f'ERROR {self.node._node_cfg} can not get p from '
                  f'{self.ps_officer.node_cfg_()}', file=sys.stderr)
            traceback.print_exc()

    def save_model(self, tag, path):
        self._finish_future()
        f = self.ps_captain.save_model_async(tag, path)
        self._future = f

    def save_models(self, step):
        self._finish_future()
        f = self.ps_captain.save_models_async(step)
        self._future = f

    def load_models(self, model_path=kc.SAVED_MODEL):
        return self.ps_captain.load_models(model_path)

    def _finish_future(self):
        if self._future is None:
            return
        raylink.get(self._future)
