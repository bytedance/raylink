__all__ = ['LocalLogAPI', 'RemoteLogAPI']

from pathlib import Path
import sys

import raylink.util.path as _p
from raylink.util.util import get_ip
from raylink.util.log.utils import *


class LogAPI(object):
    def __init__(self, node_name, node_path, logger, local=True):
        """Initialize local logger

        Args:
            node_name: node name
            node_path: node path
            logger: logger of the node
        """
        self._logger = logger
        cfg = self._logger.get_local_cfg()
        self.base_dir, self.log_cfg = cfg['base_dir'], cfg['log_cfg']
        logs_dir = get_logs_path(self.base_dir)
        self.logs_dir = _p.get_path(logs_dir)
        self._logger_name = self._logger.init_logger(node_name, node_path)
        logs_dir = get_logger_path(self.logs_dir, node_path)
        if not local and get_ip() == cfg['remote_ip']:
            self.log_cfg['enable'] = (self.log_cfg['enable'][0], False)
        self._local_logger = init_logger(
            self._logger_name, node_name, logs_dir, **self.log_cfg)
        self.make_symbol_link()

    def make_symbol_link(self):
        # link the latest
        log_path = Path(self.logs_dir).parent
        latest = log_path.parent / 'latest'
        if latest.resolve() == log_path:
            return
        latest.unlink(missing_ok=True)
        latest.symlink_to(log_path)

    def __getattr__(self, func):
        if 'add_' in func or 'flush' == func:
            raise NotImplementedError()

        elif func in ['debug', 'info', 'warning', 'error',
                      'critical', 'exception']:
            frame = sys._getframe(1)
            rv = find_caller(frame)

            def log_api(msg, **kwargs):
                if 'rv' in kwargs:
                    _rv = kwargs.pop('rv')
                else:
                    _rv = rv
                assert callable(getattr(self._local_logger, func))
                getattr(self._local_logger, func)(msg=msg, rv=_rv, **kwargs)

            return log_api
        raise AttributeError


class LocalLogAPI(LogAPI):
    def __init__(self, node_name, node_path, logger):
        super(LocalLogAPI, self).__init__(node_name + '-L', node_path, logger)


class RemoteLogAPI(LogAPI):
    def __init__(self, node_name, node_path, logger):
        from raylink.util.task import CallBackT
        super(RemoteLogAPI, self).__init__(node_name + '-R', node_path, logger, False)
        self.callback_t = CallBackT(10)
        self.callback_t.start()

    def __getattr__(self, func):
        if 'add_' in func or 'flush' == func:
            def tb_api(*args, **kwargs):
                f = self._logger.tb_api_async(func, *args, **kwargs)
                debug_log('call tb api')
                self.callback_t.put(f)

            return tb_api
        elif func in ['debug', 'info', 'warning', 'error',
                      'critical', 'exception']:
            frame = sys._getframe(1)
            rv = find_caller(frame)

            def log_api(msg, **kwargs):
                if 'rv' in kwargs:
                    _rv = kwargs.pop('rv')
                else:
                    _rv = rv
                f = self._logger.log_api_async(
                    self._logger_name, func, msg=msg, rv=_rv, **kwargs)
                assert callable(getattr(self._local_logger, func))
                getattr(self._local_logger, func)(msg=msg, rv=_rv, **kwargs)
                self.callback_t.put(f)

            return log_api
        raise AttributeError
