__all__ = ['Logger']

import logging

import raylink
import raylink.util.path as _p
from raylink.util.log.utils import *

DEFAULT_LOG_CFG = {
    'level': (logging.INFO, logging.DEBUG),
    'enable': (True, True)
}


class Logger(raylink.OutlineNode):
    TYPE = 'logger'

    def setup(self, log_cfg=None):
        from torch.utils.tensorboard import SummaryWriter
        if log_cfg is None:
            log_cfg = DEFAULT_LOG_CFG
        if 'comment' not in log_cfg:
            comment = ''
        else:
            comment = log_cfg['comment']
        if 'tensorboard' not in log_cfg:
            tensorboard = {}
        else:
            tensorboard = log_cfg['tensorboard']
        self.log_cfg = log_cfg
        self.remote_log_cfg = {
            'level': self.log_cfg['level'],
            'enable': (False, log_cfg['enable'][1])
        }
        self.base_dir = _p.get_run_rel_path(comment)
        logs_dir = get_logs_path(self.base_dir)
        self.logs_dir = _p.get_path(logs_dir)
        self.base_abs_dir = _p.get_path(self.base_dir)
        self.writer = SummaryWriter(log_dir=self.base_abs_dir, **tensorboard)
        self.loggers = {}

    def get_log_info(self):
        return {
            'base_dir': self.base_abs_dir,
        }

    def init_logger(self, node_name, node_path):
        logger_name = get_logger_name(node_name, node_path)
        if logger_name in self.loggers:
            return logger_name
        logs_dir = get_logger_path(self.logs_dir, node_path)
        self.loggers[logger_name] = init_logger(
            logger_name, node_name, logs_dir, **self.remote_log_cfg)
        return logger_name

    def tb_api(self, func, *args, **kwargs):
        assert callable(getattr(self.writer, func))
        getattr(self.writer, func)(*args, **kwargs)

    def log_api(self, name, func, *args, **kwargs):
        logger = self.loggers[name]
        assert callable(getattr(logger, func))
        getattr(logger, func)(*args, **kwargs)

    def get_local_cfg(self):
        return {
            'base_dir': self.base_dir,
            'log_cfg': self.log_cfg,
            'remote_ip': self._ip
        }
