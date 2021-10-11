__all__ = ['init_logger', 'find_caller', 'get_logger_path', 'get_logs_path',
           'get_logger_name', 'sprint', 'logtime', 'debug_log']

from functools import wraps
import logging
import time
import copy
import sys
import os

import raylink
from raylink.util.path import get_path

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
RESET_SEQ = '\033[0m'
COLOR_SEQ = '\033[1;%dm'
BOLD_SEQ = '\033[1m'
COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED,
    'RED': RED,
    'GREEN': GREEN,
    'YELLOW': YELLOW,
    'BLUE': BLUE,
    'MAGENTA': MAGENTA,
    'CYAN': CYAN,
    'WHITE': WHITE,
}


class ColoredFormatter(logging.Formatter):
    def __init__(self, msg):
        logging.Formatter.__init__(self, msg)

    def format(self, record):
        record = copy.copy(record)
        levelname = record.levelname
        if levelname in COLORS:
            levelname_color = COLOR_SEQ % (
                    30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        message = logging.Formatter.format(self, record)
        message = message.replace('$RESET', RESET_SEQ) \
            .replace('$BOLD', BOLD_SEQ)
        for k, v in COLORS.items():
            message = message.replace('$' + k, COLOR_SEQ % (v + 30)) \
                .replace('$BG' + k, COLOR_SEQ % (v + 40)) \
                .replace('$BG-' + k, COLOR_SEQ % (v + 40))
        return message + RESET_SEQ


def init_logger(name, file_name, path=None, level=(logging.INFO, logging.DEBUG),
                enable=(True, True)):
    """Initialize a logger with certain name
    Args:
        name (str): Logger name
        file_name (str): Logger file name
        path (str): Optional, specify which folder path
            the log file will be stored, for example
            '/tmp/log'
        level (tuple): Optional, consist of two logging level.
            The first stands for logging level of console handler,
            and the second stands for logging level of file handler.
        enable (tuple): Optional, define whether each handler is enabled.
            The first enables console handler,
            and the second enables file handler.
    Returns:
        logging.Logger: logger instance
    """
    import logging.handlers
    import sys
    import types
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = 0
    if path:
        path = get_path(path)
        path += '/' + file_name + '.log'
    else:
        path = get_path('log') + '/' + file_name + '.log'

    _nf = ['[%(asctime)s]',
           '[%(name)15s]',
           '[%(filename)10s:%(funcName)10s:%(lineno)3d]',
           '[%(levelname)s]',
           ' %(message)s']
    nformatter = logging.Formatter('-'.join(_nf))

    if enable[0]:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level[0])
        ch.setFormatter(nformatter)
        logger.addHandler(ch)

    if enable[1]:
        rf = logging.handlers.RotatingFileHandler(
            path, maxBytes=50 * 1024 * 1024, backupCount=5)
        rf.setLevel(level[1])
        rf.setFormatter(nformatter)
        logger.addHandler(rf)

    def _log(self, level, msg, args, exc_info=None, extra=None, rv=None):
        """
        Low-level logging routine which creates a LogRecord and then calls
        all the handlers of this logger to handle the record.
        """
        from logging import sys, _srcfile
        sinfo = None
        if _srcfile:
            # IronPython doesn't track Python frames, so findCaller raises an
            # exception on some versions of IronPython. We trap it here so that
            # IronPython can use logging.
            if rv:
                fn, lno, func, sinfo = rv
            else:
                fn, lno, func = "(unknown file)", 0, "(unknown function)"
        else:  # pragma: no cover
            fn, lno, func = "(unknown file)", 0, "(unknown function)"
        if exc_info:
            if isinstance(exc_info, BaseException):
                exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
            elif not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
        record = self.makeRecord(self.name, level, fn, lno, msg, args,
                                 exc_info, func, extra, sinfo)
        self.handle(record)

    func_type = types.MethodType
    logger._log = func_type(_log, logger)
    return logger


def find_caller(f):
    from logging import os, _srcfile
    rv = "(unknown file)", 0, "(unknown function)", None
    while hasattr(f, "f_code"):
        co = f.f_code
        filename = os.path.normcase(co.co_filename)
        if filename == _srcfile:
            f = f.f_back
            continue
        sinfo = None
        rv = (co.co_filename, f.f_lineno, co.co_name, sinfo)
        break
    return rv


def sprint(*args, **kwargs):
    import sys
    frame = sys._getframe(kwargs.pop('limit', 1))
    rv = find_caller(frame)
    print(f'[{rv[0]}:{rv[2]}:{rv[1]}] ', *args, **kwargs)


def get_logger_name(node_name, node_path):
    parent_path = os.path.dirname(node_path)
    p_idx = '/'.join([p.split('-')[-1] for p in parent_path.split('/')])
    if p_idx == '':
        return node_name
    return p_idx + '/' + node_name


def get_logger_path(logs_dir, node_path):
    parent_path = os.path.dirname(node_path)
    return os.path.join(logs_dir, parent_path)


def get_logs_path(base_dir):
    return os.path.join(base_dir, 'logs')


class logtime(object):
    def __init__(self, method, level, msg=None, index=-1):
        self.method = method
        self.level = level
        if msg is None:
            self.msg = 'logtime: {dur:.3f}s'
        else:
            self.msg = msg
        self.index = index
        self.__name__ = method.__name__
        self.__doc__ = method.__doc__
        frame = sys._getframe(3)
        rv = list(find_caller(frame))
        rv[2] = self.method.__name__
        self.rv = tuple(rv)

    def __call__(self, *args, **kwargs):
        st = time.time()
        ret = self.method(*args, **kwargs)
        dur = time.time() - st
        f_str = f"f'{self.msg}'"
        if 'self.' in f_str:
            f_str = f_str.replace('self.', 'obj.')
            obj = args[0]
            if 0 <= self.index != obj._index:
                return ret
        msg = eval(f_str)
        getattr(raylink.get_logger(), self.level)(msg=msg, rv=self.rv)
        return ret

    @staticmethod
    def wrap(f, level, msg=None, index=-1):
        logger = logtime(f, level, msg, index)

        @wraps(f)
        def wrapper(*args, **kwargs):
            return logger(*args, **kwargs)

        return wrapper

    @staticmethod
    def debug(obj, index=-1):
        if isinstance(obj, str):
            return lambda f: logtime.wrap(f, 'debug', obj, index)
        return logtime.wrap(obj, 'debug', index=index)

    @staticmethod
    def info(obj, index=-1):
        if isinstance(obj, str):
            return lambda f: logtime.wrap(f, 'info', obj, index)
        return logtime.wrap(obj, 'info', index=index)


def debug_log(log):
    import os
    from datetime import datetime
    os.system(f'echo "[{datetime.now()}]{log}" >> ~/.raylink/debug.log')
