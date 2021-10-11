__all__ = ['TunnelProxy']

from raylink.util.task import ThreadPoolExecutorWithLimit as ThreadPoolExecutor
from thrift.protocol import TBinaryProtocol
from .transport import TBufferedTransport
from thrift.transport import TSocket
from .rpc.rpc.tunnel import Client
from raylink.util.util import get_ip
from threading import Lock
import raylink
import uuid
import time
import sys


class TunnelProxy(object):
    def __init__(self, tunnel_info, local=True, debug=False, logger=None):
        self._args = {
            'tunnel_info': tunnel_info,
            'local': local,
            'debug': debug,
            'logger': logger
        }
        self.__dict__.update(self._args)
        self._is_setup = False
        self.uid = ''
        self.lock = None
        self.setup_lock = Lock()

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.setup_lock = Lock()
        if self.logger is None:
            self.logger = raylink.get_llogger()
        if self._is_setup:
            self._setup(force=True)

    def __getstate__(self):
        import copy
        state = copy.deepcopy(self._args)
        state['_is_setup'] = self._is_setup
        return state

    def _setup(self, force=False):
        self.setup_lock.acquire()
        if self._is_setup and not force:
            self.logger.debug(f'no setup for {id(self)}')
            self.setup_lock.release()
            return
        self.logger.debug(f'setup {id(self)}')
        self._is_setup = True
        self.lock = Lock()
        if self.local and get_ip() == self.tunnel_info.ip:
            self.host = '127.0.0.1'
        else:
            self.host = self.tunnel_info.ip
        self.port = self.tunnel_info.port
        self.p = self.tunnel_info.pickler
        self._try_connect()
        self._pool = ThreadPoolExecutor(max_workers=5)
        self.uid = uuid.uuid4()
        self.logger.debug(f'setup {id(self)} end')
        self.setup_lock.release()

    def _try_connect(self):
        flag = False
        while not flag:
            try:
                self._connect_to_server()
                flag = True
            except Exception as e:
                import traceback
                traceback.print_exc()
                print('retry...')

    def _connect_to_server(self):
        tsocket = TSocket.TSocket(self.host, self.port)
        transport = TBufferedTransport(tsocket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self._client = Client(protocol)
        self.transport = transport
        self.transport.open()
        self._client.incr_client_num()

    def _submit_task(self, func, args, kwargs):
        st = time.time()
        _kwargs = self.p.c2s_dumps(func, *args, **kwargs)
        self.lock.acquire()
        result = self._client.task(func, _kwargs)
        self.lock.release()
        result = self.p.s2c_loads(func, result)
        self.logger.debug(f'{func} takes {time.time() - st}')
        return result

    def submit_task(self, func, args, kwargs):
        try:
            result = self._submit_task(func, args, kwargs)
            return result
        except Exception as e:
            print(f'Error while running {func}', file=sys.stderr)
            raise e

    def _submit_task_d(self, func, args, kwargs):
        self.logger.debug(f'enter {func}')
        import time
        st = time.time()
        _kwargs = self.p.c2s_dumps(func, *args, **kwargs)
        rt = time.time() - st
        self.logger.debug(f'{func} c2s_dumps end in {rt}')
        self.logger.debug(f'acquire {id(self.lock)}')
        self.lock.acquire()
        st = time.time()
        result = self._client.task(func, _kwargs)
        rt = time.time() - st
        self.logger.debug(f'release {id(self.lock)}')
        self.lock.release()
        self.logger.debug(f'{func} client end in {rt}, {args} {kwargs}')
        st = time.time()
        result = self.p.s2c_loads(func, result)
        rt = time.time() - st
        self.logger.debug(f'{func} s2c_loads end in {rt}')
        self.logger.debug(f'exit {func}')
        return result

    def submit_task_d(self, func, args, kwargs):
        try:
            return self._submit_task_d(func, args, kwargs)
        except Exception as e:
            print(f'Error while running {func}', file=sys.stderr)
            raise e

    def __getattr__(self, func):
        self._setup()
        try:
            return super(TunnelProxy, self).__getattr__(func)
        except AttributeError:
            pass

        def tunnel_api(*args, **kwargs):
            _func = func
            return self.submit_task(_func, args, kwargs)

        def tunnel_async_api(*args, **kwargs):
            _func = func[:-6]
            return self._pool.submit(self.submit_task, _func, args, kwargs)

        def tunnel_api_d(*args, **kwargs):
            _func = func
            return self.submit_task_d(_func, args, kwargs)

        def tunnel_async_api_d(*args, **kwargs):
            _func = func[:-6]
            return self._pool.submit(self.submit_task_d, _func, args, kwargs)

        if self.debug:
            if func.endswith('_async'):
                return tunnel_async_api_d
            return tunnel_api_d
        if func.endswith('_async'):
            return tunnel_async_api
        return tunnel_api

    def __del__(self):
        if self._is_setup:
            self._client.decr_client_num()
            self.transport.close()

    def get_client_num(self):
        self._setup()
        return self._client.get_client_num()
