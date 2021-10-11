__all__ = ['TunnelServer', 'TunnelInfo']

from .transport import TBufferedTransportFactory
from thrift.protocol import TBinaryProtocol
from .rpc.rpc.tunnel import Processor
from thrift.transport import TSocket
from collections import namedtuple
from thrift.server import TServer
from threading import Thread, Lock
import portpicker
import _thread
import time
import sys

from .pickler import Pickler

TunnelInfo = namedtuple('TunnelInfo', ['ip', 'port', 'pickler'])


class Handler(object):
    def __init__(self, server, node):
        self.server = server
        self.node = node
        self.p = server.pickler
        self.debug_mode = server.debug_mode
        if hasattr(node, '_llogger'):
            self.logger = node._llogger
        else:
            self.logger = None
        self.client_num = 0

    def _task(self, func, _kwargs):
        if self.debug_mode and self.logger:
            self.logger.debug(f'enter {func}')
        if self.debug_mode and self.logger:
            st = time.time()
        args, kwargs = self.p.c2s_loads(func, _kwargs)
        if self.debug_mode and self.logger:
            rt = time.time() - st
            self.logger.debug(f'{func} c2s_loads end in {rt}')
            st = time.time()
        result = getattr(self.node, func)(*args, **kwargs)
        if self.debug_mode and self.logger:
            rt = time.time() - st
            self.logger.debug(f'{func} server end in {rt}, {args} {kwargs}')
            st = time.time()
        result = self.p.s2c_dumps(func, result)
        if self.debug_mode and self.logger:
            rt = time.time() - st
            self.logger.debug(f'{func} s2c_dumps end in {rt}')
        if self.debug_mode and self.logger:
            self.logger.debug(f'exit {func}')
        return result

    def task(self, func, _kwargs):
        try:
            return self._task(func, _kwargs)
        except Exception as e:
            args, kwargs = self.p.c2s_loads(func, _kwargs)
            print(f'Error while running {func}, {args}, {kwargs}', file=sys.stderr)
            raise e

    def incr_client_num(self):
        self.client_num += 1
        self.logger.debug(f'current client num {self.client_num}')

    def decr_client_num(self):
        self.client_num -= 1
        self.logger.debug(f'current client num {self.client_num}')

    def get_client_num(self):
        return self.client_num


class TunnelServer(Thread):
    def __init__(self, parent, pickler, debug=False):
        super(TunnelServer, self).__init__()
        self.daemon = True
        self.lock = Lock()
        self._lock()
        self.node = parent
        if pickler is None:
            self.pickler = Pickler
        else:
            self.pickler = pickler
        self.debug_mode = debug
        self.host = '0.0.0.0'
        self.max_try = 20
        self.handler = None

    def start_server(self):
        self.port = portpicker.pick_unused_port()
        self.handler = Handler(self, self.node)
        processor = Processor(self.handler)
        transport = TSocket.TServerSocket(self.host, self.port)
        tfactory = TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        rpc_server = TServer.TThreadedServer(
            processor, transport, tfactory, pfactory)

        rpc_server.serve()

    def get_client_num(self):
        return self.handler.get_client_num()

    def get_tunnel(self):
        while self.lock.locked(): time.sleep(0.02)
        return self._get_tunnel()

    def _get_tunnel(self):
        return TunnelInfo(ip=self.node.ip_(), port=self.port, pickler=self.pickler)

    def get_tunnel_stats(self):
        return {'conn_num': self.get_client_num()}

    def run(self):
        self.run_flag = False
        i = 0
        while not self.run_flag and i < self.max_try:
            try:
                self.start_server()
                self.run_flag = True
            except:
                i += 1
                time.sleep(0.02)
        import traceback
        traceback.print_exc()

    def _lock(self):
        self.lock.acquire()

    def _unlock(self):
        self.lock.release()

    def exit(self):
        _thread.exit()
