from unittest import TestCase
from raylink.data.tunnel.server import TunnelServer
from raylink.data.tunnel.client import TunnelProxy
import time


class TestTunnel(TestCase):
    def setUp(self) -> None:
        import numpy as np
        length = 10000000
        self.arr = np.arange(length)

    def test_tunnel_speed(self):
        class fakelogger(object):
            @staticmethod
            def warning(sth):
                print(sth)

            @staticmethod
            def info(sth):
                print(sth)

            @staticmethod
            def error(sth):
                print(sth)

        class FakeNode(object):
            def __init__(self):
                self._ip = '127.0.0.1'
                self._logger = fakelogger()

            def echo(self, a):
                return a

            def ip_(self):
                return self._ip

        server = TunnelServer(FakeNode(), None, True)
        server.start()
        # wait for server
        time.sleep(0.1)
        proxy = TunnelProxy(server.get_tunnel(), local=True, logger=fakelogger)

        st = time.time()
        proxy.echo(self.arr)
        print(time.time() - st)
