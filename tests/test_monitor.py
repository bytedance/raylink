from unittest import TestCase
import raylink.util.monitor as um


class TestMonitor(TestCase):
    def test_cpu(self):
        print(um.HWM.cpu())

    def test_mem(self):
        print(um.HWM.mem())

    def test_disk(self):
        print(um.HWM.disk())

    def test_proc(self):
        import time
        um.HWM.proc()
        time.sleep(1)
        print(um.HWM.proc())

    def test_proc_mem(self):
        import numpy as np
        p = um.ProcMem()
        a = []
        for _ in range(10):
            p.snapshot(True)
            a.append(np.zeros((500, 500, 10)))
            p.snapshot()
            a.append(np.zeros((50, 500, 10)))
            p.snapshot()
        print(p.stat())
