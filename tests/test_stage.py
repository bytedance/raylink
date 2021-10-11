from unittest import TestCase
from raylink.data.stage import GPUStage
import numpy as np
import torch
import time


class TestStage(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.shape = [20, 1024, 1024]
        cls.num = 50
        cls.zero = np.zeros(cls.shape)
        cls.array = [np.empty(cls.shape) for _ in range(cls.num)]
        cls.stage = GPUStage(cls.zero)

    def test_nothing(self):
        # setup takes 1.344s
        pass

    def test_normal_tensor(self):
        # normal copy takes 4.840s
        # real time 3.496s
        st = time.time()
        zero = torch.tensor(self.zero).cuda()
        for a in self.array:
            t = torch.tensor(a).cuda()
            zero += t
        print(zero)
        print(time.time() - st)

    def test_gpu_stage(self):
        # GPU stage copy takes 1.694s
        # real time 0.35s
        st = time.time()
        zero = torch.tensor(self.zero).cuda()
        for a in self.array:
            self.stage.put(a)
        for _ in range(self.num):
            t = self.stage.acquire()
            zero += t
            self.stage.release()
        print(zero)
        print(time.time() - st)
