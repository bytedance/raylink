__all__ = ['GPUStage']

from threading import Thread, Lock, Condition
from queue import Queue
import numpy as np
import torch


class _CopyThread(Thread):
    def __init__(self, queue: Queue, cuda_tensor: torch.Tensor):
        super(_CopyThread, self).__init__()
        self.daemon = True
        self.q = queue
        self.ct = cuda_tensor
        self.cond = Condition(Lock())

    def run(self) -> None:
        while True:
            self.cond.acquire()
            self.cond.wait()
            self.cond.release()
            data = self.q.get()
            self.ct.copy_(data)


class GPUStage(object):
    def __init__(self, buff_example, buff_size=2, device='cuda'):
        self.buff_example = buff_example
        self.buff_size = buff_size
        self.device = device
        self.queue = Queue()
        self.buffers = []
        self.threads = []
        self.in_use_index = 0
        self._lock = Lock()
        self.setup()

    def setup(self):
        for _ in range(self.buff_size):
            t = torch.tensor(self.buff_example)
            t = t.pin_memory()
            ct = t.to(self.device)
            self.buffers.append(ct)
            thread = _CopyThread(self.queue, ct)
            thread.start()
            self.threads.append(thread)

    def put(self, data):
        if isinstance(data, np.ndarray):
            data = torch.from_numpy(data)
        self.queue.put(data)

    def acquire(self):
        self._lock.acquire()
        return self.buffers[self.in_use_index]

    def release(self):
        self.threads[self.in_use_index].cond.acquire()
        self.threads[self.in_use_index].cond.notify()
        self.threads[self.in_use_index].cond.release()
        self.in_use_index = (self.in_use_index + 1) % self.buff_size
        self._lock.release()
