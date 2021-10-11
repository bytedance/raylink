__all__ = ['Queue']

import numpy as np
import raylink
import queue


class Queue(raylink.OutlineNode):
    TYPE = 'queue'

    def setup(self, size):
        self._buffer = queue.Queue(maxsize=size)
        self._batch_size = None

    def put(self, data: dict, timeout=1):
        """Expect using tunnel proxy to put data"""
        try:
            self._buffer.put(data, timeout=timeout)
        except queue.Full:
            pass

    def get(self, size):
        """Expect using tunnel proxy to get batch"""
        data = {}
        for _ in range(size):
            d = self._buffer.get()
            for k, v in d.items():
                if k not in data:
                    data[k] = []
                data[k].append(v)
        for k in data:
            data[k] = np.stack(data[k])
        return data
