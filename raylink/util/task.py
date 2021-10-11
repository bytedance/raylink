__all__ = ['CallBackT', 'TaskQueue', 'TaskRunner', 'ThreadPoolExecutorWithLimit']

from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore, Thread, Lock
from typing import Any
import queue
import raylink
import time
from raylink.util.log.utils import *

EMPTY_QUEUE = 'EmptyQueue'


class PriorityQueue(object):
    def __init__(self):
        self._lock = Lock()
        self.queue = {}

    def get(self):
        self._lock.acquire()
        if len(self.queue) == 0:
            self._lock.release()
            return EMPTY_QUEUE
        key = sorted(self.queue.keys())[0]
        q = self.queue[key]
        item = q.get()
        if q.qsize() == 0:
            self.queue.pop(key)
        self._lock.release()
        return item

    def put(self, priority: int, item: Any):
        self._lock.acquire()
        if priority not in self.queue:
            self.queue[priority] = queue.Queue()
        self.queue[priority].put(item)
        self._lock.release()


class TaskQueue(object):
    def __init__(self, limits: dict = None):
        self.daemon = True
        self.tasks = PriorityQueue()
        self.limits = {}
        if limits is not None:
            for k, v in limits.items():
                self.limits[k] = Semaphore(v)

    def get(self):
        v = self.tasks.get()
        if v == EMPTY_QUEUE:
            return None
        name, item = v
        if name in self.limits:
            self.limits[name].release()
        return name, item

    def put(self, name: str, priority: int, item: Any):
        """Put task

        Args:
            name: task name
            priority: larger the number, lower the priority
            item: arguments
        """

        if name in self.limits:
            self.limits[name].acquire()
        self.tasks.put(priority, (name, item))


class TaskRunner(Thread):
    """Only useful when you need to limit to single thread and arrange priority"""

    def __init__(self, parent: raylink.OutlineNode, limits: dict = None):
        super(TaskRunner, self).__init__()
        self.parent = parent
        self.queue = TaskQueue(limits)
        self.result = queue.Queue()
        self.daemon = True
        self.idle_time = 0.01

    def get(self):
        return self.result.get()

    def put(self, name: str, priority: int, item: Any, need_result=False):
        self.queue.put(name=name, priority=priority, item=(item, need_result))

    def run(self) -> None:
        while True:
            v = self.queue.get()
            if v is None:
                time.sleep(self.idle_time)
                continue
            name, item = v
            item, need_result = item
            ret = getattr(self.parent, '_' + name)(**item)
            if need_result:
                self.result.put(ret)


class CallBackT(Thread):
    def __init__(self, size=1, need_result=False):
        super(CallBackT, self).__init__()
        self.daemon = True
        self.sema = Semaphore(size)
        self.lock = Lock()
        self.tasks = []
        self.need_result = need_result
        if need_result:
            self.results = []

    def put(self, future):
        self.sema.acquire()
        self.lock.acquire()
        self.tasks.append(future)
        debug_log(f'put new future, len: {len(self.tasks)}')
        self.lock.release()

    def run(self) -> None:
        while True:
            if len(self.tasks) == 0:
                import time
                time.sleep(0.001)
                continue
            else:
                self.lock.acquire()
                ready, remain = raylink.wait(self.tasks)
                if self.need_result:
                    res = raylink.get(ready[0])
                    self.results.append(res)
                self.tasks = remain
                debug_log(f'remain len: {len(self.tasks)}')
                self.lock.release()
                self.sema.release()

    def task_done(self):
        while len(self.tasks) > 0:
            time.sleep(0.001)


class ThreadPoolExecutorWithLimit(ThreadPoolExecutor):
    def __init__(self, maxsize=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._work_queue = queue.Queue(maxsize=maxsize)
