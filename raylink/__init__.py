from raylink.node import *
from raylink.data import *
from raylink.util.log.utils import logtime, sprint
import ray

__version__ = '0.2.10'
put = ray.put


def get(fs, timeout=None):
    import concurrent.futures
    if isinstance(fs, list) and isinstance(fs[0], concurrent.futures.Future):
        results = []
        for f in concurrent.futures.as_completed(fs, timeout):
            results.append(f.result())
        return results
    if isinstance(fs, concurrent.futures.Future):
        return fs.result(timeout)
    return ray.get(fs, timeout=timeout)


def wait(fs, timeout=None):
    import concurrent.futures
    if isinstance(fs, list) and isinstance(fs[0], concurrent.futures.Future):
        futures = concurrent.futures.wait(
            fs, timeout, concurrent.futures.FIRST_COMPLETED)
        return list(futures.done), list(futures.not_done)
    if isinstance(fs, concurrent.futures.Future):
        if fs.done():
            return [fs], []
    return ray.wait(fs, timeout=timeout)


remote = ray.remote
