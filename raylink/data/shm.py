from multiprocessing.shared_memory import SharedMemory
import mmap
import os
import errno
import secrets

if os.name == "nt":
    import _winapi

    _USE_POSIX = False
else:
    import _posixshmem

    _USE_POSIX = True

_O_CREX = os.O_CREAT | os.O_EXCL

# FreeBSD (and perhaps other BSDs) limit names to 14 characters.
_SHM_SAFE_NAME_LENGTH = 14

# Shared memory block name prefix
if _USE_POSIX:
    _SHM_NAME_PREFIX = '/psm_'
else:
    _SHM_NAME_PREFIX = 'wnsm_'


def _make_filename():
    "Create a random filename for the shared memory object."
    # number of random bytes to use for name
    nbytes = (_SHM_SAFE_NAME_LENGTH - len(_SHM_NAME_PREFIX)) // 2
    assert nbytes >= 2, '_SHM_NAME_PREFIX too long'
    name = _SHM_NAME_PREFIX + secrets.token_hex(nbytes)
    assert len(name) <= _SHM_SAFE_NAME_LENGTH
    return name


# This shm wouldn't free memory when only attach
class ShM(SharedMemory):
    def __init__(self, name=None, create=False, size=0):
        if not size >= 0:
            raise ValueError("'size' must be a positive integer")
        if create:
            self._flags = _O_CREX | os.O_RDWR
        if name is None and not self._flags & os.O_EXCL:
            raise ValueError("'name' can only be None if create=True")

        if _USE_POSIX:

            # POSIX Shared Memory

            if name is None:
                while True:
                    name = _make_filename()
                    try:
                        self._fd = _posixshmem.shm_open(
                            name,
                            self._flags,
                            mode=self._mode
                        )
                    except FileExistsError:
                        continue
                    self._name = name
                    break
            else:
                name = "/" + name if self._prepend_leading_slash else name
                self._fd = _posixshmem.shm_open(
                    name,
                    self._flags,
                    mode=self._mode
                )
                self._name = name
            try:
                if create and size:
                    os.ftruncate(self._fd, size)
                stats = os.fstat(self._fd)
                size = stats.st_size
                self._mmap = mmap.mmap(self._fd, size)
            except OSError:
                self.unlink()
                raise

            if create:
                from multiprocessing.resource_tracker import register
                register(self._name, "shared_memory")

        else:

            # Windows Named Shared Memory

            if create:
                while True:
                    temp_name = _make_filename() if name is None else name
                    # Create and reserve shared memory block with this name
                    # until it can be attached to by mmap.
                    h_map = _winapi.CreateFileMapping(
                        _winapi.INVALID_HANDLE_VALUE,
                        _winapi.NULL,
                        _winapi.PAGE_READWRITE,
                        (size >> 32) & 0xFFFFFFFF,
                        size & 0xFFFFFFFF,
                        temp_name
                    )
                    try:
                        last_error_code = _winapi.GetLastError()
                        if last_error_code == _winapi.ERROR_ALREADY_EXISTS:
                            if name is not None:
                                raise FileExistsError(
                                    errno.EEXIST,
                                    os.strerror(errno.EEXIST),
                                    name,
                                    _winapi.ERROR_ALREADY_EXISTS
                                )
                            else:
                                continue
                        self._mmap = mmap.mmap(-1, size, tagname=temp_name)
                    finally:
                        _winapi.CloseHandle(h_map)
                    self._name = temp_name
                    break

            else:
                self._name = name
                # Dynamically determine the existing named shared memory
                # block's size which is likely a multiple of mmap.PAGESIZE.
                h_map = _winapi.OpenFileMapping(
                    _winapi.FILE_MAP_READ,
                    False,
                    name
                )
                try:
                    p_buf = _winapi.MapViewOfFile(
                        h_map,
                        _winapi.FILE_MAP_READ,
                        0,
                        0,
                        0
                    )
                finally:
                    _winapi.CloseHandle(h_map)
                size = _winapi.VirtualQuerySize(p_buf)
                self._mmap = mmap.mmap(-1, size, tagname=name)

        self._size = size
        self._buf = memoryview(self._mmap)
