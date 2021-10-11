from thrift.transport.TTransport import CReadableTransport, BufferIO
import io


class TTransportBase(object):
    """Base class for Thrift transport layer."""

    def isOpen(self):
        pass

    def open(self):
        pass

    def close(self):
        pass

    def read(self, sz):
        pass

    def readAll(self, sz):
        if not hasattr(self, '_readall_buffer'):
            self._readall_buffer = io.BytesIO()
        self._readall_buffer.seek(0, 0)
        buff = self._readall_buffer
        have = 0
        while (have < sz):
            chunk = self.read(sz - have)
            chunkLen = len(chunk)
            have += chunkLen
            buff.write(chunk)

            if chunkLen == 0:
                raise EOFError()

        return buff.getvalue()[:buff.tell()]

    def write(self, buf):
        pass

    def flush(self):
        pass


class TBufferedTransport(TTransportBase, CReadableTransport):
    """Class that wraps another transport and buffers its I/O.

    The implementation uses a (configurable) fixed-size read buffer
    but buffers all writes until a flush is performed.
    """
    DEFAULT_BUFFER = 4096

    def __init__(self, trans, rbuf_size=DEFAULT_BUFFER):
        self.__trans = trans
        self.__wbuf = BufferIO()
        # Pass string argument to initialize read buffer as cStringIO.InputType
        self.__rbuf = BufferIO(b'')
        self.__rbuf_size = rbuf_size

    def isOpen(self):
        return self.__trans.isOpen()

    def open(self):
        return self.__trans.open()

    def close(self):
        return self.__trans.close()

    def read(self, sz):
        ret = self.__rbuf.read(sz)
        if len(ret) != 0:
            return ret
        self.__rbuf = BufferIO(self.__trans.read(max(sz, self.__rbuf_size)))
        return self.__rbuf.read(sz)

    def write(self, buf):
        try:
            self.__wbuf.write(buf)
        except Exception as e:
            # on exception reset wbuf so it doesn't contain a partial function call
            self.__wbuf = BufferIO()
            raise e

    def flush(self):
        out = self.__wbuf.getvalue()
        # reset wbuf before write/flush to preserve state on underlying failure
        self.__wbuf = BufferIO()
        self.__trans.write(out)
        self.__trans.flush()

    # Implement the CReadableTransport interface.
    @property
    def cstringio_buf(self):
        return self.__rbuf

    def cstringio_refill(self, partialread, reqlen):
        retstring = partialread
        if reqlen < self.__rbuf_size:
            # try to make a read of as much as we can.
            retstring += self.__trans.read(self.__rbuf_size)

        # but make sure we do read reqlen bytes.
        if len(retstring) < reqlen:
            retstring += self.__trans.readAll(reqlen - len(retstring))

        self.__rbuf = BufferIO(retstring)
        return self.__rbuf


class TBufferedTransportFactory(object):
    """Factory transport that builds buffered transports"""

    def getTransport(self, trans):
        buffered = TBufferedTransport(trans)
        return buffered
