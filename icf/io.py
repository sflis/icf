import struct
import binascii
from .utils import get_si_prefix
from datetime import datetime
import os
from typing import Union
import numpy as np
import bz2



###Raw object IO classes#####


class IndexedContainerWriter:
    """ Acts as a file object for writing chunks of serialized data
        to file. Prepends each chunk with:
        chunk length in bytes (4 bytes)
        and a crc32 hash      (4 bytes)
    """

    _protocols = {}

    def __init__(self, filename: str, protocol=1, compressor="bz2", **kwargs):
        self._writer = IndexedContainerWriter._protocols[protocol](
            filename, compressor=compressor, **kwargs
        )
        self.protocol = protocol

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._writer.close()

    def write(self, data: bytes):
        """ Writes a stream of bytes to file

            args:
                data (bytes): bytes to be writen to file
        """
        self._writer.write(data)

    def close(self):
        self._writer.close()

    @classmethod
    def _register(cls, scls):
        cls._protocols[scls._protocol_v] = scls
        return scls

    @property
    def data_counter(self):
        return self._writer.data_counter


@IndexedContainerWriter._register
class IndexedContainerWriterV0:
    """ Acts as a file object for writing chunks of serialized data
        to file. Prepends each chunk with:
        chunk length in bytes (4 bytes)
        and a crc32 hash      (4 bytes)

        The file header is 24 bytes long and has the following layout

            bytes:      field:
            0-7         Custom field for file format specifications (set by the header parameter)
            8-11        Protocol version
            12-15       Not used
            16-19       Not used
            20-23       Not used

        General file structure:
            +-------------+
            | File Header |
            +-------------+
            | Chunk Header|
            +-------------+
            |     Data    |
            +-------------+
            | Chunk Header|
            +-------------+
            |     Data    |
            +-------------+
                  ...
                  ...

    """

    _protocol_v = 0
    _chunk_header = struct.Struct("<2I")
    _file_header = struct.Struct("<Q4I")
    def __init__(self, filename: str, header: int = 0):
        self.filename = filename
        self.file = open(self.filename, "wb")
        self.data_counter = 0
        self.version = 0
        self.file.write(IndexedContainerWriterV0._file_header.pack(header, self.version, 0, 0, 0))
        # self.protocol = protocol

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def write(self, data: bytes):
        """ Writes a stream of bytes to file

            args:
                data (bytes): bytes to be writen to file
        """
        self.file.write(IndexedContainerWriterV0._chunk_header.pack(len(data), binascii.crc32(data)))
        self.file.write(data)
        self.data_counter += 1

    def close(self):
        self.file.close()


@IndexedContainerWriter._register
class IndexedContainerWriterV1:
    """ An indexed

    """

    _protocol_v = 1
    _file_header = struct.Struct("<4s4sIQ2H")
    _bunch_trailer_header = struct.Struct("<3Q2IH")
    _compressors = {"bz2": (bz2, 1)}

    def __init__(
        self,
        filename: str,
        header_ext: bytes = None,
        marker_ext: str = "",
        compressor=None,
        bunchsize: int = 1000000,
    ):
        """Summary

        Args:
            filename (str): Description
            header_ext (bytes, optional): Description
            marker_ext (str, optional): Description
            compressor (None, optional): Description
            bunchsize (int, optional): Description
        """
        self.filename = filename
        self._file = open(self.filename, "wb")

        self.compress = False
        compressor_id = 0
        if compressor is not None:
            self.compress = True
            self.compressor = IndexedContainerWriterV1._compressors[compressor][0]
            compressor_id = IndexedContainerWriterV1._compressors[compressor][1]

        self.data_counter = 0
        self.version = IndexedContainerWriterV1._protocol_v
        self.time_stamp = int(datetime.now().timestamp())
        self.bunchsize = bunchsize
        self._fp = 0
        self._marker_ext = marker_ext
        header_ext = header_ext or []
        self._write(
            IndexedContainerWriterV1._file_header.pack(
                "SOF".encode(),
                marker_ext.encode(),
                self.version,
                self.time_stamp,
                compressor_id,
                len(header_ext),
            )
        )

        if len(header_ext) > 0:
            self._write(header_ext)
        self._buffer = []
        self._cbunchindex = []
        self._cbunchoffset = 0
        self._last_bunch_fp = 0
        self._bunch_number = 0

    def _write(self, data: bytes):
        self._file.write(data)
        self._fp += len(data)

    def write(self, data: bytes):
        """ Writes a stream of bytes to file

            args:
                data (bytes): bytes to be writen to file
        """

        self._buffer.append(data)
        self._cbunchindex.append((binascii.crc32(data), len(data)))
        self._cbunchoffset += len(data)
        self.data_counter += 1
        if self._cbunchoffset > self.bunchsize:
            self.flush()

    def flush(self):
        """Flushes any data in buffer to file.

        """
        if len(self._buffer) < 1:
            return
        bunch_start_fp = self._fp
        # writing the data bunch
        byte_buff = bytearray()
        for data in self._buffer:
            byte_buff.extend(data)
        bunch_crc = binascii.crc32(data)
        if self.compress:
            self._write(self.compressor.compress(byte_buff))
        else:
            self._write(byte_buff)


        # constructing the index and writing it in the bunch trailer
        index = list(zip(*self._cbunchindex))
        n = len(self._buffer)
        bunch_index = struct.pack("{}I{}I".format(n, n), *index[0], *index[1])
        self._write(bunch_index)


        bunch_index_trailer = IndexedContainerWriterV1._bunch_trailer_header.pack(
            self._fp - self._last_bunch_fp,
            self._fp - bunch_start_fp,
            self._fp,
            bunch_crc,
            len(self._buffer),
            self._bunch_number,
        )

        # before writing the bunch trailer we update
        # the file pointer for the last bunch
        self._last_bunch_fp = self._fp

        self._write(bunch_index_trailer)

        # reseting/updating the last bunch descriptors
        self._cbunchindex.clear()
        self._buffer.clear()
        self._cbunchoffset = 0
        self._bunch_number += 1

    def close(self):
        self.flush()
        self._file.close()


class IndexedContainerReader:

    """Summary

    Attributes:
        file (TYPE): file handle
        filename (TYPE): the full filename
        metadata (dict): Dictonary containing file meta data
    """

    _protocols = {}

    def __init__(self, filename: str):
        """ Reads Streamed Object Files

        Args:
            filename (str): filename and path to the file

        Raises:
            TypeError: Raised if the file is not recognized as SOF
        """

        self.filename = filename
        self.file = open(self.filename, "rb")
        self._fhead = self.file.read(12)  # _file_header.size)
        self.file.seek(0)
        self.metadata = {}

        self.fhead, self.version = struct.unpack("QI", self._fhead)

        if self.version == 0 and self.fhead >= 0:
            readerclass = IndexedContainerReader._protocols[self.version]
            self._reader = readerclass(self.file)
        elif self.version > 0:
            readerclass = IndexedContainerReader._protocols[self.version]
            self._fhead = self.file.read(readerclass._file_header.size)
            self._reader = readerclass(self.file)
        else:
            raise TypeError("This file appears not to be a stream object file (SOF)")

    @classmethod
    def _register(cls, scls):
        cls._protocols[scls._protocol_v] = scls
        return scls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def reload(self):
        """ Reload the index table. Useful if the file
            is being written too when read
        """
        self._reader.reload()

    def resetfp(self):
        """ Resets file pointer to the first object in file
        """
        self._reader.resetfp()

    def __getitem__(self, ind: Union[int, slice, list])->bytes:
        """Indexing interface to the streamed file.
        Objects are read by their index, slice or list of indices.

        Args:
            ind (Union[int, slice, list]): an integer index, slice or list off indices to be read

        Returns:
            bytes: Bytes that represent the read object
        """
        if isinstance(ind, slice):
            data = [self.read_at(ii) for ii in range(*ind.indices(self.n_entries))]
            return data
        elif isinstance(ind, list):
            data = [self.read_at(ii) for ii in ind]
            return data
        elif isinstance(ind, int):
            return self.read_at(ind)

    def read_at(self, ind: int) -> bytes:
        """Reads one object at the index indicated by `ind`

        Args:
            ind (int): the index of the object to be read

        Returns:
            bytes: that represent the object

        Raises:
            IndexError: if index out of range
        """
        return self._reader.read_at(ind)

    def read(self) -> bytes:
        """ Reads one object at the position of the file pointer.

            Returns:
                bytes: Bytes that represent the object
        """
        return self._reader.read()

    def close(self):
        """Summary
        """
        self.file.close()

    @property
    def n_entries(self):
        """ Number of entries in file
        Returns:
            int: Number of entries (objects) in file
        """
        return self._reader.n_entries

    @property
    def filesize(self):
        """Summary

        Returns:
            int: Description
        """
        return self._reader.filesize

    @property
    def timestamp(self):
        """A timestamp from the creation of the file

        Returns:
            int: unix timestamp
        """
        return datetime.fromtimestamp(self._reader._timestamp)

    def __str__(self):

        s = "{}:\n".format(self.__class__.__name__)
        s += "filename: {}\n".format(self.filename)
        s += "timestamp: {}\n".format(self.timestamp)
        s += "n_entries: {}\n".format(self._reader.n_entries)
        s += "file size: {} {}B\n".format(*get_si_prefix(self._reader.filesize))
        for k, v in self.metadata.items():
            s += "{}: {}\n".format(k, v)
        s += "file format version: {}".format(self.version)
        return s


@IndexedContainerReader._register
class IndexedContainerReaderV1:

    """Summary

    Attributes:
        file (TYPE): Description
        file_index (list): Description
        filesize (TYPE): Description
        n_bunches (TYPE): Description
        n_entries (TYPE): Description
    """

    _protocol_v = 1
    _file_header = IndexedContainerWriterV1._file_header
    def __init__(self, file):
        """Summary

        Args:
            file (TYPE): Description

        Raises:
            TypeError: Description
        """
        fileheader_def = IndexedContainerWriterV1._file_header
        self.file = file
        self.file.seek(0)
        fileheader = self.file.read(fileheader_def.size)
        marker, extmarker, self._version, self._timestamp, self._compressed, self._lenheadext = fileheader_def.unpack(
            fileheader
        )
        if marker[:3].decode() != "SOF":
            raise TypeError("This file appears not to be a stream object file (SOF)")
        if self._version != IndexedContainerWriterV1._protocol_v:
            raise TypeError(
                "This file is written with protocol V{}"
                " while this class reads protocol V{}".format(
                    self._version, IndexedContainerWriterV1._protocol_v
                )
            )
        self._compressor = None
        if self._compressed > 0:
            compressorsr = {}
            for k, v in IndexedContainerWriterV1._compressors.items():
                compressorsr[v[1]] = (v[0], k)
            self._compressor = compressorsr[self._compressed][0]
            self._compressor_name = compressorsr[self._compressed][1]
        self._headext = None
        if self._lenheadext > 0:
            self._headext = self.file.read(self._lenheadext)
        self._fp_start = self.file.tell()
        self.n_bunches = None
        self.n_entries = None
        self.filesize = None
        self._rawindex = {}
        self._bunch_buffer = {}
        self._current_index = 0
        self._scan_file()

    def _scan_file(self):
        from collections import namedtuple

        BunchTrailer = namedtuple(
            "BunchTrailer", "bunchoff dataoff fileoff crc bunchsize ndata index objsize"
        )
        self.file.seek(-IndexedContainerWriterV1._bunch_trailer_header.size, os.SEEK_END)

        self.filesize = self.file.tell()
        self.file_index = [0]
        while self.file.tell() > self._fp_start:
            # read bunch trailer
            last_bunch_trailer = self.file.read(
                IndexedContainerWriterV1._bunch_trailer_header.size
            )
            bunchoff, dataoff, fileoff, crc, ndata, bunch_n = IndexedContainerWriterV1._bunch_trailer_header.unpack(
                last_bunch_trailer
            )

            # read bunch index
            self.file.seek(
                self.file.tell()
                - IndexedContainerWriterV1._bunch_trailer_header.size
                - ndata * 2 * 4
            )
            index = struct.unpack(
                "{}I{}I".format(ndata, ndata), self.file.read(ndata * 2 * 4)
            )
            objsize = np.array(index[ndata:], dtype=np.uint32)
            self._rawindex[(0, bunch_n)] = BunchTrailer(
                bunchoff,  # Offset to earlier bunch or file header if first bunch
                dataoff,  # Offset to beginning of data in bunch
                fileoff,  # Offset to beginning of file
                crc,  # bunch crc
                dataoff - ndata * 2 * 4,  # Size of data bunch
                ndata,  # number of objects in bunch
                [0] + list(np.cumsum(objsize[:-1])),  # Object offsets in bunch
                objsize,  # object sizes
            )
            self.file.seek(self.file.tell() - bunchoff)

        self._index = []
        self._bunch_index = {}
        for k, bunch in sorted(self._rawindex.items()):
            self._bunch_index[k] = (bunch.fileoff - bunch.dataoff, bunch.bunchsize)
            for i, obj in enumerate(bunch.index):
                self._index.append((k, int(obj), int(bunch.objsize[i])))
        self.n_entries = len(self._index)

    def _get_bunch(self, bunch_id):

        if bunch_id in self._bunch_buffer:
            return self._bunch_buffer[bunch_id]
        else:
            self.file.seek(self._bunch_index[bunch_id][0])
            bunch = self._compressor.decompress(
                self.file.read(
                    self.file_index[bunch_id[0]] + self._bunch_index[bunch_id][1]
                )
            )
            self._bunch_buffer[bunch_id] = bunch
            return bunch

    def read_at(self, ind: int) -> bytes:
        """Reads one object at the index indicated by `ind`

        Args:
            ind (int): the index of the object to be read

        Returns:
            bytes: that represent the object

        Raises:
            IndexError: if index out of range
        """

        if ind > self.n_entries - 1:
            raise IndexError(
                "The requested file object ({}) is out of range".format(ind)
            )
        obji = self._index[ind]
        if self._compressed:
            bunch = self._get_bunch(obji[0])
            return bunch[obji[1] : obji[1] + obji[2]]
        else:
            fpos = self.file_index[obji[0][0]] + self._bunch_index[obji[0]][0] + obji[1]
            self.file.seek(fpos)

            return self.file.read(obji[2])

    def resetfp(self):
        """ Resets file pointer to the first object in file
        """
        self._current_index = 0

    def read(self) -> bytes:
        """ Reads one object at the position of the file pointer.

            Returns:
                bytes: Bytes that represent the object
        """
        self._current_index += 1
        return self.read_at(self._current_index - 1)


@IndexedContainerReader._register
class IndexedContainerReaderV0:

    _protocol_v = 0
    _chunk_header = IndexedContainerWriterV0._chunk_header
    _file_header = IndexedContainerWriterV0._file_header
    def __init__(self, file):
        self.file = file
        self._scan_file()
        self._timestamp = 0

    def reload(self):
        """ Reload the index table. Useful if the file
            is being written too when read
        """
        self._scan_file(self.filesize, self.n_entries, self.fpos)

    def resetfp(self):
        """ Resets file pointer to the first object in file
        """
        self.file.seek(IndexedContainerReaderV0._file_header.size)

    def _scan_file(self, offset=0, n_entries=0, fpos=[]):
        """Summary

        Args:
            offset (int, optional): Description
            n_entries (int, optional): Description
            fpos (list, optional): Description
        """
        self.file.seek(offset)
        fh = self.file
        self.n_entries = n_entries
        self.fpos = fpos
        # Skipping file header
        fp = IndexedContainerReaderV0._file_header.size
        while True:
            fh.seek(fp)
            rd = fh.read(IndexedContainerReaderV0._chunk_header.size)
            if rd == b"":
                break
            self.fpos.append(fp)
            offset, crc = IndexedContainerReaderV0._chunk_header.unpack(rd)
            self.n_entries += 1
            fp = fh.tell() + offset
        self.file.seek(IndexedContainerReaderV0._file_header.size)
        self.filesize = self.fpos[-1] + offset

    def read_at(self, ind: int) -> bytes:
        """Reads one object at the index indicated by `ind`

        Args:
            ind (int): the index of the object to be read

        Returns:
            bytes: that represent the object

        Raises:
            IndexError: if index out of range
        """
        if ind > len(self.fpos) - 1:
            raise IndexError(
                "The requested file object ({}) is out of range".format(ind)
            )
        self.file.seek(self.fpos[ind])
        return self.read()

    def read(self) -> bytes:
        """ Reads one object at the position of the file pointer.

            Returns:
                bytes: Bytes that represent the object
        """
        sized = self.file.read(IndexedContainerReaderV0._chunk_header.size)
        if sized == b"":
            return None
        size, crc = IndexedContainerReaderV0._chunk_header.unpack(sized)
        return self.file.read(size)


