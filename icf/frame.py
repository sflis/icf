import struct
import numpy as np
from importlib import import_module
import logging
import sys


class SerializationDispatcher:

    """Summary

    Attributes:
        subclasses (list): Description
    """

    subclasses = []
    _names = []
    serializers = {}
    serializers_c = {}

    def __init_subclass__(cls, types, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.__class__ not in cls._names:
            cls.subclasses.append(cls)
            cls._names.append(cls.__name__)
            for t in types:
                cls.serializers[t] = cls
                cls.serializers_c[cls.__name__] = cls
        return cls


def dispatch_serializer(obj):
    if type(obj) in SerializationDispatcher.serializers:
        return SerializationDispatcher.serializers[type(obj)](obj)
    return obj


class N(SerializationDispatcher, types=[np.ndarray]):

    _big = sys.byteorder == "big"
    _types = {
        np.dtype(np.uint8): 1,
        np.dtype(np.int8): 2,
        np.dtype(np.uint16): 3,
        np.dtype(np.int16): 4,
        np.dtype(np.uint32): 5,
        np.dtype(np.int32): 6,
        np.dtype(np.uint64): 7,
        np.dtype(np.int64): 8,
        np.dtype(np.float16): 9,
        np.dtype(np.float32): 10,
        np.dtype(np.float64): 11,
        np.dtype(np.float128): 12,
        np.dtype(np.complex64): 13,
        np.dtype(np.complex128): 14,
    }
    _types_r = {v: k for k, v in _types.items()}

    def __init__(self, obj):
        self.arr = obj

    def serialize(self):
        if self._big:
            self.arr.byteswap(True)
        shape = self.arr.shape
        data = bytearray(
            struct.pack(
                "<bb{}I".format(len(shape)),
                self._types[self.arr.dtype],
                len(shape),
                *shape,
            )
        )
        data.extend(self.arr.tobytes())
        return data

    @classmethod
    def deserialize(cls, data):
        dtype = cls._types_r[data[0]]
        ndim = data[1]
        shape = struct.unpack(f"<{ndim}I", data[2 : 2 + ndim * 4])
        arr = np.frombuffer(data[2 + ndim * 4 :], dtype)
        if cls._big:
            arr.byteswap(True)
        return arr.reshape(shape)


class S(SerializationDispatcher, types=[str]):
    def __init__(self, obj):
        self.str = obj

    def serialize(self):
        return self.str.encode()

    @classmethod
    def deserialize(cls, data):
        return data.decode()


class B(SerializationDispatcher, types=[bytes, bytearray]):
    def __init__(self, obj):
        self.bytes = obj

    def serialize(self):
        return self.bytes

    @classmethod
    def deserialize(cls, data):
        return cls.data


class I(SerializationDispatcher, types=[int]):
    def __init__(self, obj):
        self.int = obj

    def serialize(self):
        size = int(self.int.bit_length() / 8 + 1)
        return self.int.to_bytes(size, byteorder="little")

    @classmethod
    def deserialize(cls, data):
        return int.from_bytes(data, byteorder="little")


class F(SerializationDispatcher, types=[float]):
    encode = struct.Struct("<d")

    def __init__(self, obj):
        self.float = obj

    def serialize(self):
        return self.encode.pack(self.float)

    @classmethod
    def deserialize(cls, data):
        return cls.encode.unpack(data)[0]


class C(SerializationDispatcher, types=[complex]):
    encode = struct.Struct("<dd")
    def __init__(self, obj):
        self.complex = obj

    def serialize(self):
        data = self.encode.pack(self.complex.real, self.complex.imag)
        return data

    @classmethod
    def deserialize(cls, data):
        real, imag = cls.encode.unpack(data)
        return complex(real, imag)


class Q(SerializationDispatcher, types=[list, tuple, set]):
    seq_types = {list: "L", tuple: "T", set: "E"}
    seq_types_r = {v: k for k, v in seq_types.items()}

    def __init__(self, obj):
        self.seqcon = obj

    def serialize(self):
        data = bytearray(
            "{}".format(self.seq_types[type(self.seqcon)]), encoding="utf-8"
        )
        for el in self.seqcon:
            els = dispatch_serializer(el)
            tmpdata = els.serialize()
            size = len(tmpdata) * 2
            # The size of the object is always encoded as 2*(number of bytes).
            # The first bit of the size field is reserved to indicate whether
            # the size of the field is 16 or 32 bit.
            if size > 2 ** 16:
                el_header = struct.pack(
                    "<sI", els.__class__.__name__.encode(), size + 1
                )
            else:
                el_header = struct.pack("<sH", els.__class__.__name__.encode(), size)
            data.extend(el_header)
            data.extend(tmpdata)
        return data

    @classmethod
    def deserialize(cls, data):
        tmp_list = []
        seq_type = cls.seq_types_r[str(chr(data[0]))]
        data_p = 1
        while data_p < len(data):
            des_c, size = struct.unpack("<sH", data[data_p : data_p + 3])
            deserializer = SerializationDispatcher.serializers_c[des_c.decode()]
            # If size is not divisable by 2 the size of the field is 4 bytes
            # instead of 2 bytes
            if size % 2 != 0:
                des_c, size = struct.unpack("<sI", data[data_p : data_p + 5])
                data_p += 5
                size -= 1  # set the first bit to 0
            else:
                data_p += 3
            # Remove the encoding by dividing by 2
            size //= 2

            tmp_list.append(deserializer.deserialize(data[data_p : data_p + size]))
            data_p += size
        return seq_type(tmp_list)


class Frame:
    def __init__(self):
        self._objects = {}
        self._cache = {}
        self._serialized = {}
        self._log = logging.getLogger(__name__)

    def add(self, key, obj):
        # add checks here to see if the object supports serialization
        self._objects[key] = obj

    def items(self):
        for k, v in self._objects.items():
            yield k, v

        for k, v in self._serialized.items():
            if k in self._objects:

                continue
            self._deserialized_obj(k)
            yield k, self._objects[k]

    def __setitem__(self, key, obj):
        self.add(key, obj)

    def __getitem__(self, key):
        if key not in self._objects and key in self._serialized:
            self._deserialized_obj(key)
        return self._objects[key]

    def get(self, key):
        """ Behaves like the dict version of `get`. Returns
            the object if it exiest in the frame. If not
            the return value is None.

        Args:
            key (str): the key string

        Returns:
            obj or None: the object stored at `key` or n
        """
        if key not in self._objects and key in self._serialized:
            self._deserialized_obj(key)
        return self._objects.get(key)

    def keys(self):
        """

        Returns:
            TYPE: Description
        """
        keys = set(list(self._objects.keys()) + list(self._serialized.keys()))
        return iter(list(keys))

    def serialize(self) -> bytes:
        """Serializes the frame to a bytestream

        Returns:
            bytes: serialized frame
        """
        data_stream = bytearray()
        index = []
        classes = []
        pos = 0
        for k, v in self.items():
            v = dispatch_serializer(v)
            _module = v.__class__.__module__
            _module = _module if _module != __name__ else ""
            classes.append("{},{},{}\n".format(k, v.__class__.__name__, _module))
            d = v.serialize()
            pos += len(d)
            index.append(pos)
            data_stream.extend(d)
        n_obj = len(index)

        classes = "".join(classes)
        trailer = struct.pack(
            "<{}I{}s3I".format(n_obj, len(classes)),
            *index,
            classes.encode(),
            len(classes),
            n_obj,
            pos,
        )
        data_stream.extend(trailer)
        return data_stream

    def _deserialized_obj(self, key):
        class_, data = self._serialized[key]
        if self._cache[class_] is not None:
            cls = self._cache[class_]
            try:
                self._objects[key] = cls.deserialize(data)
            except Exception as e:
                self._log.error(
                    "An error occured while deserializing object {} of type {}:\n{}".format(
                        key, class_, e
                    )
                )
        else:
            # If we don't know how to deserialize the object we just expose
            # the raw byte stream
            self._objects[key] = data

    @classmethod
    def deserialize(cls, data_stream: bytes):
        inst = cls()
        inst.deserialize_m(data_stream)
        return inst

    def deserialize_m(self, data_stream: bytes):
        """Deserializes a frame from byte buffer

        Args:
            data_stream (bytes): byte buffer to be deserialized
        """
        l_cls, n_obj, indexpos = struct.unpack("<3I", data_stream[-12:])
        index = struct.unpack("<{}I{}s".format(n_obj, l_cls), data_stream[indexpos:-12])
        classes = index[n_obj:][0].decode()
        index = list(index[:n_obj])
        last_pos = 0
        for i, c in zip(index, classes.split("\n")):
            key, class_, module_ = c.split(",")

            if class_ not in self._cache.keys():
                try:
                    if module_ == "":
                        m = import_module(__name__)
                    else:
                        m = import_module(module_)
                    self._cache[class_] = getattr(m, class_)
                except AttributeError:
                    self._cache[class_] = None
                    self._log.warn(
                        f"Failed to import class `{class_}` to deserialize object at key: `{key}`"
                    )
            self._serialized[key] = (class_, data_stream[last_pos:i])
            last_pos = i

    @classmethod
    def unpack(cls, data_stream: bytes):
        return cls.deserialize(data_stream)  # frame

    def pack(self):
        return self.serialize()

    def __str__(self):
        s = "{\n"
        for k in set(list(self._objects.keys()) + list(self._serialized.keys())):
            if k not in self._objects:
                class_, _ = self._serialized[k]
                obj_str = "**serialized data**"
            else:
                class_ = type(self._objects[k])
                obj_str = self._objects[k].__str__()
            s += f"`{k}` <{class_}>: {obj_str},\n"
        s += "}"
        return s

    def __repr__(self):
        return self.__str__()
