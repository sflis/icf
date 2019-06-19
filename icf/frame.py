import struct
from importlib import import_module
import pyarrow
import numpy as np

class NumpyArray:
    def __init__(self,array):
        self.array = array

    def serialize(self):
        return pyarrow.serialize(self.array).to_buffer()

    @classmethod
    def deserialize(cls,data):
        return pyarrow.deserialize(data)

def dynamic_import(abs_module_path, class_name):
    module_object = import_module(abs_module_path)

    target_class = getattr(module_object, class_name)

    return target_class


class Frame:

    """Summary
    """

    def __init__(self):
        """Summary
        """
        self._objects = {}
        self._cache = {}

    @classmethod
    def from_bytes(cls, data):
        inst = cls()
        inst.deserialize(data)
        return inst

    def add(self, key:str, obj):
        """Summary

        Args:
            key (str): Description
            obj (TYPE): Description
        """
        if isinstance(obj, np.ndarray):
            obj = NumpyArray(obj)
        self._objects[key] = obj

    def items(self):
        return self._objects.items()

    def __getitem__(self, key):
        return self._objects[key]

    def keys(self):
        return self._objects.keys()

    def serialize(self)->bytes:
        """Serializes the frame to a bytestream

        Returns:
            bytes: serialized frame
        """
        data_stream = bytearray()
        index = []
        classes = []
        pos = 0
        for k, v in self._objects.items():
            classes.append(
                "{},{},{}\n".format(k, v.__class__.__name__, v.__class__.__module__)
            )
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
            pos
        )
        data_stream.extend(trailer)
        return data_stream

    def deserialize(self, data_stream:bytes):
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
                    m = import_module(module_)
                    self._cache[class_] = getattr(m, class_)
                except AttributeError:
                    self._cache[class_] = None

            if self._cache[class_] is not None:
                cls = self._cache[class_]
                self._objects[key] = cls.deserialize(data_stream[last_pos:i])
            else:
                # If we don't know how to deserialize the object we just expose
                # the raw byte stream
                self._objects[key] = data_stream[last_pos:i]
            last_pos = i


# class FrameObject:
#     def __init__(self, pack, unpack):
#         self.pack = pack
#         self.unpack = unpack

#     def serialize(self):
#         return self.pack()

#     def deserialize(self, data):
#         return self.unpack(data)


