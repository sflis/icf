from .io import IndexedContainerWriter, IndexedContainerReader


# FrameFileHeader:
#     def __init__(self):


# class FrameWriter(IndexedContainerWriter):
#     def __init__(self, filename: str, **kwargs):
#         super().__init__(filename, header_ext=CHECFileHeader(3).pack(), **kwargs)

#     def write(self, frame):
#         super().write(frame.serialize())