import pyarrow
from icf import io
from icf.frame import Frame
import numpy as np
from datetime import datetime


class MyObject:
    def __init__(self):
        self.array = np.random.exponential(
            np.random.uniform(0, 2),
            (int(np.random.uniform(1, 100)), int(np.random.uniform(1, 100))),
        )

    def serialize(self):
        return pyarrow.serialize(self.array).to_buffer()

    @classmethod
    def deserialize(self, data):
        return pyarrow.deserialize(data)


startTime = datetime.now()
writer = io.IndexedContainerWriter("testing.icf", compressor=None)  #'bz2')
for i in range(90000):

    frame = Frame()
    frame.add("rawarray", np.arange(100))
    data = np.zeros(1000)
    for i in range(np.random.poisson(200)):
        data[int(np.random.uniform(0, 1000 - 1))] = np.random.exponential(2)

    frame.add("randomarr", data)
    frame["a_list_of_lists"] = [1, 3, 4, 5, [9, 4, 5], (93, 3.034)]
    writer.write(frame.serialize())

writer.close()
print("Time to write:", datetime.now() - startTime)

startTime = datetime.now()
reader = io.IndexedContainerReader("testing.icf")
frames = []
raws = reader[:]
for r in raws:
    frame = Frame.deserialize(r)
    # frames.append(frame)
    for k, v in frame.items():
        pass
    # s = frame["rawarray"]
    # s = frame["randomarr"]

print(frame)
for k, v in frame.items():
    print(k, v)
print(frame)
print("Time to read:", datetime.now() - startTime)
print(reader)
