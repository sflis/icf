import pyarrow
from icf import io
from icf.frame import Frame
import numpy as np

class MyObject:
    def __init__(self):
        self.array = np.random.exponential(np.random.uniform(0,2),
                                       (int(np.random.uniform(1,100)),int(np.random.uniform(1,100)))
                                      )
    def serialize(self):
        return pyarrow.serialize(self.array).to_buffer()
    @classmethod
    def deserialize(self,data):
        return pyarrow.deserialize(data)


writer = io.IndexedContainerWriter('testing.icf',compressor=None)
for i in range(3000):

    frame = Frame()
    frame.add('rawarray',np.arange(100))
    data = np.zeros(10000)
    for i in range(np.random.poisson(100)):
        data[int(np.random.uniform(0,10000-1))] = np.random.exponential(2)
    frame.add('randomarr',data)
    writer.write(frame.serialize())

writer.close()

reader = io.IndexedContainerReader('testing.icf')
print(reader)
frames = [ ]
raws = reader[:]
for r in raws:
    frame=Frame()
    frame.deserialize(r)
    frames.append(frame)
for k,v in frame.items():
    print(k,v)
