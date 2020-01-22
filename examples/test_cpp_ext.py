import icf_py
from icf import frame
from icf.frame import Frame
import numpy as np
class FrameFile(icf_py.ICFFile):
    def __init__(self, path):
        super().__init__(path)

    def write(self, frame):
        super().write(bytes(frame.serialize()))

    def read_at(self, ind):
        return Frame.deserialize(super().read_at(ind))





f = FrameFile('Testing_python_ext.icf')

frame = Frame()
frame.add("rawarray", np.arange(100))
# data = np.zeros(1000)
# for i in range(np.random.poisson(200)):
#     data[int(np.random.uniform(0, 1000 - 1))] = np.random.exponential(2)

# frame.add("randomarr", data)
frame["array"] = np.array([2,3,5,6,4,5,.120])
frame["a_list_of_lists"] = [1, 3, 4, 5, [9, 4, 5], (93, 3.034)]
print(f.size())
f.write(frame)
print(f.size())
f.close()

f = FrameFile('Testing_python_ext.icf')
print(f.size())
frame2 = f.read_at(0)
print(frame2)
frame3 = Frame()
frame3['col1'] = np.arange(100)
frame3['col2'] = np.random.uniform(0,1,100)
frame4 = Frame.deserialize(frame3.serialize())
print(frame4)
import pandas as pd
df = pd.DataFrame(dict(frame4))
print(df)

