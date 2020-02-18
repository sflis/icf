# icf
This library provides an implementation of an indexed container file format. Each chunk of data is indexed and the chunks can therefore be retrieved in random order. Furthermore two files can also be merged by simply being concatenaded by cat and the resulting file is still a valid icf file.


## Installation
For a normal install run

`python setup.py install`

or in the root directory of the project do

`pip install .`

If you are developing it is recommendended to do

`pip install -e .`

instead and adding the `--user` option if not installing in a conda env. This lets changes made to the project automatically propagate to the install without the need to reinstall.


## Usage

A simple example:

```python
from icf import pyicf
f = pyicf.ICFFile("/tmp/test.icf")
f.write(b'some bytes of data')
f.write(b'some more bytes of data')
f.close()

f = pyicf.ICFFile("/tmp/test.icf")
first_chunk = f.read_at(0)
# or
all_data = f[:]

```