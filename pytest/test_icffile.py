import pytest
from icf import pyicf
from icf import ICFFile
import os

@pytest.fixture(params=[ICFFile, pyicf.ICFFile])
def icf_impl(request):
    return request.param


def test_write_and_readback_from_icffile(icf_impl):
    try:
        os.remove("/tmp/test.icf")
    except:
        pass
    f = icf_impl("/tmp/test.icf")
    testdata1 = b'blablakaskdlaskd'
    testdata2 = b'blabasdasdlakaskdlaskd'
    f.write(testdata1)
    f.write(testdata2)
    original_timestamp = f.get_timestamp()
    assert f.size() == 2, "Correct number of entries"
    f.close()
    # open file again
    f = icf_impl("/tmp/test.icf")
    assert f.get_timestamp() == original_timestamp, "Read back correct timestamp"
    assert f.size() == 2, "Read back correct number of entries"
    assert f.read_at(0) == testdata1, "Read back correct data"
    assert f.read_at(1) == testdata2, "Read back correct data"


def test_read_while_writing(icf_impl):
    try:
        os.remove("/tmp/test.icf")
    except:
        pass
    f = icf_impl("/tmp/test.icf")
    testdata1 = b'blablakaskdlaskd'
    testdata2 = b'blabasdasdlakaskdlaskd'
    f.write(testdata1)
    f.write(testdata2)

    assert f.size() == 2, "Correct number of objects in file"
    assert f.read_at(0) == testdata1, "Read back correct data"
    assert f.read_at(1) == testdata2, "Read back correct data"


def test_write_read_multiple_bunches(icf_impl):
    try:
        os.remove("/tmp/test.icf")
    except:
        pass

    f = pyicf.ICFFile("/tmp/test.icf", bunchsize=50)
    data = b"0" * 26

    for i in range(6):
        f.write(data)
    assert f._bunch_number == 3, "Correct number of bunches"
    f.close()

    f = pyicf.ICFFile("/tmp/test.icf")

    for i in range(6):
        assert f.read_at(i) == data


def test_read_merged_files(icf_impl):
    try:
        os.remove("/tmp/test1.icf")
        os.remove("/tmp/test2.icf")
        os.remove("/tmp/cat.icf")
    except:
        pass
    f1 = icf_impl("/tmp/test1.icf")
    f2 = icf_impl("/tmp/test2.icf")
    testdata1 = b'blablakaskdlaskd'
    testdata2 = b'blabasdasdlakaskdlaskd'
    f1.write(testdata1)
    f2.write(testdata2)
    f1.close()
    f2.close()
    os.system("cat /tmp/test1.icf /tmp/test2.icf >> /tmp/cat.icf")
    fcat = icf_impl("/tmp/cat.icf")


    assert fcat.size() == 2, "Correct number of objects in file"
    assert fcat.read_at(0) == testdata1, "Correct data from file 1"
    assert fcat.read_at(1) == testdata2, "Correct data from file 2"


def test_read_merged_files_multiple_bunches(icf_impl):
    try:
        os.remove("/tmp/testm1.icf")
        os.remove("/tmp/testm2.icf")
        os.remove("/tmp/catm.icf")
    except:
        pass
    f1 = icf_impl("/tmp/testm1.icf", bunchsize=50)
    f2 = icf_impl("/tmp/testm2.icf", bunchsize=50)
    testdata1 = b"0" * 26
    testdata2 = b'1' * 26
    f1.write(testdata1)
    f1.write(testdata1)
    f1.write(testdata1)
    f2.write(testdata2)
    f2.write(testdata2)
    f2.write(testdata2)
    f1.close()
    f2.close()
    os.system("cat /tmp/testm1.icf /tmp/testm2.icf >> /tmp/catm.icf")
    fcat = icf_impl("/tmp/catm.icf")

    assert fcat.size() == 6, "Correct number of objects in file"
    assert fcat.read_at(0) == testdata1, "Correct data from file 1"
    assert fcat.read_at(3) == testdata2, "Correct data from file 2"


def test_extwrie_pyread():
    try:
        os.remove("/tmp/ext.icf")
    except:
         pass
    f = ICFFile("/tmp/ext.icf")
    testdata1 = b'blablakaskdlaskd'
    testdata2 = b'blabasdasdlakaskdlaskd'
    f.write(testdata1)
    f.write(testdata2)
    timestamp = f.get_timestamp()
    f.close()

    f_py = pyicf.ICFFile("/tmp/ext.icf")

    assert f_py.get_timestamp() == timestamp, "Consistent time stamp"
    assert f_py.size() == 2, "Consistent number of chunks"
    assert f_py.read_at(0) == testdata1, "Correct data written in ext and read in py"
    assert f_py.read_at(1) == testdata2, "Correct data written in ext and read in py"


def test_bunch_buffer():
    n = 10
    bf = pyicf.icffile.BunchBuffer(n)

    for i in range(n):
        bf[i] = [i]
    for i in range(n):
        assert i in bf, "Element still in Bunch Buffer"

    bf[n] = [n]
    assert n in bf, "New element in Bunch Buffer"
    assert 0 not in bf, "Oldest element removed from Bunch Buffer"

