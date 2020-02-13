from icf import pyicf


def test_write_and_readback_from_icffile():
    f = pyicf.ICFFile("/tmp/test.icf",'trunc')
    testdata1 = b'blablakaskdlaskd'
    testdata2 = b'blabasdasdlakaskdlaskd'
    f.write(testdata1)
    f.write(testdata2)
    original_timestamp = f.get_timestamp()
    assert f.n_entries == 2, "Correct number of entries"
    f.close()
    # open file again
    f = pyicf.ICFFile("/tmp/test.icf")
    assert f.get_timestamp() == original_timestamp, "Read back correct timestamp"
    assert f.n_entries == 2, "Read back correct number of entries"
    assert f.read_at(0) == testdata1, "Read back correct data"
    assert f.read_at(1) == testdata2, "Read back correct data"


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
