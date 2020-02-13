from icf._icf import icffile


def test_write_and_readback_icffile():
    f = icffile.ICFFile("/tmp/test.icf",'trunc')
    testdata1 = b'blablakaskdlaskd'
    testdata2 = b'blabasdasdlakaskdlaskd'
    f.write(testdata1)
    f.write(testdata2)
    original_timestamp = f.get_timestamp()
    assert f.n_entries == 2, "Correct number of entries"
    f.close()
    # open file again
    f = icffile.ICFFile("/tmp/test.icf")
    assert f.get_timestamp() == original_timestamp, "Read back correct timestamp"
    assert f.n_entries == 2, "Read back correct number of entries"
    assert f.read_at(0) == testdata1, "Read back correct data"
    assert f.read_at(1) == testdata2, "Read back correct data"
