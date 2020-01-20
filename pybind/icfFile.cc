// Copyright 2019 Cherenkov Telescope Array Observatory
// This software is distributed under the terms of the BSD-3-Clause license.

#include "icf/icfFile.h"
#include <pybind11/pybind11.h>
#include <string>

namespace icf {

namespace py = pybind11;

py::bytes read_at(ICFFile &obj,uint64_t index){
    auto data = obj.read_at(index);
    std::string s(data->begin(),data->end());
    return py::bytes(s);
}

void write(ICFFile &obj, std::string &data){
    obj.write(data.data(),data.size());
}


void icf_file(py::module &m) {
    py::class_<ICFFile> icf_file(m, "ICFFile");
    icf_file.def(py::init<std::string>());
    icf_file.def("read_at",&read_at);
    icf_file.def("write",&write);

}
} //icf namespace