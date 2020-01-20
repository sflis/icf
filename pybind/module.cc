// Copyright 2019 Cherenkov Telescope Array Observatory
// This software is distributed under the terms of the BSD-3-Clause license.

#include <pybind11/pybind11.h>

namespace icf {


namespace py = pybind11;

void icf_file(py::module &m);

PYBIND11_MODULE(icf_py, m) {
    icf_file(m);
}

}  // namespace icf
