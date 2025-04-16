// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "pybind11/pybind11.h"
#include "test/tools/data_mesh_mock.h"

#include "dataproxy_sdk/api.h"

namespace py = pybind11;

namespace dataproxy_sdk {

PYBIND11_MODULE(_dm_mock, m) {
  m.doc() = R"pbdoc(
              Secretflow-DataProxy-SDK Python Test Library
                  )pbdoc";

  py::register_exception_translator(
      [](std::exception_ptr p) {  // NOLINT: pybind11
        try {
          if (p) {
            std::rethrow_exception(p);
          }
        } catch (const yacl::Exception& e) {
          // Translate this exception to a standard RuntimeError
          PyErr_SetString(PyExc_RuntimeError,
                          fmt::format("what: \n\t{}\n", e.what()).c_str());
        }
      });

  py::class_<DataMeshMock, std::unique_ptr<DataMeshMock>>(m, "DataMeshMock")
      .def(py::init([]() -> std::unique_ptr<DataMeshMock> {
        return DataMeshMock::Make();
      }))
      .def("start",
           [](DataMeshMock& self, const std::string& ip, bool open_dp) {
             if (ip.empty()) {
               CHECK_ARROW_OR_THROW(self.StartServer(open_dp));

             } else {
               CHECK_ARROW_OR_THROW(self.StartServer(ip, open_dp));
             }
           })
      .def("address",
           [](DataMeshMock& self) { return self.GetServerAddress(); })
      .def("close", [](DataMeshMock& self) {
        CHECK_ARROW_OR_THROW(self.CloseServer());
      });
}

}  // namespace dataproxy_sdk