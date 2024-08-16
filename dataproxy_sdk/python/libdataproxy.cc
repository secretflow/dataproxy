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

#include "dataproxy_sdk/cc/api.h"
#include "dataproxy_sdk/cc/exception.h"
#include "pybind11/pybind11.h"

namespace py = pybind11;

namespace dataproxy_sdk {

PYBIND11_MODULE(libdataproxy, m) {
  m.doc() = R"pbdoc(
              Secretflow-DataProxy-SDK Python Library
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

  py::class_<DataProxyFile, std::unique_ptr<DataProxyFile>>(m, "DataProxyFile")
      .def(py::init(
          [](const py::bytes& config_str) -> std::unique_ptr<DataProxyFile> {
            proto::DataProxyConfig config;
            config.ParseFromString(config_str);
            return DataProxyFile::Make(config);
          }))
      .def("download_file",
           [](DataProxyFile& self, const py::bytes& info_str,
              const std::string& file_path, int file_format) {
             proto::DownloadInfo info;
             info.ParseFromString(info_str);

             self.DownloadFile(info, file_path,
                               static_cast<proto::FileFormat>(file_format));
           })
      .def("upload_file",
           [](DataProxyFile& self, const py::bytes& info_str,
              const std::string& file_path, int file_format) {
             proto::UploadInfo info;
             info.ParseFromString(info_str);

             self.UploadFile(info, file_path,
                             static_cast<proto::FileFormat>(file_format));
           })
      .def("close", &DataProxyFile::Close);
}

}  // namespace dataproxy_sdk