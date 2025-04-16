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

#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include "dataproxy_sdk/api.h"

namespace py = pybind11;

namespace dataproxy_sdk {

void DeletePtr(void* ptr) {
  if (ptr) {
    free(ptr);
  }
}

void CreateProtoObj(class py::module_& m) {
  py::enum_<proto::FileFormat>(m, "FileFormat")
      .value("UNKNOWN", proto::FileFormat::UNKNOWN)
      .value("CSV", proto::FileFormat::CSV)
      .value("BINARY", proto::FileFormat::BINARY)
      .value("ORC", proto::FileFormat::ORC)
      .export_values()
      .def("__int__",
           [](proto::FileFormat val) { return static_cast<int>(val); });

  py::class_<proto::TlSConfig>(m, "TlSConfig")
      .def(py::init<std::string, std::string, std::string>(),
           py::arg("private_key_path") = "", py::arg("certificate_path") = "",
           py::arg("ca_file_path") = "")
      .def_property("private_key_path", &proto::TlSConfig::private_key_path,
                    &proto::TlSConfig::set_private_key_path)
      .def_property("certificate_path", &proto::TlSConfig::certificate_path,
                    &proto::TlSConfig::set_certificate_path)
      .def_property("ca_file_path", &proto::TlSConfig::ca_file_path,
                    &proto::TlSConfig::set_ca_file_path);

  py::class_<proto::DataProxyConfig>(m, "DataProxyConfig")
      .def(py::init<std::string, std::optional<proto::TlSConfig>>(),
           py::arg("data_proxy_addr") = "",
           py::arg("tls_config") = std::nullopt)
      .def_property("data_proxy_addr", &proto::DataProxyConfig::data_proxy_addr,
                    &proto::DataProxyConfig::set_data_proxy_addr)
      .def_property("tls_config", &proto::DataProxyConfig::tls_config,
                    &proto::DataProxyConfig::set_tls_config);

  py::class_<proto::ORCFileInfo> orcfi_cls(m, "ORCFileInfo");
  py::enum_<proto::ORCFileInfo::CompressionType>(orcfi_cls, "CompressionType")
      .value("UNCOMPRESSED", proto::ORCFileInfo::CompressionType::UNCOMPRESSED)
      .value("SNAPPY", proto::ORCFileInfo::CompressionType::SNAPPY)
      .value("GZIP", proto::ORCFileInfo::CompressionType::GZIP)
      .value("BROTLI", proto::ORCFileInfo::CompressionType::BROTLI)
      .value("ZSTD", proto::ORCFileInfo::CompressionType::ZSTD)
      .value("LZ4", proto::ORCFileInfo::CompressionType::LZ4)
      .value("LZ4_FRAME", proto::ORCFileInfo::CompressionType::LZ4_FRAME)
      .value("LZO", proto::ORCFileInfo::CompressionType::LZO)
      .value("BZ2", proto::ORCFileInfo::CompressionType::BZ2)
      .value("LZ4_HADOOP", proto::ORCFileInfo::CompressionType::LZ4_HADOOP)
      .export_values()
      .def("__int__", [](proto::ORCFileInfo::CompressionType val) {
        return static_cast<int>(val);
      });
  orcfi_cls
      .def(py::init<proto::ORCFileInfo::CompressionType, int64_t, int64_t>(),
           py::arg("compression") =
               proto::ORCFileInfo::CompressionType::UNCOMPRESSED,
           py::arg("compression_block_size") = 0, py::arg("stripe_size") = 0)
      .def(py::init<proto::ORCFileInfo::CompressionType, int64_t, int64_t>(),
           py::arg("compression"), py::arg("compression_block_size"),
           py::arg("stripe_size"))
      .def_property("compression", &proto::ORCFileInfo::compression,
                    &proto::ORCFileInfo::set_compression)
      .def_property("compression_block_size",
                    &proto::ORCFileInfo::compression_block_size,
                    &proto::ORCFileInfo::set_compression_block_size)
      .def_property("stripe_size", &proto::ORCFileInfo::stripe_size,
                    &proto::ORCFileInfo::set_stripe_size);

  py::class_<proto::DownloadInfo>(m, "DownloadInfo")
      .def(py::init<std::string, std::string,
                    std::variant<std::monostate, proto::ORCFileInfo>>(),
           py::arg("domaindata_id") = "", py::arg("partition_spec") = "",
           py::arg("orc_info") = py::none())
      .def_property("domaindata_id", &proto::DownloadInfo::domaindata_id,
                    &proto::DownloadInfo::set_domaindata_id)
      .def_property("partition_spec", &proto::DownloadInfo::partition_spec,
                    &proto::DownloadInfo::set_partition_spec)
      .def_property("orc_info", &proto::DownloadInfo::orc_info,
                    &proto::DownloadInfo::set_orc_info);

  py::class_<proto::SQLInfo>(m, "SQLInfo")
      .def(py::init<std::string, std::string>(), py::arg("datasource_id") = "",
           py::arg("sql") = "")
      .def_property("datasource_id", &proto::SQLInfo::datasource_id,
                    &proto::SQLInfo::set_datasource_id)
      .def_property("sql", &proto::SQLInfo::sql, &proto::SQLInfo::set_sql);

  py::class_<proto::DataColumn>(m, "DataColumn")
      .def(py::init<std::string, std::string, std::string, bool>(),
           py::arg("name") = "", py::arg("type") = "", py::arg("comment") = "",
           py::arg("not_nullable") = false)
      .def_property("name", &proto::DataColumn::name,
                    &proto::DataColumn::set_name)
      .def_property("type", &proto::DataColumn::type,
                    &proto::DataColumn::set_type)
      .def_property("comment", &proto::DataColumn::comment,
                    &proto::DataColumn::set_comment)
      .def_property("not_nullable", &proto::DataColumn::not_nullable,
                    &proto::DataColumn::set_not_nullable);

  py::class_<proto::UploadInfo>(m, "UploadInfo")
      .def(py::init<std::string, std::string, std::string, std::string,
                    std::string, std::map<std::string, std::string>,
                    std::vector<proto::DataColumn>, std::string>(),
           py::arg("domaindata_id") = "", py::arg("name") = "",
           py::arg("type") = "", py::arg("relative_uri") = "",
           py::arg("datasource_id") = "",
           py::arg("attributes") = std::map<std::string, std::string>{},
           py::arg("columns") = std::vector<proto::DataColumn>{},
           py::arg("vendor") = "")
      .def_property("domaindata_id", &proto::UploadInfo::domaindata_id,
                    &proto::UploadInfo::set_domaindata_id)
      .def_property("name", &proto::UploadInfo::name,
                    &proto::UploadInfo::set_name)
      .def_property("type", &proto::UploadInfo::type,
                    &proto::UploadInfo::set_type)
      .def_property("relative_uri", &proto::UploadInfo::relative_uri,
                    &proto::UploadInfo::set_relative_uri)
      .def_property("datasource_id", &proto::UploadInfo::datasource_id,
                    &proto::UploadInfo::set_datasource_id)
      .def_property("attributes", &proto::UploadInfo::attributes, nullptr)
      .def_property("columns", &proto::UploadInfo::columns, nullptr)
      .def_property("vendor", &proto::UploadInfo::vendor,
                    &proto::UploadInfo::set_vendor);
}

PYBIND11_MODULE(_lib, m) {
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

  CreateProtoObj(m);

  py::class_<DataProxyFile, std::unique_ptr<DataProxyFile>>(m, "DataProxyFile")
      .def(py::init([]() -> std::unique_ptr<DataProxyFile> {
        return DataProxyFile::Make();
      }))
      .def(py::init([](const proto::DataProxyConfig& config)
                        -> std::unique_ptr<DataProxyFile> {
        return DataProxyFile::Make(config);
      }))
      .def("download_file",
           [](DataProxyFile& self, const proto::DownloadInfo& download_info,
              const std::string& file_path, proto::FileFormat file_format) {
             self.DownloadFile(download_info, file_path, file_format);
           })
      .def("upload_file",
           [](DataProxyFile& self, proto::UploadInfo& upload_info,
              const std::string& file_path, proto::FileFormat file_format) {
             self.UploadFile(upload_info, file_path, file_format);
           })
      .def("close", &DataProxyFile::Close);

  py::class_<DataProxyStreamReader, std::unique_ptr<DataProxyStreamReader>>(
      m, "DataProxyStreamReader")
      .def("get",
           [](DataProxyStreamReader& self) -> py::object {
             std::shared_ptr<arrow::RecordBatch> batch;
             self.Get(&batch);
             if (batch == nullptr) {
               return py::none();
             }
             ArrowArray* ret_array = (ArrowArray*)malloc(sizeof(ArrowArray));
             CHECK_ARROW_OR_THROW(arrow::ExportRecordBatch(*batch, ret_array));
             return py::capsule(ret_array, nullptr, DeletePtr);
           })
      .def("schema", [](DataProxyStreamReader& self) -> py::object {
        auto schema = self.Schema();
        DATAPROXY_ENFORCE(schema != nullptr);
        ArrowSchema* ret_schema = (ArrowSchema*)malloc(sizeof(*ret_schema));
        CHECK_ARROW_OR_THROW(arrow::ExportSchema(*schema, ret_schema));
        return py::capsule(ret_schema, "arrow_schema", DeletePtr);
      });

  py::class_<DataProxyStreamWriter, std::unique_ptr<DataProxyStreamWriter>>(
      m, "DataProxyStreamWriter")
      .def("put",
           [](DataProxyStreamWriter& self, py::capsule schema_capsule,
              py::capsule array_capsule) {
             ArrowArray* array = array_capsule.get_pointer<ArrowArray>();
             ArrowSchema* schema = schema_capsule.get_pointer<ArrowSchema>();
             ASSIGN_DP_OR_THROW(auto batch,
                                arrow::ImportRecordBatch(array, schema));
             self.Put(batch);
           })
      .def("close", [](DataProxyStreamWriter& self) { self.Close(); });

  py::class_<DataProxyStream, std::shared_ptr<DataProxyStream>>(
      m, "DataProxyStream")
      .def(py::init([]() -> std::shared_ptr<DataProxyStream> {
        return DataProxyStream::Make();
      }))
      .def(py::init([](const proto::DataProxyConfig& config)
                        -> std::shared_ptr<DataProxyStream> {
        return DataProxyStream::Make(config);
      }))
      .def("get_reader",
           [](DataProxyStream& self, const proto::DownloadInfo& download_info)
               -> std::unique_ptr<DataProxyStreamReader> {
             return self.GetReader(download_info);
           })
      .def("get_writer",
           [](DataProxyStream& self, proto::UploadInfo& upload_info)
               -> std::unique_ptr<DataProxyStreamWriter> {
             return self.GetWriter(upload_info);
           });
}

}  // namespace dataproxy_sdk