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

#pragma once

#include "dataproxy_sdk/proto/data_proxy_pb.pb.h"
#include "kuscia/proto/api/v1alpha1/datamesh/flightdm.pb.h"

namespace dataproxy_sdk {

namespace proto {
// using namespace kuscia::proto::api::v1alpha1;
using namespace kuscia::proto::api::v1alpha1::datamesh;
}  // namespace proto

namespace dm_proto = kuscia::proto::api::v1alpha1::datamesh;
namespace kuscia_proto = kuscia::proto::api::v1alpha1;

google::protobuf::Any BuildDownloadAny(const proto::DownloadInfo& info,
                                       proto::FileFormat file_format);

google::protobuf::Any BuildUploadAny(const proto::UploadInfo& info,
                                     proto::FileFormat file_format);

proto::CreateDomainDataRequest BuildActionCreateDomainDataRequest(
    const proto::UploadInfo& info, proto::FileFormat file_format);

proto::DeleteDomainDataRequest BuildActionDeleteDomainDataRequest(
    const proto::UploadInfo& info);

proto::CreateDomainDataResponse GetActionCreateDomainDataResponse(
    const std::string& msg);

void CheckUploadInfo(const proto::UploadInfo& info);

}  // namespace dataproxy_sdk