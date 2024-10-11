# Copyright 2024 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import libdataproxy
from . import dp_pb2 as proto
import logging
import os


class DataProxyFileAdapter:
    def __init__(self, config: proto.DataProxyConfig):
        self.data_proxy_file = libdataproxy.DataProxyFile(config.SerializeToString())

    def close(self):
        self.data_proxy_file.close()

    def download_file(
        self, info: proto.DownloadInfo, file_path: str, file_format: proto.FileFormat
    ):
        logging.info(
            f"dataproxy sdk: start download_file[{file_path}], type[{file_format}]"
        )

        self.data_proxy_file.download_file(
            info.SerializeToString(), file_path, file_format
        )

        size = os.path.getsize(file_path)
        logging.info(
            f"dataproxy sdk: download_file[{file_path}], type[{file_format}], size[{size}]"
        )

    def upload_file(
        self, info: proto.UploadInfo, file_path: str, file_format: proto.FileFormat
    ):
        logging.info(
            f"dataproxy sdk: start upload_file[{file_path}], type[{file_format}]"
        )

        self.data_proxy_file.upload_file(
            info.SerializeToString(), file_path, file_format
        )

        size = os.path.getsize(file_path)
        logging.info(
            f"dataproxy sdk: upload_file[{file_path}], type[{file_format}], size[{size}]"
        )
