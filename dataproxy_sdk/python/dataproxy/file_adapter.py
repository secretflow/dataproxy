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

from ._lib import DataProxyFile
from . import proto
import pyarrow as pa
import os
import logging
import hashlib


def get_file_sha256(fname):
    m = hashlib.sha256()  # 创建sha256对象
    with open(fname, "rb") as fobj:
        while True:
            data = fobj.read(4096)
            if not data:
                break
            m.update(data)  # 更新sha256对象

    return m.hexdigest()  # 返回sha256对象


class FileAdapter:
    def __init__(self, config: proto.DataProxyConfig):
        self.file = DataProxyFile(config.SerializeToString())

    def close(self):
        self.file.close()

    def download_file(
        self, info: proto.DownloadInfo, file_path: str, file_format: proto.FileFormat
    ):
        logging.info(
            f"dataproxy sdk: start download_file[{file_path}], type[{file_format}]"
        )

        self.file.download_file(info.SerializeToString(), file_path, file_format)

        size = os.path.getsize(file_path)
        sha256 = get_file_sha256(file_path)
        logging.info(
            f"dataproxy sdk: download_file[{file_path}], type[{file_format}], size[{size}], sha256[{sha256}]"
        )

    def upload_file(
        self, info: proto.UploadInfo, file_path: str, file_format: proto.FileFormat
    ):
        logging.info(
            f"dataproxy sdk: start upload_file[{file_path}], type[{file_format}]"
        )

        self.file.upload_file(info.SerializeToString(), file_path, file_format)

        size = os.path.getsize(file_path)
        sha256 = get_file_sha256(file_path)
        logging.info(
            f"dataproxy sdk: upload_file[{file_path}], type[{file_format}], size[{size}], sha256[{sha256}]"
        )
