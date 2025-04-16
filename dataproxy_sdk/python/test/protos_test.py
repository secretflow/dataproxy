# Copyright 2025 Ant Group Co., Ltd.
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

import unittest
from dataproxy.protos import (
    DataProxyConfig,
    UploadInfo,
    DownloadInfo,
    DataColumn,
    TlSConfig,
    ORCFileInfo,
)


class TestStream(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)

    def test_config(self):
        address = "127.0.0.1:50051"
        certificate_path = "certificate_path"
        private_key_path = "private_key_path"
        ca_file_path = "ca_file_path"

        dp_config = DataProxyConfig(
            address,
            tls_config=TlSConfig(
                certificate_path=certificate_path,
                private_key_path=private_key_path,
            ),
        )
        dp_config.tls_config.ca_file_path = ca_file_path
        self.assertEqual(address, dp_config.data_proxy_addr)
        self.assertEqual(certificate_path, dp_config.tls_config.certificate_path)
        self.assertEqual(private_key_path, dp_config.tls_config.private_key_path)
        self.assertEqual(ca_file_path, dp_config.tls_config.ca_file_path)

    def test_download(self):
        domain_data_id = "domain_data_id"
        partition_spec = "partition_spec"
        orc_compression = ORCFileInfo.CompressionType.ZSTD
        compression_block_size = 1111
        stripe_size = 2222
        download_info = DownloadInfo(
            domaindata_id=domain_data_id,
            partition_spec=partition_spec,
            orc_info=ORCFileInfo(),
        )
        download_info.orc_info.compression = orc_compression
        download_info.orc_info.compression_block_size = compression_block_size
        download_info.orc_info.stripe_size = stripe_size
        self.assertEqual(domain_data_id, download_info.domaindata_id)
        self.assertEqual(partition_spec, download_info.partition_spec)
        self.assertEqual(orc_compression, download_info.orc_info.compression)
        self.assertEqual(
            compression_block_size, download_info.orc_info.compression_block_size
        )
        self.assertEqual(stripe_size, download_info.orc_info.stripe_size)

    def test_upload(self):
        domaindata_id = "domaindata_id"
        name = "name"
        type = "type"
        relative_uri = "relative_uri"
        datasource_id = "datasource_id"
        attributes = {"key1": "value1", "key2": "value2"}
        columns = [
            DataColumn("name1", "type1", "comment", True),
            DataColumn(
                name="name2", type="type2", comment="comment2", not_nullable=False
            ),
        ]
        vendor = "vendor"
        upload_info = UploadInfo(
            domaindata_id=domaindata_id,
            name=name,
            type=type,
            relative_uri=relative_uri,
            datasource_id=datasource_id,
            attributes=attributes,
            columns=columns,
            vendor=vendor,
        )
        self.assertEqual(domaindata_id, upload_info.domaindata_id)
        self.assertEqual(name, upload_info.name)
        self.assertEqual(type, upload_info.type)
        self.assertEqual(relative_uri, upload_info.relative_uri)
        self.assertEqual(datasource_id, upload_info.datasource_id)
        self.assertEqual(attributes, upload_info.attributes)
        self.assertEqual(vendor, upload_info.vendor)
        for i in range(len(columns)):
            self.assertEqual(columns[i].name, upload_info.columns[i].name)
            self.assertEqual(columns[i].type, upload_info.columns[i].type)
            self.assertEqual(columns[i].comment, upload_info.columns[i].comment)
            self.assertEqual(
                columns[i].not_nullable, upload_info.columns[i].not_nullable
            )


if __name__ == "__main__":
    unittest.main()
