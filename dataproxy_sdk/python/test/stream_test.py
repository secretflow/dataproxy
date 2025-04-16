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

import os
from dataproxy.stream import (
    Stream,
    StreamReader,
    StreamWriter,
)
from dataproxy.protos import (
    DataProxyConfig,
    UploadInfo,
    DownloadInfo,
    DataColumn,
)
from test.dm_mock import DataMesh
import pyarrow as pa
import unittest


class TestStream(unittest.TestCase):
    def __init__(self, methodName: str = "runTest") -> None:
        super().__init__(methodName)
        self.dm = DataMesh()
        self.dm.start("127.0.0.1:22002")
        self.dm_ip = self.dm.address()

    def test_stream(self):
        x = pa.array([2, 2, 4, 4, 5, 100])
        y = pa.array(
            ["Flamingo", "Parrot", "Dog", "Horse", "Brittle stars", "Centipede"]
        )
        schema = pa.schema([("x", pa.int32()), ("y", pa.string())])
        batch = pa.RecordBatch.from_arrays([x, y], schema=schema)

        config = DataProxyConfig(data_proxy_addr=self.dm_ip)
        stream = Stream(config)

        columns = []
        for name, type in zip(schema.names, schema.types):
            columns.append(DataColumn(name=str(name), type=str(type)))

        upload_info = UploadInfo(type="table", columns=columns)
        stream_writer = stream.get_writer(upload_info)
        stream_writer.put(batch)
        stream_writer.close()

        download_info = DownloadInfo(domaindata_id="test")
        stream_reader = stream.get_reader(download_info)
        ret_batchs = stream_reader.get()

        self.assertTrue(ret_batchs.equals(batch))

        os.environ["KUSCIA_DATA_MESH_ADDR"] = self.dm_ip
        stream_2 = Stream()

        stream_writer_2 = stream_2.get_writer(upload_info)
        stream_writer_2.put(batch)
        stream_writer_2.close()

        stream_reader_2 = stream.get_reader(download_info)
        ret_batchs_2 = stream_reader_2.get()

        self.assertTrue(ret_batchs_2.equals(batch))


if __name__ == "__main__":
    unittest.main()
