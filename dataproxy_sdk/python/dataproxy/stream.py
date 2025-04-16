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

from ._lib import (
    DataProxyStreamWriter,
    DataProxyStreamReader,
    DataProxyStream,
)
from . import protos
import logging
import os
import pyarrow


class StreamReader:
    def __init__(self, reader: DataProxyStreamReader):
        self.reader = reader
        schema_capsule = reader.schema()
        self.schema = pyarrow.Schema._import_from_c_capsule(schema_capsule)

    def get(self):
        array_ptr = self.reader.get()
        if array_ptr is None:
            raise StopIteration

        return pyarrow.RecordBatch._import_from_c(array_ptr, self.schema)

    def get_schema(self):
        return self.schema


class StreamWriter:
    def __init__(self, writer: DataProxyStreamWriter):
        self.writer = writer

    def put(self, batch: pyarrow.RecordBatch):
        schema_capsule, array_capsule = batch.__arrow_c_array__()
        self.writer.put(schema_capsule, array_capsule)

    def close(self):
        self.writer.close()


class Stream:
    def __init__(self, config: protos.DataProxyConfig = None):
        if config is None:
            self.stream = DataProxyStream()
        else:
            self.stream = DataProxyStream(config)

    def get_reader(self, info: protos.DownloadInfo):
        reader = self.stream.get_reader(info)
        return StreamReader(reader)

    def get_writer(self, info: protos.UploadInfo):
        writer = self.stream.get_writer(info)
        return StreamWriter(writer)
