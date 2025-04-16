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

from ._dm_mock import DataMeshMock


class DataMesh:
    def __init__(self):
        self.dm_server = DataMeshMock()

    def start(self, ip="", open_dp=False):
        self.dm_server.start(ip, open_dp)

    def address(self):
        return self.dm_server.address()

    def close(self):
        self.dm_server.close()
