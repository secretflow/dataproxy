#!/bin/bash
#
# Copyright 2023 Ant Group Co., Ltd.
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
#

set -ex

for i in "$@"
do
case $i in
    --serving_config_file=*)
    CONFIG_FILE="${i#*=}"
esac
done

export DP_CONFIG_FILE=$CONFIG_FILE
java -Dsun.net.http.allowRestrictedHeaders=true --add-opens=java.base/java.nio=ALL-UNNAMED -jar ./dataproxy.jar