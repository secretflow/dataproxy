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

FROM secretflow-registry.cn-hangzhou.cr.aliyuncs.com/secretflow/java-base:17.0.13-jre-anolis23

ENV LANG=C.UTF-8
WORKDIR /app

# fix: RunP proot + java bug
RUN ln -s ${JAVA_HOME}/lib/libjli.so /lib64

COPY dataproxy-server/target/dataproxy-server-0.0.1-SNAPSHOT.jar dataproxy.jar
COPY libs/*.jar libs/

ENV JAVA_OPTS=""
ENV LOG_LEVEL=INFO
EXPOSE 8023
ENTRYPOINT ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dsun.net.http.allowRestrictedHeaders=true --add-opens=java.base/java.nio=ALL-UNNAMED -jar ./dataproxy.jar