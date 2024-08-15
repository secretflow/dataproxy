FROM secretflow-registry.cn-hangzhou.cr.aliyuncs.com/secretflow/secretpad-base-lite:0.3

ENV LANG=C.UTF-8
WORKDIR /app

# fix: RunP proot + java bug
RUN ln -s ${JAVA_HOME}/lib/libjli.so /lib64

COPY target/*.jar dataproxy.jar
COPY config/application.yaml application.yaml
COPY scripts/start_dp.sh start_dp.sh
ENV JAVA_OPTS="" SPRING_PROFILES_ACTIVE="default"
EXPOSE 8023
ENTRYPOINT ${JAVA_HOME}/bin/java ${JAVA_OPTS} -Dsun.net.http.allowRestrictedHeaders=true --add-opens=java.base/java.nio=ALL-UNNAMED -jar -Dspring.profiles.active=${SPRING_PROFILES_ACTIVE} ./dataproxy.jar