# https://issues.apache.org/jira/browse/HADOOP-18154
# HADOOP 连接器现在仍然不支持 IRSA

FROM flink:1.17.1-java11
COPY lib/*.jar $FLINK_HOME/lib/
COPY hadoop-libs/*.jar $FLINK_HOME/hadoop-libs/
COPY hadoop-libs/client/*.jar $FLINK_HOME/hadoop-libs/client/
COPY plugins/s3/*.jar $FLINK_HOME/lib/plugins/s3/
COPY target/flink-cdc-multi-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
COPY conf/flink-conf.yaml $FLINK_HOME/conf/
RUN apt-get update
RUN apt-get install -y --fix-missing less vim unzip mysql-client netcat dnsutils
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN ./aws/install
RUN aws sts get-caller-identity
ENV FLINK_CONF_DIR=$FLINK_HOME/conf