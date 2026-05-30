# docker/hive/Dockerfile
FROM apache/hive:3.1.3

USER root

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/* && \
    wget -q \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.2/hadoop-aws-2.10.2.jar \
      -O /opt/hive/lib/hadoop-aws-2.10.2.jar && \
    wget -q \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -O /opt/hive/lib/aws-java-sdk-bundle-1.12.262.jar

USER hive