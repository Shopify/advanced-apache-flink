FROM flink:2.1.0-scala_2.12-java17

USER root

# Install JDK 17 (replacing the default JRE)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Update JAVA_HOME to point to JDK (handles both amd64 and arm64)
RUN ln -sf /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

USER flink
