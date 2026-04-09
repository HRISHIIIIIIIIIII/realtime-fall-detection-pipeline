# ── Stage 1: Build ────────────────────────────────────────────────────────────
# Maven compiles the Scala source and produces a fat JAR.
# This stage only runs during `docker compose build` — it doesn't stay running.
FROM maven:3.9.6-eclipse-temurin-11 AS builder

WORKDIR /build

# Copy pom.xml first. Docker caches this layer separately from source code.
# If only .scala files change, Maven won't re-download all dependencies.
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy source and build the fat JAR
COPY src/ ./src/
RUN mvn clean package -B -DskipTests

# Output: target/flink-delta-writer-1.0-SNAPSHOT-fat.jar
# (the "-fat" suffix comes from <shadedClassifierName>fat</shadedClassifierName> in pom.xml)

# ── Stage 2: Submit ───────────────────────────────────────────────────────────
# Use the same flink:1.18 image as the rest of the cluster so JARs are compatible.
# This container's only job: submit the fat JAR to the running Flink JobManager.
FROM flink:1.18

WORKDIR /opt/flink

# Copy the fat JAR from the build stage
COPY --from=builder \
  /build/target/flink-delta-writer-1.0-SNAPSHOT-fat.jar \
  /opt/flink/jobs/flink-delta-writer.jar

# Create checkpoint directory inside the container
RUN mkdir -p /opt/flink/checkpoints/delta-writer

# Submit the JAR to the JobManager, then keep the container alive.
#
# Why `tail -f /dev/null`?
#   `flink run` submits the job and returns immediately — the job runs on the
#   TaskManager, not in this container. Without tail, Docker thinks the container
#   finished and restarts it (with restart: on-failure), causing a loop.
#   tail keeps the container alive without burning CPU.
#
# --jobmanager flink-jobmanager:8081   connects to the running cluster
# --class com.kubocare.delta.DeltaWriterJob  the Scala object with main()
# --parallelism 1                      matches the rest of the pipeline
CMD ["bash", "-c", \
     "/opt/flink/bin/flink run \
       --jobmanager flink-jobmanager:8081 \
       --class com.kubocare.delta.DeltaWriterJob \
       --parallelism 1 \
       /opt/flink/jobs/flink-delta-writer.jar \
     && tail -f /dev/null"]
