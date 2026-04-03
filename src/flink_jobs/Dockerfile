FROM flink:1.18

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /opt/flink/
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# Download Flink Kafka SQL Connector JAR
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy the PyFlink job
COPY fall_detection_job.py /opt/flink/jobs/

ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
