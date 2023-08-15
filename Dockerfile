FROM ubuntu:22.04

# Set environment variables for Spark and Java
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/spark
ENV JAVA_HOME=/usr

# Install dependencies
RUN apt-get update && apt-get install -y default-jdk curl python3 python3-pip

# Download and install Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Set environment variables for Spark and PySpark
ENV PATH="${PATH}:${SPARK_HOME}/bin"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"


# Set the directories
RUN mkdir /root/.kaggle
WORKDIR /hema_assignment
COPY requirements.txt ./requirements.txt
RUN mkdir ./landing
RUN mkdir ./bronze
RUN mkdir ./silver
RUN mkdir ./golden

# Install packages from requirements.txt
RUN pip3 install -r ./requirements.txt


# Copy code into the container
COPY etl_process.py .
COPY helper.py .
COPY kaggle.json /root/.kaggle/
RUN chmod 600 /root/.kaggle/kaggle.json