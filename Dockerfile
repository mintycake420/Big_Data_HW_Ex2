# Dockerfile for Spark Big Data Homework
FROM maven:3.9-eclipse-temurin-11

# Install necessary tools
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy project files
COPY pom.xml .
COPY src ./src
COPY data ./data

# Download dependencies
RUN mvn dependency:resolve

# Compile the project
RUN mvn clean compile

# Create output directory
RUN mkdir -p /app/output

# Set environment variables
ENV SPARK_LOCAL_IP=127.0.0.1
ENV MAVEN_OPTS="-Xmx4g"

# Default command
CMD ["/bin/bash"]
