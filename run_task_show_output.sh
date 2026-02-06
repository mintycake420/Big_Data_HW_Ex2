#!/bin/bash
# Run a task and show only the important output
export HADOOP_HOME=/c/hadoop
cd "$(dirname "$0")"

echo "=== Running Task 1: Data Cleaning ==="
echo ""
mvn exec:java -Dexec.mainClass="Task1_DataCleaning" 2>&1 | \
  grep -A 200 "=== Task 1" | \
  grep -v "^\[" | \
  head -100
