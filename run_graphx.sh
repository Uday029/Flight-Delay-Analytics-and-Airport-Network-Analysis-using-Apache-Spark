#!/bin/bash
# Runs the Scala / GraphX program with the spark-shell that comes with the pip package "pyspark".
# Usage (from the project folder, with the virtual environment active):  bash run_graphx.sh
cd "$(dirname "$0")"
SPARK_HOME_DIR=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
"$SPARK_HOME_DIR/bin/spark-shell" -i flights_graphx.scala
# when the Scala prompt  scala>  appears at the end, type  :quit  and press Enter
