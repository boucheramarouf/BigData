#!/usr/bin/env bash
set -e

# Vérification des chemins d'entrée et sortie
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  src/jobs/processor.py \
  --raw-base hdfs://namenode:9000/data/apple/raw \
  --silver-base hdfs://namenode:9000/data/apple/silver \
  --run-date "$1" \
  --hive-db apple_platform \
  --log-path logs/processor_"$1".txt
