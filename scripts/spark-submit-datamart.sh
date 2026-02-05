#!/usr/bin/env bash
set -e

# Vérification des chemins d'entrée
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  src/jobs/datamart.py \
  --hive-db apple_platform \
  --gold-base hdfs://namenode:9000/data/apple/gold \
  --run-date "$1" \
  --log-path logs/datamart_"$1".txt
