#!/usr/bin/env bash
set -e

# Vérification des fichiers CSV source
if [ ! -f "$1" ]; then
  echo "Le fichier de produits est manquant : $1"
  exit 1
fi

if [ ! -f "$2" ]; then
  echo "Le fichier des stocks est manquant : $2"
  exit 1
fi

spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  src/jobs/feeder.py \
  --products-csv "$1" \
  --stocks-csv "$2" \
  --raw-base hdfs://namenode:9000/data/apple/raw \
  --run-date "$3" \
  --hive-db apple_platform \
  --log-path logs/feeder_"$3".txt
