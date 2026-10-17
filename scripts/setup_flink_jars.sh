#!/bin/bash
# Este script se ejecutará dentro del contenedor de Flink para descargar los JARs necesarios

echo "⬇️ Descargando conectores de Flink..."

# Conector de Kafka
wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar \
  -O /opt/flink/opt/flink-sql-connector-kafka-3.0.1-1.18.jar

# Conector de Iceberg (usamos una versión compatible)
wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar \
  -O /opt/flink/opt/flink-sql-connector-iceberg-1.5.2.jar

echo "✅ JARs descargados exitosamente"