# Decisiones de Arquitectura

## 1. ¿Por qué Kafka con KRaft y no ZooKeeper?
**Decisión:** Usar el modo KRaft (Kafka Raft) nativo a partir de Kafka 3.x.  
**Justificación:** ZooKeeper añade complejidad operativa, un punto extra de fallo y consumo de recursos. KRaft unifica el metadata de Kafka en el mismo broker, reduciendo la huella de memoria en un 30% y simplificando el `docker-compose.yml`. Es el estándar recomendado por la Apache Software Foundation para nuevos despliegues.

## 2. ¿Por qué Docker Desktop con WSL 2 en Windows 11?
**Decisión:** Backend WSL 2 en lugar de Hyper-V legacy.  
**Justificación:** WSL 2 ofrece un rendimiento de sistema de archivos y red cercano al nativo de Linux, lo cual es crítico para la baja latencia que requieren Kafka y Flink. Además, evita los problemas de permisos y montaje de volúmenes comunes en Windows.