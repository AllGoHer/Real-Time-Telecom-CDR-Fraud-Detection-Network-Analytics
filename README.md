# Real-Time-Telecom-CDR-Fraud-Detection-Network-Analytics
_______________________________________________________________________________________________________________________

Pipeline de procesamiento de streaming para detección de fraude en llamadas (CDR) y analítica de red, construido con un stack 100% Open Source y autohospedado.

______________________________________________________________________________________________________________________
## 🏗️ Arquitectura
CDR → Kafka (KRaft) → Flink (PyFlink) → Alertas en tiempo real  
└→ Histórico → MinIO + Iceberg → Spark/dbt → Superset

______________________________________________________________________________________________________________________
## 🚀 Estado del Proyecto
- [x] Fase 1: Configuración del entorno (Docker, Git, WSL2)
- [ ] Fase 2: Kafka KRaft y generación de CDRs sintéticos
- [ ] Fase 3: Procesamiento en tiempo real con PyFlink
- [ ] Fase 4: Detección de fraude por ventanas temporales
- [ ] Fase 5: Lakehouse con MinIO + Iceberg + PySpark
- [ ] Fase 6: Monitorización, visualización y CI/CD
      
______________________________________________________________________________________________________________________
## 🛠️ Stack Tecnológico
- **Ingesta:** Apache Kafka (KRaft mode, sin ZooKeeper)
- **Streaming:** Apache Flink + PyFlink
- **Almacenamiento:** MinIO + Apache Iceberg
- **Batch:** Apache Spark + PySpark
- **Orquestación y Calidad:** Airflow + Great Expectations + dbt Core
