# Real-Time-Telecom-CDR-Fraud-Detection-Network-Analytics
_______________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/5c7b31a2-7eb0-4ee0-85b0-e742a562b9de)

___________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/baa7a835-0a28-483e-ab42-21015289ff16) ![image](https://github.com/user-attachments/assets/0c59d33a-219b-48f8-a617-d8cfbf0ec3a8) ![image](https://github.com/user-attachments/assets/0f59f13c-3489-48a3-9bd9-84754132c214) ![image](https://github.com/user-attachments/assets/edd63e42-6a03-4774-84a3-ddebdc029c43) ![image](https://github.com/user-attachments/assets/42314a70-22a4-4f6d-be37-cb3db9ac3859) ![image](https://github.com/user-attachments/assets/9fb782bb-6891-411c-a381-63cf6ea6cc70)

## Sistema de Detección de Fraude en Tiempo Real para Telecomunicaciones
Arquitectura Medallón + Streaming + ML-Ready

📊 Ver Dashboard | 🏗️ Arquitectura | 🚀 Quick Start | 📈 Métricas de Negocio

Pipeline de procesamiento de streaming para detección de fraude en llamadas (CDR) y analítica de red, construido con un stack 100% Open Source, autohospedado, con visualización dinámica y arquitectura Medallion..
__________________________________________________________________________________________________________________________________________________________________________________________________
### 📋 Tabla de Contenido

🎯 Problema de Negocio
💡 Solución Propuesta
🏗️ Arquitectura
️ Stack Tecnológico
📁 Estructura del Proyecto
🚀 Quick Start
📊 Resultados en Vivo
📈 Impacto en el Negocio
🔑 Decisiones Arquitectónicas Clave
🛠️ Troubleshooting Highlights
Lo que Aprenderás
🚀 Próximos Pasos

__________________________________________________________________________________________________________________________________________________________________________________________________
##🎯 Problema de Negocio
Las operadoras de telecomunicaciones pierden $38.1 billones USD anualmente por fraude telefónico (CFCA 2023). Los sistemas tradicionales de detección:
❌ Procesan datos en batch (horas/días de retraso)
❌ No detectan patrones de ataque en tiempo real
❌ Generan falsos positivos que afectan clientes legítimos
❌ No escalan con el volumen de CDRs (Call Detail Records)
Este proyecto resuelve estos problemas con una arquitectura de streaming end-to-end.
__________________________________________________________________________________________________________________________________________________________________________________________________
## 💡 Solución Propuesta
Sistema de detección de fraude en tiempo real que procesa CDRs desde su generación hasta la visualización en dashboard en menos de 60 segundos, utilizando:
* Apache Kafka para ingestión de eventos en tiempo real
* Apache Flink para procesamiento de streaming con ventanas de tiempo
* Arquitectura Medallón (Bronze → Silver → Gold) para calidad de datos
* PostgreSQL para almacenamiento de métricas
* Grafana para visualización en tiempo real

### 🎯 Características Clave
✅ Latencia < 60 segundos desde la llamada hasta la alerta
✅ Procesamiento de 120+ CDRs/minuto en tiempo real
✅ Detección de patrones por país, duración y frecuencia
✅ Arquitectura escalable lista para producción
✅ 100% containerizado con Docker Compose

__________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura

Diagrama de Flujo de Datos

┌─────────────────────────────────────────────────────────────────────────┐
│                        ARQUITECTURA COMPLETA                            │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Generador  │───▶│   Apache     │───▶ │   Apache     │
│   Python     │     │   Kafka      │     │   Flink      │
│  (CDRs)      │     │  (Broker)    │     │  (Stream)    │
└──────────────┘     └──────────────┘       ──────┬──────┘
                                                  │
                                    ┌─────────────┼────────────┐
                                    │             │            │
                              ┌─────▼─────┐ ┌─────▼───┐ ┌──────▼──────┐
                              │  Bronze   │ │  Silver │ │    Gold     │
                              │   Layer   │ │  Layer  │ │    Layer    │
                              │  (JSON)   │ │(Parquet)│ │ (PostgreSQL)│
                              └───────────┘ └─────────┘ └──────┬──────┘
                                                                │
                                                          ┌─────▼─────┐
                                                          │  Grafana  │
                                                          │(Dashboard)│
                                                          └───────────┘


![image](https://github.com/user-attachments/assets/1aa0bd6d-f0c9-4398-842b-37d00e0e6031)
______________________________________________________________________________________________________________________
## Arquitectura Medallón

 CAPA BRONCE (Raw Data)
├── Formato: JSON crudo
├── Fuente: Kafka (cdr_events topic)
├── Procesamiento: Apache Flink (streaming)
├── Características: Inmutable, append-only, con checkpointing
└── Output: /data/bronze_data/*.json

 CAPA PLATA (Cleaned Data)
├── Formato: Parquet (columnar)
├── Fuente: Bronze Layer
├── Procesamiento: Polars (batch)
├── Transformaciones: Limpieza, validación, enriquecimiento
└── Output: /data/silver_data/*.parquet

🥇 CAPA ORO (Business Metrics)
── Formato: Tablas relacionales
├── Fuente: Kafka (streaming en tiempo real)
├── Procesamiento: Flink SQL (Tumbling Windows de 1 min)
├── Métricas: Tasa de fraude, volumen, duración promedio
└── Output: PostgreSQL (fraud_metrics, fraud_alerts)


![imagen](https://github.com/user-attachments/assets/d737ac6a-b5e0-42cb-9e8b-27b240c23bdd)

🚀 Estado del Proyecto
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

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()

![image]()
