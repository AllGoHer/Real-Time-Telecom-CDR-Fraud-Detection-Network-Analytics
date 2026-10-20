# Real-Time-Telecom-CDR-Fraud-Detection-Network-Analytics
__________________________________________________________________________________________________________________________________________________________________________________________________
![image](https://github.com/user-attachments/assets/5c7b31a2-7eb0-4ee0-85b0-e742a562b9de)

__________________________________________________________________________________________________________________________________________________________________________________________________
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
️ 
Stack Tecnológico

📁 Estructura del Proyecto

🚀 Quick Start

📊 Resultados en Vivo

📈 Impacto en el Negocio

🔑 Decisiones Arquitectónicas Clave

🛠️ Troubleshooting Highlights

Lo que Aprenderás

🚀 Próximos Pasos

__________________________________________________________________________________________________________________________________________________________________________________________________
## 🎯 Problema de Negocio

Las operadoras de telecomunicaciones pierden $38.1 billones USD anualmente por fraude telefónico (CFCA 2023). Los sistemas tradicionales de detección:

* ❌ Procesan datos en batch (horas/días de retraso)

* ❌ No detectan patrones de ataque en tiempo real

* ❌ Generan falsos positivos que afectan clientes legítimos

* ❌ No escalan con el volumen de CDRs (Call Detail Records)


Este proyecto resuelve estos problemas con una arquitectura de streaming end-to-end.
__________________________________________________________________________________________________________________________________________________________________________________________________
## 💡 Solución Propuesta

Sistema de detección de fraude en tiempo real que procesa CDRs desde su generación hasta la visualización en dashboard en menos de 60 segundos, utilizando:

* Apache Kafka para ingestión de eventos en tiempo real

* Apache Flink para procesamiento de streaming con ventanas de tiempo

* Arquitectura Medallón (Bronze → Silver → Gold) para calidad de datos

* PostgreSQL para almacenamiento de métricas

* Grafana para visualización en tiempo real

__________________________________________________________________________________________________________________________________________________________________________________________________
### 🎯 Características Clave

✅ Latencia < 60 segundos desde la llamada hasta la alerta

✅ Procesamiento de 120+ CDRs/minuto en tiempo real

✅ Detección de patrones por país, duración y frecuencia

✅ Arquitectura escalable lista para producción

✅ 100% containerizado con Docker Compose

__________________________________________________________________________________________________________________________________________________________________________________________________
## 🏗️ Arquitectura

Diagrama de Flujo de Datos

![image](https://github.com/user-attachments/assets/f20293d1-1448-4464-9703-170675b68539)

## 🏗️ Arquitectura del Proyecto



```mermaid
graph LR
    subgraph Windows_Host["️ Windows 11 Host"]
        A[("📱 CDR Generator<br/>Python 3.11 + kafka-python-ng")]
        B[("️ Polars Batch<br/>Limpieza + Enriquecimiento")]
    end

    subgraph Docker_Network["🐳 Docker Network (telecom-network)"]
        subgraph Kafka_Cluster["📨 Apache Kafka 3.8.0 (KRaft)"]
            K[("📥 Topic: cdr_events<br/>3 Particiones<br/>Multi-listener:<br/>• localhost:9094 (Host)<br/>• kafka:9092 (Docker)")]
        end

        subgraph Flink_Cluster[" Apache Flink 1.18.1"]
            JM["🎯 JobManager<br/>Puerto 8081 (Web UI)"]
            TM[" TaskManager<br/>2 Task Slots"]
        end

        subgraph PostgreSQL_DB["🗄️ PostgreSQL 15-alpine"]
            FM[("📊 fraud_metrics<br/>Métricas agregadas")]
            FA[(" fraud_alerts<br/>Alertas individuales")]
        end

        subgraph Grafana_Viz["📊 Grafana 10.4.2"]
            GD["🖥️ Dashboard 360°<br/>Auto-provisioned<br/>3 Paneles:<br/>• Tasa de Fraude<br/>• Volumen<br/>• Tabla métricas"]
        end
    end

    subgraph Shared_Volume["📁 Shared Volume (./data)"]
        BL[("🥉 Bronze Layer<br/>JSON crudo<br/>Checkpoint: 10s<br/>Rolling: 10KB/10s")]
        SL[("🥈 Silver Layer<br/>Parquet columnar<br/>Particionado: country_code")]
    end

    %% Flujo de datos
    A -->|"JSON<br/>localhost:9094<br/>2 CDRs/seg"| K
    K -->|"kafka:9092<br/>latest-offset"| JM
    JM --> TM
    
    TM -->|"PyFlink<br/>Filesystem Sink<br/>Checkpoint 10s"| BL
    BL -->|"Polars<br/>Lazy Evaluation<br/>scan_ndjson()"| B
    B -->|"write_parquet()<br/>Enriquecido"| SL
    
    TM -->|"PyFlink SQL<br/>Tumbling Windows<br/>1 minuto<br/>Watermark: 5s"| FM
    FM --> FA
    
    FM -->|"SQL Queries<br/>$__timeFilter"| GD
    FA -->|"SQL Queries<br/>$__timeFilter"| GD

    %% Estilos
    style Windows_Host fill:#e1f5ff,stroke:#0288d1,stroke-width:2px
    style Docker_Network fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    style Kafka_Cluster fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    style Flink_Cluster fill:#e8f5e9,stroke:#388e3c,stroke-width:2px
    style PostgreSQL_DB fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    style Grafana_Viz fill:#fff9c4,stroke:#fbc02d,stroke-width:2px
    style Shared_Volume fill:#fce4ec,stroke:#c2185b,stroke-width:2px
```



______________________________________________________________________________________________________________________
## Arquitectura Medallón

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
