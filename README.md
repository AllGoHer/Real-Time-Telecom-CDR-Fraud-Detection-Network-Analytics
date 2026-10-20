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
## 🏗️ Arquitectura del Proyecto

Diagrama de Flujo de Datos

![image](https://github.com/user-attachments/assets/f20293d1-1448-4464-9703-170675b68539)



__________________________________________________________________________________________________________________________________________________________________________________________________

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
__________________________________________________________________________________________________________________________________________________________________________________________________

## Arquitectura Medallón

 ![imagen](https://github.com/user-attachments/assets/d737ac6a-b5e0-42cb-9e8b-27b240c23bdd)

 _________________________________________________________________________________________________________________________________________________________________________________________________

## 🛠️ Stack Tecnológico

| Categoría | Tecnología | Versión | Propósito |
|-----------|------------|---------|-----------|
| Lenguaje | Python | 3.11+ | Desarrollo de scripts y jobs |
| Message Broker | Apache Kafka | 3.8.0 | Ingestión de eventos en tiempo real |
| Stream Processing | Apache Flink | 1.18.1 | Procesamiento de streaming con ventanas |
| Batch Processing | Polars | Latest | Limpieza y transformación de datos |
| Database | PostgreSQL | 15 | Almacenamiento de métricas |
| Visualization | Grafana | 10.4.2 | Dashboard en tiempo real |
| Containerization | Docker + Compose | Latest | Orquestación de servicios |
| Version Control | Git | Latest | Control de versiones |


**¿Por qué estas tecnologías?**

* **Kafka sobre RabbitMQ:** Mayor throughput, persistencia de mensajes, replay de eventos.
* **Flink sobre Spark Streaming:** Latencia sub-segundo, procesamiento exactamente-una-vez.
* **Polars sobre Pandas:** 10x más rápido, lazy evaluation, mejor manejo de memoria.
* **PostgreSQL sobre MongoDB:** ACID compliance, ventanas de tiempo SQL nativas.

__________________________________________________________________________________________________________________________________________________________________________________________________

## 📁 Estructura del Proyecto

telecom-cdr-fraud-detection/
├── 📄 README.md                          # Este archivo
├── 📄 docker-compose.yml                 # Orquestación de servicios
├── 📄 Dockerfile.flink                   # Imagen personalizada de Flink
├── 📄 .gitignore                         # Archivos ignorados
│
├── 📁 data/                              # Datos generados (no commiteados)
│   ├── bronze_data/                      # Capa Bronce (JSON)
│   └── silver_data/                      # Capa Plata (Parquet)
│
── 📁 src/
│   ├── 📁 producers/
│   │   ├── cdr_generator.py              # Generador de CDRs sintéticos
│   │   └── requirements.txt              # Dependencias Python
│   │
│   ├── 📁 flink_jobs/
│   │   ├── bronze_ingestion.py           # Job: Kafka → Bronze Layer
│   │   └── gold_layer.py                 # Job: Kafka → Gold Layer (PostgreSQL)
│   │
│   └── 📁 spark_jobs/
│       └── silver_layer.py               # Script: Bronze → Silver (Polars)
│
├── 📁 scripts/
│   ├── init_postgres.sql                 # Esquema de base de datos
│   └── kafka-tools.ps1                   # Utilidades de Kafka
│
├── 📁 docker/
│   ── 📁 grafana/
│       └── 📁 provisioning/
│           └── 📁 datasources/
│               └── datasource.yml        # Configuración automática de Grafana
│
└── 📁 docs/
    ├── ARCHITECTURE.md                   # Documentación arquitectónica
    ├── DECISIONS.md                      # Decisiones técnicas
    └── TROUBLESHOOTING.md                # Guía de resolución de problemas


![image](https://github.com/user-attachments/assets/8ce4c1c8-db8f-45f8-9ea4-e541234cc402)
 
### 🛠️ Stack Tecnológico por Proceso

**1. GENERACIÓN DE DATOS (Windows Host)**

| Componente | Tecnología | Versión | Ubicación |
|------------|------------|---------|-----------|
| Lenguaje | Python | 3.11+ | Local (Windows) |
| Librería Kafka | kafka-python-ng | 2.2.2 | src/producers/requirements.txt |
| Script | cdr_generator.py | - | src/producers/ |
| Conexión | Kafka Bootstrap | localhost:9094 | Multi-listener externo |


**Propósito:** Simular tráfico real de telecomunicaciones generando CDRs (Call Detail Records) cada 0.5 segundos con 10% de probabilidad de fraude.

2. MESSAGE BROKER (Docker Container)
Componente
Tecnología
Versión
Configuración Clave
Broker
Apache Kafka
3.8.0
Modo KRaft (sin ZooKeeper)
Listeners
Multi-listener
-
PLAINTEXT://kafka:9092 (interno)
PLAINTEXT_HOST://localhost:9094 (externo)
Tópico
cdr_events
3 particiones
Replication factor: 1
Persistencia
Docker Volume
kafka_data
/var/lib/kafka/data
Propósito: Recibir eventos de CDRs del generador Python y distribuirlos a Flink para procesamiento en tiempo real.
Cambio Crítico Implementado:
✅ Configuración de multi-listener para resolver conflictos de red entre Docker (Flink) y Windows (Python)
✅ Uso de KRaft mode (elimina dependencia de ZooKeeper)
3. PROCESAMIENTO EN STREAMING (Docker Containers)
Componente
Tecnología
Versión
Configuración
Motor
Apache Flink
1.18.1
Scala 2.12, Java 11
Python
Python 3
3.10 (Debian)
Instalado vía apt-get
PyFlink
apache-flink
1.18.1
Instalado vía pip3
JobManager
Flink JobManager
-
Puerto 8081 (Web UI)
TaskManager
Flink TaskManager
-
2 task slots
Conectores
JARs en /opt/flink/lib/
-
Carga global garantizada
Conectores Descargados:

1234
Propósito: Procesar streams de datos en tiempo real con ventanas de tiempo y escribir resultados en PostgreSQL.
Cambios Críticos Implementados:
✅ JARs movidos de /opt/flink/opt/ a /opt/flink/lib/ para carga global sin errores de classpath
✅ Checkpointing habilitado cada 10s para garantizar semántica exactly-once
✅ Formato JSON en Capa Bronce (evita Dependency Hell de Hadoop con Parquet)
✅ Volumen compartido ./data/bronze_data:/tmp/bronze_data para acceso desde Windows
4. CAPA BRONCE - Raw Data (Shared Volume)
Componente
Tecnología
Formato
Características
Almacenamiento
Filesystem (JSON)
JSON crudo
Append-only, inmutable
Ubicación
./data/bronze_data/
Local (Windows)
Mapeado a /tmp/bronze_data en Docker
Política
Rolling Policy
10KB / 10s
Rollover rápido para demo
Checkpoint
Flink Checkpointing
10s
Finaliza archivos .inprogress → .json
Propósito: Almacenar datos crudos e inmutables directamente desde Kafka, garantizando trazabilidad completa.
Decisión Arquitectónica:
❌ NO se usó Parquet (requiere Hadoop dependencies → Dependency Hell)
✅ Se usó JSON (nativo en Flink, sin dependencias externas)
✅ Parquet se deja para Capa Plata con Polars
5. CAPA PLATA - Cleaned Data (Local Processing)
Componente
Tecnología
Versión
Modo de Ejecución
Motor
Polars
Latest
Local (Windows)
Formato Entrada
JSON
-
Desde data/bronze_data/
Formato Salida
Parquet
-
Hacia data/silver_data/
Procesamiento
Batch
-
Lazy evaluation
Transformaciones Aplicadas:
python

123456
Propósito: Limpiar, validar y enriquecer datos crudos, convirtiéndolos a formato columnar optimizado para análisis.
Cambio Crítico Implementado:
❌ Se eliminó Apache Spark (problemas con winutils.exe en Windows)
✅ Se adoptó Polars (10x más rápido que Pandas, sin dependencias de Java/Hadoop)
✅ Ejecución local en lugar de cluster Docker
6. CAPA ORO - Business Metrics (Real-Time)
Componente
Tecnología
Configuración
Motor
Flink SQL
Tumbling Windows de 1 minuto
Fuente
Kafka
kafka_cdr table
Destino
PostgreSQL
fraud_metrics table
Watermark
5 segundos
event_time - INTERVAL '5' SECOND
Checkpoint
10 segundos
Consistencia en JDBC
Métricas Calculadas:
sql

1234
Propósito: Aplicar lógica de negocio en tiempo real para detectar patrones de fraude y almacenar métricas agregadas.
7. ALMACENAMIENTO DE MÉTRICAS (Docker Container)
Componente
Tecnología
Versión
Configuración
Database
PostgreSQL
15-alpine
Ligera y rápida
Usuario
fraud_user
-
Credenciales en docker-compose.yml
Base de Datos
fraud_detection
-
Auto-creada
Tablas
fraud_metrics, fraud_alerts
-
Con índices optimizados
Persistencia
Docker Volume
postgres_data
/var/lib/postgresql/data
Inicialización
SQL Script
init_postgres.sql
Auto-ejecutado en primer start
Esquema de Base de Datos:
sql

12345678910111213141516
Propósito: Almacenar métricas de fraude agregadas por ventanas de tiempo para consulta y visualización.
8. VISUALIZACIÓN EN TIEMPO REAL (Docker Container)
Componente
Tecnología
Versión
Configuración
Dashboard
Grafana
10.4.2
Puerto 3000
Datasource
PostgreSQL_Fraud
-
Auto-provisionado
Credenciales
admin/admin
-
Configuradas en YAML
Provisioning
YAML
datasource.yml
/etc/grafana/provisioning/
Paneles
3 visualizaciones
-
Time series, Bar chart, Table
Paneles del Dashboard:
** Tasa de Fraude por País (%)** - Gráfico de series temporales
📞 Volumen de Llamadas por País - Gráfico de barras
** Detalle de Métricas (Últimos Minutos)** - Tabla en tiempo real
Propósito: Visualizar métricas de fraude en tiempo real con actualizaciones automáticas cada 10-30 segundos.
______________________________________________________________________________________________________________________



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
