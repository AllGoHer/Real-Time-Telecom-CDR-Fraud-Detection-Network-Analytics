from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    print("⚙️  Configurando entorno de PyFlink en modo Streaming...")
    
    # 1. Inicialización estándar
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # 🌟 ¡NUEVA LÍNEA CRUCIAL! Habilitar Checkpointing cada 10 segundos
    t_env.get_config().get_configuration().set_string('execution.checkpointing.interval', '10s')
    print("✅ Checkpointing habilitado cada 10 segundos")

    # 2. Cargar SOLO el conector de Kafka
    jars = ["file:///opt/flink/opt/flink-sql-connector-kafka-3.0.1-1.18.jar"]
    t_env.get_config().get_configuration().set_string("pipeline.jars", ";".join(jars))
    print("✅ Conector de Kafka cargado correctamente (JSON es nativo en Flink)")

    # 3. Crear Tabla Fuente: Kafka (Lectura)
    print("📡 Creando tabla fuente en Kafka...")
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            call_id STRING,
            caller STRING,
            callee STRING,
            duration_sec INT,
            country_code STRING,
            timestamp_str STRING,
            is_suspicious BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'cdr_events',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # 4. Crear Tabla Destino: Filesystem JSON (Capa Bronce - Datos Crudos)
    warehouse_path = "file:///tmp/bronze_data"
    print(f"🧊 Creando tabla destino JSON (Capa Bronce) en: {warehouse_path}")
    
    t_env.execute_sql(f"""
        CREATE TABLE bronze_cdr (
            call_id STRING,
            caller STRING,
            callee STRING,
            duration_sec INT,
            country_code STRING,
            timestamp_str STRING,
            is_suspicious BOOLEAN
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{warehouse_path}',
            'format' = 'json',
            'sink.rolling-policy.file-size' = '10KB',
            'sink.rolling-policy.rollover-interval' = '10s',
            'sink.rolling-policy.check-interval' = '1s'
        )
    """)

    # 5. Ejecutar la inserción continua (Streaming)
    print("🚀 Iniciando ingestión continua de Kafka a JSON (Capa Bronce)...")
    print("   (Este job se quedará corriendo. Presiona Ctrl+C en la terminal para detenerlo)")
    
    table_result = t_env.execute_sql("INSERT INTO bronze_cdr SELECT * FROM kafka_source")
    table_result.wait()

if __name__ == "__main__":
    main()
