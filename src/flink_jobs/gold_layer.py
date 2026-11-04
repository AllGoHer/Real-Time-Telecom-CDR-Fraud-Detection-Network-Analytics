
import os
import time

from pyflink.table import EnvironmentSettings, TableEnvironment


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable is not set: {name}"
        )
    return value


def sql_string(value: str) -> str:
    """Escape single quotes in Flink SQL string literals."""
    return value.replace("'", "''")


def main():
    print("🔥 Iniciando Capa Oro: Detección de Fraude en Tiempo Real...")

    # 1. Configuración del entorno Flink
    postgres_host = required_env("POSTGRES_HOST")
    postgres_port = required_env("POSTGRES_PORT")
    postgres_db = required_env("POSTGRES_DB")
    postgres_user = sql_string(required_env("POSTGRES_USER"))
    postgres_password = sql_string(required_env("POSTGRES_PASSWORD"))

    postgres_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Limitar paralelismo para reducir conexiones JDBC
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    t_env.get_config().set(
        "pipeline.name", "sentinelstream-gold-metrics"
    )
    # Habilitar checkpointing (crítico para escritura consistente en JDBC)
    t_env.get_config().set("execution.checkpointing.interval", "10s")
    print("✅ Checkpointing habilitado (10s)")

    # 2. Cargar conectores necesarios
    jars = [
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.6.0.jar"
    ]
    t_env.get_config().set("pipeline.jars", ";".join(jars))
    print("✅ Conectores cargados (Kafka, JDBC, PostgreSQL)")

    # 3. TABLA FUENTE: Kafka (datos en tiempo real)
    # Usamos backticks alrededor de timestamp porque es palabra reservada en SQL
    print("📡 Creando tabla fuente desde Kafka...")

    t_env.execute_sql("""
        CREATE TABLE kafka_cdr (
            call_id STRING,
            caller STRING,
            callee STRING,
            duration_sec INT,
            country_code STRING,
            `timestamp` STRING,
            is_suspicious BOOLEAN,
            event_time AS TO_TIMESTAMP(SUBSTRING(REPLACE(`timestamp`, 'T', ' '), 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS'),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'cdr_events',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # 4. TABLA DESTINO: PostgreSQL (métricas de fraude por ventana)
    # Los nombres de las columnas coinciden con la tabla de PostgreSQL
    print("🗄️ Creando tabla destino en PostgreSQL...")

    t_env.execute_sql(f"""
        CREATE TABLE postgres_fraud_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            country_code STRING,
            total_calls BIGINT,
            suspicious_calls BIGINT,
            fraud_percentage DOUBLE,
            avg_duration_sec DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{postgres_url}',
            'table-name' = 'fraud_metrics',
            'username' = '{postgres_user}',
            'password' = '{postgres_password}',
            'sink.buffer-flush.max-rows' = '10',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    # 5. LÓGICA DE DETECCIÓN DE FRAUDE (Ventanas de 1 minuto)
    print("🔍 Aplicando lógica de detección de fraude con ventanas de 1 minuto...")

    t_env.execute_sql("""
        INSERT INTO postgres_fraud_metrics
        SELECT
            window_start,
            window_end,
            country_code,
            COUNT(*) AS total_calls,
            COUNT(CASE WHEN is_suspicious = TRUE THEN 1 END) AS suspicious_calls,
            CAST(COUNT(CASE WHEN is_suspicious = TRUE THEN 1 END) AS DOUBLE)
                / NULLIF(COUNT(*), 0) AS fraud_percentage,
            AVG(duration_sec) AS avg_duration_sec
        FROM TABLE(
            TUMBLE(TABLE kafka_cdr, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        GROUP BY window_start, window_end, country_code
    """)

    print("✅ Capa Oro iniciada exitosamente")
    print("📊 Las métricas de fraude se están escribiendo en PostgreSQL.")
    print("🛑 Presiona Ctrl+C para detener el job")

    # Mantener el job corriendo
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
