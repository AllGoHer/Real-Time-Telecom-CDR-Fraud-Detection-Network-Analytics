
import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Required environment variable is not set: {name}"
        )
    return value


def sql_string(value: str) -> str:
    """Escape single quotes in SQL string literals."""
    return value.replace("'", "''")


def main():
    print("Starting SentinelStream explainable fraud rules...")

    # PostgreSQL connection from environment variables
    postgres_host = required_env("POSTGRES_HOST")
    postgres_port = required_env("POSTGRES_PORT")
    postgres_db = required_env("POSTGRES_DB")
    postgres_user = sql_string(required_env("POSTGRES_USER"))
    postgres_password = sql_string(required_env("POSTGRES_PASSWORD"))

    postgres_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    # Limitar paralelismo para reducir conexiones JDBC
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("table.exec.resource.default-parallelism", "1")

    t_env.get_config().set(
        "pipeline.name", "sentinelstream-fraud-alerts"
    )

    config = t_env.get_config().get_configuration()
    config.set_string("execution.checkpointing.interval", "10s")

    jars = [
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.6.0.jar",
    ]
    config.set_string("pipeline.jars", ";".join(jars))

    t_env.execute_sql("""
        CREATE TABLE kafka_alert_source (
            call_id STRING,
            caller STRING,
            callee STRING,
            duration_sec INT,
            country_code STRING,
            `timestamp` STRING,
            is_suspicious BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'cdr_events',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'sentinelstream-fraud-alerts',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE postgres_fraud_alerts (
            call_id STRING,
            caller STRING,
            callee STRING,
            duration_sec INT,
            country_code STRING,
            `timestamp` TIMESTAMP(3),
            fraud_score DECIMAL(5, 2),
            fraud_reason STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{postgres_url}',
            'table-name' = 'fraud_alerts',
            'username' = '{postgres_user}',
            'password' = '{postgres_password}',
            'sink.buffer-flush.max-rows' = '1',
            'sink.buffer-flush.interval' = '1s',
            'sink.max-retries' = '3'
        )
    """)

    print("Applying rule R001: short call duration (1-5 seconds)...")


    result = t_env.execute_sql("""
        INSERT INTO postgres_fraud_alerts
        SELECT
            call_id,
            caller,
            callee,
            duration_sec,
            country_code,
            TO_TIMESTAMP(SUBSTRING(REPLACE(`timestamp`, 'T', ' '), 1, 23), 'yyyy-MM-dd HH:mm:ss.SSS'),
            CAST(80.00 AS DECIMAL(5, 2)),
            CONCAT(
                'Short call duration: ',
                CAST(duration_sec AS STRING),
                ' seconds (rule R001)'
            )
        FROM kafka_alert_source
        WHERE duration_sec BETWEEN 1 AND 5
    """)

    result.wait()


if __name__ == "__main__":
    main()
