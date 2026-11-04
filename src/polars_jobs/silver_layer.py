import polars as pl
import os
import glob


def main():
    print("Iniciando procesamiento Silver con Polars...")

    bronze_path = "data/bronze_data"
    silver_path = "data/silver_data"
    output_file = os.path.join(silver_path, "silver_data_v3.parquet")

    os.makedirs(silver_path, exist_ok=True)

    json_files = [
        f for f in glob.glob(os.path.join(bronze_path, "part-*"))
        if not os.path.basename(f).startswith(".")
    ]

    if not json_files:
        print("No se encontraron archivos Bronze.")
        return

    print(f"Leyendo {len(json_files)} archivos Bronze...")

    # Esquema explícito: evita perder columnas nuevas
    # cuando los primeros archivos son históricos.
    schema = {
        "call_id": pl.String,
        "caller": pl.String,
        "callee": pl.String,
        "duration_sec": pl.Int64,
        "country_code": pl.String,
        "timestamp": pl.String,
        "is_suspicious": pl.Boolean,
        "is_burst_event": pl.Boolean,
        "burst_id": pl.String,
        "burst_call_index": pl.Int64
    }

    try:
        df = pl.scan_ndjson(json_files, schema=schema)

        print("Validando y enriqueciendo registros...")

        silver_df = (
            df.filter(
                pl.col("call_id").is_not_null(),
                pl.col("caller").is_not_null(),
                pl.col("callee").is_not_null(),
                pl.col("duration_sec").is_not_null(),
                pl.col("duration_sec") >= 0
            )
            .unique(subset=["call_id"])
            .with_columns([
                (pl.col("duration_sec") / 60)
                    .round(2).alias("duration_minutes"),

                pl.when(pl.col("duration_sec") < 30)
                    .then(pl.lit("short"))
                    .when(pl.col("duration_sec") < 300)
                    .then(pl.lit("medium"))
                    .otherwise(pl.lit("long"))
                    .alias("call_type"),

                (pl.col("country_code") != "ES")
                    .alias("is_international")
            ])
        )

        result = silver_df.collect()

        # Archivo nuevo: no sobrescribe versiones anteriores.
        result.write_parquet(output_file)

        print(f"\nArchivo generado: {output_file}")
        print(f"Total de registros: {result.height}")

        print("\nDistribución de eventos de ráfaga:")
        print(
            result.group_by("is_burst_event")
            .agg(pl.len().alias("count"))
            .sort("is_burst_event")
        )

        print("\nEjemplos de eventos de ráfaga:")
        print(
            result.filter(pl.col("is_burst_event") == True)
            .select([
                "call_id",
                "caller",
                "is_burst_event",
                "burst_id",
                "burst_call_index"
            ])
            .head(10)
        )

        print("\nEstadísticas por tipo de llamada:")
        print(
            result.group_by("call_type")
            .agg(pl.len().alias("count"))
            .sort("call_type")
        )

        print("\nCapa Silver completada.")

    except Exception as e:
        print(f"Error procesando Silver: {e}")
        raise


if __name__ == "__main__":
    main()