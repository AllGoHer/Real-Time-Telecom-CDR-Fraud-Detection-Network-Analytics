import polars as pl
import os
import glob

def main():
    print("🔧 Iniciando procesamiento de Capa Plata con Polars...")
    
    bronze_path = "data/bronze_data"
    silver_path = "data/silver_data"
    
    # Crear carpeta de destino si no existe
    os.makedirs(silver_path, exist_ok=True)
    
    # Buscar solo los archivos finales de Flink (los que NO empiezan con '.')
    # Flink nombra los archivos finales como 'part-...' y los temporales como '.part-...inprogress...'
    json_files = [f for f in glob.glob(f"{bronze_path}/part-*") if not os.path.basename(f).startswith('.')]
    
    if not json_files:
        print("⚠️ No se encontraron archivos finales en la Capa Bronce aún.")
        print("   Asegúrate de que el generador de CDRs y el job de Flink estén corriendo y hayan hecho checkpoint.")
        return

    print(f"📖 Leyendo {len(json_files)} archivos de Capa Bronce...")
    
    try:
        # Leer todos los archivos JSON encontrados de forma eficiente (Lazy evaluation)
        df = pl.scan_ndjson(json_files)
        
        print("🧹 Limpiando y enriqueciendo datos...")
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
                # Enriquecimiento: duración en minutos
                (pl.col("duration_sec") / 60).round(2).alias("duration_minutes"),
                
                # Enriquecimiento: tipo de llamada
                pl.when(pl.col("duration_sec") < 30).then(pl.lit("short"))
                 .when(pl.col("duration_sec") < 300).then(pl.lit("medium"))
                 .otherwise(pl.lit("long")).alias("call_type"),
                 
                # Enriquecimiento: es internacional?
                (pl.col("country_code") != "ES").alias("is_international")
            ])
        )
        
        print(f"💾 Escribiendo datos en Capa Plata (Parquet): {silver_path}")
        # Recopilar los datos y escribirlos en un archivo Parquet optimizado
        silver_df.collect().write_parquet(f"{silver_path}/silver_data.parquet")
        
        print("✅ Capa Plata completada exitosamente")
        
        # Mostrar estadísticas
        print("\n📈 Estadísticas de la Capa Plata:")
        stats = silver_df.group_by("call_type").agg(pl.len().alias("count")).collect()
        print(stats)
        
    except Exception as e:
        print(f"❌ Error al procesar los datos: {e}")

if __name__ == "__main__":
    main()