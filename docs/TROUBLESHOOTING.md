# Solución de problemas — SentinelStream

## 1. Objetivo

Esta guía reúne problemas conocidos y procedimientos de diagnóstico para el entorno local de SentinelStream. Distingue las decisiones técnicas actuales de los antecedentes de desarrollo.

## 2. Kafka: ejecutable no encontrado

**Síntoma:** Docker devuelve un error indicando que no encuentra `kafka-topics.sh` u otro ejecutable de Kafka.

**Causa:** La imagen oficial de Kafka puede no incluir `/opt/kafka/bin/` en la variable `PATH`.

**Diagnóstico y solución:** Utilizar la ruta absoluta del ejecutable. Por ejemplo:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --help
```

Verificar primero que el servicio `kafka` esté en ejecución y que la ruta del ejecutable exista en la imagen utilizada.

## 3. Kafka: el consumidor parece congelado

**Síntoma:** La terminal no devuelve el prompt después de ejecutar un consumidor.

**Causa:** Los consumidores de Kafka permanecen activos esperando mensajes.

**Solución:** Comprobar que el productor esté enviando mensajes. Utilizar `Ctrl + C` para detener el consumidor cuando corresponda.

## 4. Kafka: problemas de conexión entre contenedores

**Síntoma:** Flink no logra conectarse a Kafka o presenta errores de asignación de nodos y tiempos de espera.

**Causa posible:** Kafka anuncia una dirección que el cliente no puede resolver desde su entorno.

**Configuración actual del proyecto:**
- Desde los contenedores: `kafka:9092`.
- Desde el generador ejecutado en el equipo anfitrión: `localhost:9094`.

**Diagnóstico:** Revisar los listeners anunciados por Kafka, la configuración del cliente y los logs de los contenedores. No asumir que `localhost` dentro de un contenedor apunta a otro contenedor.

## 5. Flink: errores de inicialización o conectores

**Síntoma:** El trabajo falla al inicializar PyFlink o indica que no encuentra un conector.

**Causas posibles:**
- Incompatibilidad entre las versiones de Python, Java y Flink.
- Dependencias o archivos JAR ausentes.
- Configuración incorrecta del entorno de ejecución.
- Ruta incorrecta para los conectores.

**Diagnóstico:** Revisar los logs de `flink-jobmanager` y `flink-taskmanager`, la versión de Flink y la configuración efectiva de los conectores.

La presencia de las bibliotecas Python no garantiza que todos los conectores Java necesarios estén disponibles.

Revisar también `Dockerfile.flink`, donde se definen las versiones de los conectores y del driver JDBC utilizados por el entorno.

## 6. Flink: archivos de salida que permanecen como `.inprogress`
**Síntoma:** Los archivos de salida permanecen en estado temporal y no se finalizan.

**Causa posible:** El conector de archivos puede depender de los checkpoints para confirmar la salida.

**Diagnóstico:** Revisar la configuración de checkpointing, el estado de los checkpoints y los logs del trabajo. Confirmar el comportamiento del conector y la configuración efectiva antes de cambiar parámetros.

## 7. PostgreSQL: no aparecen métricas

**Síntoma:** La tabla de métricas no recibe nuevos registros.

**Causas posibles:**
- El trabajo de Flink está fallando o reiniciándose.
- El conector JDBC no está disponible.
- La conexión o las credenciales son incorrectas.
- Los eventos no llegan al destino esperado.
- Las ventanas de tiempo todavía no se han cerrado.

**Diagnóstico:** Revisar los logs de Flink, el estado del trabajo, la configuración del conector y la conexión con PostgreSQL. Comprobar la tabla y sus registros mediante una consulta de solo lectura.

## 8. Procesamiento temporal: ventanas que no generan resultados

**Síntoma:** El trabajo recibe eventos, pero no genera métricas agregadas.

**Causas posibles:** Fechas mal formadas, marcas de tiempo nulas, configuración incorrecta de watermarks o ventanas que todavía no se han cerrado.

**Diagnóstico:** Verificar el formato de la fecha de los eventos, su conversión a timestamp y el avance de los watermarks. Confirmar que la configuración coincida con el formato real del generador.

## 9. Grafana: paneles sin datos

**Síntoma:** Un panel muestra `No data`.

**Causas posibles:**
- PostgreSQL no contiene registros para el intervalo seleccionado.
- El generador o el trabajo de procesamiento no están produciendo datos.
- La consulta SQL utiliza nombres de tablas o columnas incorrectos.
- El rango de tiempo del dashboard no coincide con las fechas almacenadas.

**Diagnóstico:** Verificar primero los registros en PostgreSQL. Después revisar la consulta del panel, el rango temporal y la conexión del datasource.

## 10. Grafana: error de conexión a PostgreSQL

**Síntoma:** Grafana no puede conectarse a la base de datos.

**Causa posible:** La configuración utiliza `localhost` desde el contenedor de Grafana o apunta a un nombre de servicio incorrecto.

**Diagnóstico:** Revisar `docker/grafana/provisioning/datasources/datasource.yml`, la red de Docker Compose y los logs de Grafana. Dentro de Docker, `localhost` hace referencia al propio contenedor.

Comprobar que el contenedor PostgreSQL (`postgres-fraud`) y Grafana compartan la red `telecom-network`, y que el datasource utilice el nombre de host y puerto configurados.

## 11. Grafana: conflicto de puerto

**Síntoma:** Grafana no inicia porque el puerto publicado ya está ocupado.

**Diagnóstico:** Revisar el mensaje de error y el mapeo de puertos del archivo Compose. Antes de modificarlo, comprobar qué aplicación utiliza el puerto y qué dirección se espera utilizar para acceder a Grafana.

## 12. Capa Silver: errores de procesamiento

**Síntoma:** La transformación no genera el archivo Parquet esperado o falla al validar los datos.

**Diagnóstico:** Revisar:
- La existencia y el formato de los archivos JSON de Bronze.
- Los nombres y tipos de las columnas.
- Los valores nulos y los identificadores duplicados.
- La validez de las duraciones.
- La ruta de entrada y la ruta de salida.

La implementación actual utiliza Polars para la transformación local. No ejecutar el proceso Silver de producción únicamente para comprobarlo, porque puede sobrescribir `data/silver_data/silver_data.parquet`.

## 13. Machine Learning: resultados de anomalías V11

El experimento V11 con Isolation Forest y scikit-learn es independiente del pipeline principal de streaming y utiliza datos sintéticos de CDR.

El modelo utiliza cuatro características de comportamiento de llamadas y un umbral seleccionado durante la calibración. Su puntuación de anomalía no representa una probabilidad calibrada de fraude.

Los resultados obtenidos no constituyen evidencia de detección de fraude real ni demuestran rendimiento en producción.

Si se revisa el experimento, comprobar los archivos de resultados, el modelo y sus metadatos existentes, sin sobrescribirlos ni volver a entrenar el modelo innecesariamente.

## 14. Antecedentes de desarrollo

Durante el desarrollo se evaluaron distintas tecnologías, entre ellas Apache Spark, PySpark y Apache Iceberg. La arquitectura actual utiliza Polars para el procesamiento histórico de la capa Silver. Las tecnologías descartadas no forman parte de las dependencias ni de los servicios activos del proyecto.

## 15. Precauciones operativas

- Revisar los logs antes de modificar configuraciones.
- Confirmar los nombres reales de los servicios en Docker Compose.
- No reiniciar ni reconstruir contenedores sin una razón concreta.
- No utilizar `docker compose down -v` como limpieza rutinaria: puede eliminar volúmenes y datos persistentes.
- No borrar archivos de resultados ni copias de seguridad sin verificar su contenido.
- Registrar los cambios realizados y validar cada corrección por separado.
