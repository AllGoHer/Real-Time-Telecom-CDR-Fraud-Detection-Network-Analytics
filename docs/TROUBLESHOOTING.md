### Problema: "executable file not found in $PATH" al ejecutar scripts de Kafka
**Síntoma:** Al intentar ejecutar `docker exec -it kafka-cdr kafka-topics.sh`, Docker devuelve un error de que el ejecutable no se encuentra.
**Causa:** La imagen oficial de Apache Kafka (`apache/kafka:3.x`) no incluye el directorio `/opt/kafka/bin/` en la variable de entorno `$PATH` del contenedor por diseño de seguridad y minimalismo.
**Solución:** Utilizar la ruta absoluta al ejecutable. Ejemplo: `docker exec -it kafka-cdr /opt/kafka/bin/kafka-topics.sh ...`


### Problema: La terminal parece "congelada" al ejecutar kafka-console-consumer
**Síntoma:** Al ejecutar el consumidor, la terminal no devuelve el prompt de PowerShell y parece no hacer nada.
**Causa:** No es un error. El consumidor de Kafka es un proceso de larga duración (long-running) diseñado para escuchar el tópico de forma continua. Se queda en espera activa hasta que llegan mensajes o hasta que el usuario lo interrumpe.
**Solución:** Para ver resultados, se debe abrir una segunda terminal y usar el `kafka-console-producer` para enviar mensajes. Para detener el consumidor, usar la combinación de teclas `Ctrl + C`.


### Problema: "executable file not found in $PATH" con scripts de Kafka
**Síntoma:** Al ejecutar `docker exec -it kafka-cdr kafka-topics.sh` o `kafka-console-consumer.sh`, Docker devuelve un error de ejecutable no encontrado.
**Causa:** La imagen oficial `apache/kafka:3.8.0` no añade `/opt/kafka/bin/` al `$PATH` del contenedor por diseño de seguridad y minimalismo.
**Solución:** Siempre usar la ruta absoluta. Ejemplo correcto: `docker exec -it kafka-cdr /opt/kafka/bin/kafka-console-consumer.sh ...`



### Problema: La terminal parece "congelada" al ejecutar el consumidor
**Síntoma:** El comando `kafka-console-consumer.sh` no devuelve el prompt de PowerShell y parece no hacer nada.
**Causa:** No es un error. Es un proceso de larga duración (long-running) que queda en espera activa escuchando el tópico.
**Solución:** Abrir una segunda terminal para producir mensajes y verlos aparecer en la primera. Para detener el consumidor, usar `Ctrl + C`.


### Problema: Incompatibilidad de versiones de Python y Java con PyFlink
**Síntoma:** `ModuleNotFoundError: No module named 'pyflink'` a pesar de intentar instalarlo con pip.
**Causa:** El entorno local tiene Python 3.14 y Java 25, pero PyFlink 1.18.1 solo soporta Python 3.8-3.11 y Java 8/11.
**Solución Implementada:** Se migró la ejecución de PyFlink a un contenedor Docker oficial de Apache Flink (`flink:1.18.1-scala_2.12-java11`), que ya incluye las versiones compatibles de Python y Java preinstaladas. Esto elimina los conflictos de versiones y refleja las mejores prácticas de despliegue en producción.


### Problema: AttributeError en EnvironmentSettings y falta de conectores en PyFlink Docker
**Síntoma:** El script falla con `AttributeError: 'EnvironmentSettings' object has no attribute 'with_executor'` o, tras corregirlo, falla indicando que no encuentra el conector de Kafka/Iceberg.
**Causa:** 1) Uso de sintaxis obsoleta de PyFlink para la inicialización del entorno. 2) La imagen oficial de Flink no incluye los JARs de los conectores SQL (Kafka, Iceberg) por defecto, y la instalación de `apache-flink` vía pip solo instala las librerías de Python, no los binarios Java necesarios.
**Solución:** Se corrigió la inicialización a `EnvironmentSettings.in_streaming_mode()`. Además, se mejoró el `Dockerfile.flink` para incluir comandos `wget` que descargan automáticamente los JARs de los conectores en `/opt/flink/opt/` durante la construcción de la imagen, garantizando un entorno 100% funcional y reproducible sin pasos manuales.


### Problema: TimeoutException en Flink al conectar con Kafka en Docker (El error del Advertised Listener)
**Síntoma:** El job de Flink inicia, pero falla a los pocos segundos con `TimeoutException: Timed out waiting for a node assignment` al intentar leer el tópico.
**Causa:** Kafka estaba configurado con `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`. Cuando Flink (dentro de su contenedor) se conecta, Kafka le responde que use `localhost`. Sin embargo, `localhost` dentro del contenedor de Flink se refiere a sí mismo, no al contenedor de Kafka, provocando un fallo de conexión.
**Solución:** Cambiar `KAFKA_ADVERTISED_LISTENERS` para que use el nombre del servicio de Docker Compose (`PLAINTEXT://kafka:9092`). Esto garantiza que los contenedores dentro de la misma red de Docker puedan resolverse y comunicarse entre sí correctamente.


### Problema: Los archivos del Sink de Flink permanecen eternamente como ".inprogress"
**Síntoma:** El job de Flink está corriendo, los datos fluyen, y se crean archivos `.inprogress` en el TaskManager, pero nunca se finalizan (no se renombra a `.json` o `.parquet`), incluso después de varios minutos.
**Causa:** En modo streaming, el conector `filesystem` de Flink requiere que el **Checkpointing** esté habilitado para garantizar la semántica "exactly-once". Flink no finaliza (commits) un archivo de salida hasta que se completa un checkpoint exitoso, para evitar datos corruptos o duplicados en caso de fallo del job.
**Solución:** Habilitar el checkpointing en la configuración del TableEnvironment de PyFlink: `t_env.get_config().get_configuration().set_string('execution.checkpointing.interval', '10s')`.





## FASE 4: Capa Plata y Capa Oro

### Problema 4.1: `UnsatisfiedLinkError` y falta de `winutils.exe` en PySpark (Windows)
**Síntoma:** Al ejecutar PySpark localmente en Windows, falla con `java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0...'`.  
**Causa:** Spark depende de librerías nativas de Hadoop (`winutils.exe` y `hadoop.dll`) para interactuar con el sistema de archivos de Windows. Configurar esto manualmente es propenso a errores, especialmente con versiones modernas de Java (21+) que tienen restricciones de seguridad más estrictas.  
**Solución (Decisión Arquitectónica):** Se reemplazó PySpark por la librería **Polars** para el procesamiento de la Capa Plata en el entorno de desarrollo local. Polars es nativo de Rust/Python, no tiene dependencias de Java/Hadoop, es significativamente más rápido para volúmenes de datos locales y escribe archivos Parquet de forma nativa, eliminando la complejidad innecesaria sin sacrificar la lógica de transformación.

### Problema 4.2: Docker falla al descargar imágenes de Spark (`not found`)
**Síntoma:** `docker compose up` falla con `failed to resolve reference "docker.io/bitnami/spark:3.5.3": not found`.  
**Causa:** Bloqueos de red, rate limits de Docker Hub o cambios en las etiquetas de las imágenes en el registro.  
**Solución:** Dado que ya se adoptó Polars para el procesamiento local (Problema 4.1), se eliminaron por completo los servicios `spark-master` y `spark-worker` del archivo `docker-compose.yml`, simplificando la arquitectura y eliminando este punto de fallo.

### Problema 4.3: Flink no encuentra el conector JDBC (`Could not find any factory for identifier 'jdbc'`)
**Síntoma:** El job de Flink falla al crear la tabla destino en PostgreSQL con el error `Cannot discover a connector using option: 'connector'='jdbc'`.  
**Causa:** Combinación de dos errores:  
1. El nombre del artifact en Maven es `flink-connector-jdbc`, no `flink-sql-connector-jdbc` (error 404 en `wget`).  
2. Los archivos JAR descargados se colocaron en `/opt/flink/opt/`, pero PyFlink requiere que los conectores estén en `/opt/flink/lib/` para que el classloader global los cargue correctamente.  
**Solución:** Se corrigió la URL en el `Dockerfile.flink` a `flink-connector-jdbc-3.1.2-1.18.jar` y se modificó la ruta de destino de todos los `wget` a `/opt/flink/lib/`.

### Problema 4.4: Flink SQL Parser Exception con la columna "timestamp"
**Síntoma:** `SqlParserException: Encountered "timestamp" at line 8, column 13.`  
**Causa:** `timestamp` es una palabra reservada en SQL (y en Flink SQL). Usarla como nombre de columna sin escapar provoca un error de sintaxis en el parser de Calcite/Flink.  
**Solución:** Envolver el nombre de la columna en backticks (`` `timestamp` ``) en la definición de la tabla de Flink SQL para indicar que es un identificador literal que coincide con la clave del JSON.

### Problema 4.5: Job de Flink en estado `RESTARTING` y 0 filas en PostgreSQL
**Síntoma:** El job de Flink aparece como `RUNNING` momentáneamente pero entra en un bucle de `RESTARTING`, y la tabla `fraud_metrics` en PostgreSQL permanece vacía (`0 rows`).  
**Causa:** El generador de Python enviaba fechas en formato ISO 8601 con zona horaria (`2026-10-17T00:18:04.139742+00:00`). La función `TO_TIMESTAMP` de Flink fallaba al parsear este formato complejo, devolviendo `NULL` en la columna `event_time`. Si `event_time` es `NULL`, el **Watermark** nunca avanza. Si el Watermark no avanza, la **Tumbling Window** (ventana de 1 minuto) nunca se considera "cerrada" y Flink nunca dispara la escritura hacia el sink de PostgreSQL.  
**Solución:** Se modificó el generador de Python (`cdr_generator.py`) para emitir fechas en un formato simple y universalmente compatible: `YYYY-MM-DD HH:MM:SS`, utilizando `datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")`. Esto permitió que `TO_TIMESTAMP` funcionara, el Watermark avanzara y las ventanas se cerraran y escribieran correctamente.


## FASE 5: Dashboard en Tiempo Real (Grafana)

### Problema 5.1: Grafana no muestra datos ("No data")
**Síntoma:** El panel de Grafana está vacío a pesar de que PostgreSQL tiene registros.
**Causa:** 
1. El rango de tiempo seleccionado en Grafana (ej. "Last 5 minutes") no coincide con la hora de los datos en PostgreSQL.
2. El generador de datos o el job de Flink se detuvieron, por lo que no hay datos nuevos en la ventana de tiempo actual.
3. Desajuste de zonas horarias entre la JVM de Flink y el navegador web.
**Solución:** Asegurarse de que el generador y Flink estén corriendo. Expandir el rango de tiempo en Grafana a "Last 1 hour" para verificar la presencia de datos. Usar las macros nativas de Grafana (`$__timeFilter(window_start)`) en las consultas SQL para que el filtrado de tiempo se maneje automáticamente.

### Problema 5.2: Error de conexión al Data Source de PostgreSQL
**Síntoma:** Grafana no puede conectarse a la base de datos.
**Causa:** Intentar conectar usando `localhost:5432` desde dentro del contenedor de Grafana. En Docker, `localhost` se refiere al propio contenedor de Grafana.
**Solución:** Utilizar el nombre del servicio de Docker Compose (`postgres-fraud:5432`) en la configuración del Data Source. Además, se implementó *Provisioning* mediante un archivo `datasource.yml` montado en el contenedor, eliminando la necesidad de configuración manual y garantizando la reproducibilidad del entorno.

### Problema 5.3: Conflicto de puerto 3000
**Síntoma:** Docker falla al iniciar Grafana porque el puerto 3000 ya está en uso.
**Causa:** Otra aplicación local (como React, Node.js o otro servicio) ya está escuchando en el puerto 3000.
**Solución:** Cambiar el mapeo de puertos en `docker-compose.yml` de `"3000:3000"` a `"3001:3000"` y acceder a Grafana mediante `http://localhost:3001`.