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