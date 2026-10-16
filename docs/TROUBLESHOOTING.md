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