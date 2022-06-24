# Uso de imágenes de Docker durante el desarrollo

Hemos creado un especial `docker-compose.dev.yml` Archivo de anulación que debe configurar las imágenes de Docker para que sean más fáciles de usar
durante el desarrollo.

Normalmente, reconstruirías tus imágenes desde cero con `docker-compose build` (o `docker-compose up --build`). Sin embargo
esto lleva demasiado tiempo para el desarrollo. Tiene que copiar todo el repositorio a cada imagen y reconstruirlo allí.

El `docker-compose.dev.yml` El archivo omite este problema montando binarios, secuencias de comandos de inicio y otros datos en
imágenes especiales y adelgazadas (de las cuales el Dockerfile generalmente se define en `<service>/debug/Dockerfile`).

Estas imágenes de desarrollo utilizarán su *código creado localmente*, por lo que primero deberá construir localmente con gradle
(y cada vez que desee actualizar la instancia). Construir localmente debería ser mucho más rápido que construir en Docker.

Le recomendamos encarecidamente que simplemente invoque el `docker/dev.sh` script que hemos incluido. Es bastante pequeño si quieres leerlo
para ver qué hace, pero termina usando nuestro `docker-compose.dev.yml` archivo.

## Depuración

Las imágenes de desarrollo predeterminadas, aunque están configuradas para usar el código local, no habilitan la depuración de forma predeterminada. Para habilitar la depuración,
necesitas hacer dos pequeñas ediciones (¡no marques estos cambios!).

*   Agregue los indicadores de depuración de JVM al archivo de entorno del servicio.
*   Asigne el puerto en el archivo docker-compose.

Por ejemplo, para depurar `datahub-gms`:

```shell
# Add this line to docker/datahub-gms/env/docker.env. You can change the port and/or change suspend=n to y.
JAVA_TOOL_OPTIONS=-agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n
```

```yml
# Change the definition in docker/docker-compose.dev.yml to this
  datahub-gms:
    image: linkedin/datahub-gms:debug
    build:
      context: datahub-gms/debug
      dockerfile: Dockerfile
    ports:             # <--- Add this line
      - "5005:5005"    # <--- And this line. Must match port from environment file.
    volumes:
      - ./datahub-gms/start.sh:/datahub/datahub-gms/scripts/start.sh
      - ../gms/war/build/libs/:/datahub/datahub-gms/bin
```

## Consejos para personas nuevas en Docker

## Acceso a los registros

Es muy recomendable que utilice [Panel de control de Docker Desktop](https://www.docker.com/products/docker-desktop) para acceder a los registros de servicio. Si hace doble clic en una imagen, se mostrarán los registros por usted.

### Contenedores conflictivos

Si corriste `docker/quickstart.sh` antes, es posible que su máquina ya tenga un contenedor para DataHub. Si quieres correr
`docker/dev.sh` En su lugar, asegúrese de que el contenedor antiguo se quita ejecutando `docker container prune`. Todo lo contrario también
Se aplica.

> Tenga en cuenta que esto solo elimina contenedores, no imágenes. Aún debe ser rápido para cambiar entre estos una vez que haya iniciado ambos
> al menos una vez.

### Personaje inesperado

Si está utilizando Windows WSL (con Ubuntu) y recibe un error de 'carácter inesperado "." en el nombre de la variable ...' durante la ejecución `docker/dev.sh` pruebe estos pasos:

*   Abra Docker Desktop, haga clic en el icono de engranaje en la parte superior para abrir la configuración y desmarque la opción "Usar Docker Compose V2".  Cierre su terminal, abra uno nuevo e intente volver a ejecutar el comando `docker/dev.sh`. En algunos casos, desmarcar la casilla puede no ser suficiente, si el problema persiste, intente ejecutar `docker-compose disable-v2` desde su terminal.
*   A continuación, pruebe `sudo docker/dev.sh` y finalmente, intente mover el archivo `~/.docker/config.json` Para `~/.docker/config.json.bak` y vuelva a intentar el comando con `sudo`.

### Ejecución de un servicio específico

`docker-compose up` iniciará todos los servicios de la configuración, incluidas las dependencias, a menos que ya estén
corriente. Si, por alguna razón, desea cambiar este comportamiento, consulte estos comandos de ejemplo.

    docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up datahub-gms

Solo comenzará `datahub-gms` y sus dependencias.

    docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml up --no-deps datahub-gms

Solo comenzará `datahub-gms`, sin dependencias.
