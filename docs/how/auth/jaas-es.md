# Autenticación JaaS

## Visión general

El servidor frontend DataHub viene con soporte para conectar [JaaS](https://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html) Módulos.
Esto le permite usar un protocolo de autenticación personalizado para iniciar sesión con sus usuarios en DataHub.

De forma predeterminada, incluimos la configuración de muestra de un módulo de autenticación de nombre de usuario / contraseña basado en archivos ([PropiedadFileLoginModule](http://archive.eclipse.org/jetty/8.0.0.M3/apidocs/org/eclipse/jetty/plus/jaas/spi/PropertyFileLoginModule.html))
que se configura con una sola combinación de nombre de usuario / contraseña: datahub - datahub.

Para cambiar o ampliar el comportamiento predeterminado, tiene varias opciones, cada una de las cuales depende del entorno de implementación en el que esté operando.

### Modificar el archivo user.props directamente (pruebas locales)

La primera opción para personalizar los usuarios basados en archivos es modificar el archivo `datahub-frontend/app/conf/user.props` directamente.
Una vez que haya agregado los usuarios deseados, simplemente puede ejecutar `./dev.sh` o `./datahub-frontend/run-local-frontend` para validar su
los nuevos usuarios pueden iniciar sesión.

### Montar un archivo user.props personalizado (Docker Compose)

De forma predeterminada, el `datahub-frontend` El contenedor buscará un archivo llamado `user.props` montado en la ruta del contenedor
`/datahub-frontend/conf/user.props`. Si desea iniciar este contenedor con un conjunto personalizado de usuarios, deberá anular el valor predeterminado
montaje de archivos cuando se ejecuta mediante `docker-compose`.

Para ello, cambie el botón `datahub-frontend-react` en el archivo docker-compose.yml que lo contiene para incluir el archivo personalizado:

    datahub-frontend-react:
        build:
          context: ../
          dockerfile: docker/datahub-frontend/Dockerfile
        image: linkedin/datahub-frontend-react:${DATAHUB_VERSION:-head}
        env_file: datahub-frontend/env/docker.env
        hostname: datahub-frontend-react
        container_name: datahub-frontend-react
        ports:
          - "9002:9002"
        depends_on:
          - datahub-gms
        volumes:
          - ./my-custom-dir/user.props:/datahub-frontend/conf/user.props

Y luego corre `docker-compose up` contra el archivo de redacción.

## Configuración personalizada de JaaS

Para cambiar la configuración predeterminada del módulo JaaS, deberá iniciar el `datahub-frontend-react` contenedor con la custom `jaas.conf` archivo montado como un volumen
en la ubicación `/datahub-frontend/conf/jaas.conf`.

Para ello, cambie el botón `datahub-frontend-react` en el archivo docker-compose.yml que lo contiene para incluir el archivo personalizado:

    datahub-frontend-react:
        build:
          context: ../
          dockerfile: docker/datahub-frontend/Dockerfile
        image: linkedin/datahub-frontend-react:${DATAHUB_VERSION:-head}
        env_file: datahub-frontend/env/docker.env
        hostname: datahub-frontend-react
        container_name: datahub-frontend-react
        ports:
          - "9002:9002"
        depends_on:
          - datahub-gms
        volumes:
          - ./my-custom-dir/jaas.conf:/datahub-frontend/conf/jaas.conf

Y luego corre `docker-compose up` contra el archivo de redacción.
