# Guía de inicio rápido de DataHub

## Implementación de DataHub

Para implementar una nueva instancia de DataHub, realice los pasos siguientes.

1.  Instalar [estibador](https://docs.docker.com/install/), [jq](https://stedolan.github.io/jq/download/) y [docker-compose v1 ](https://github.com/docker/compose/blob/master/INSTALL.md) (si
    usando Linux). Asegúrese de asignar suficientes recursos de hardware para el motor de Docker. Configuración probada y confirmada: 2 CPU,
    8 GB de RAM, 2 GB de área de intercambio y 10 GB de espacio en disco.

2.  Inicie Docker Engine desde la línea de comandos o la aplicación de escritorio.

3.  Instalar la CLI de DataHub

    un. Asegúrese de tener Python 3.6+ instalado y configurado. (Compruebe usando `python3 --version`)

    b. Ejecute los siguientes comandos en su terminal

        python3 -m pip install --upgrade pip wheel setuptools
        python3 -m pip uninstall datahub acryl-datahub || true  # sanity check - ok if it fails
        python3 -m pip install --upgrade acryl-datahub
        datahub version

:::nota

Si ve "comando no encontrado", intente ejecutar comandos cli con el prefijo 'python3 -m' en su lugar como `python3 -m datahub version`
Tenga en cuenta que la CLI de DataHub no es compatible con Python 2.x.

:::

4.  Para implementar DataHub, ejecute el siguiente comando cli desde su terminal

        datahub docker quickstart

    Al completar este paso, debería poder navegar a la interfaz de usuario de DataHub
    en <http://localhost:9002> en su navegador. Puede iniciar sesión con `datahub` como ambos el
    nombre de usuario y contraseña.

5.  Para ingerir los metadatos de ejemplo, ejecute el siguiente comando cli desde el terminal

        datahub docker ingest-sample-data

¡Eso es todo! Para comenzar a enviar los metadatos de su empresa a DataHub, eche un vistazo a
el [Marco de ingesta de metadatos](../metadata-ingestion/README.md).

## Restablecimiento de DataHub

Para limpiar DataHub de todo su estado (por ejemplo, antes de ingerir el suyo propio), puede usar la CLI `nuke` mandar.

    datahub docker nuke

## Actualización de DataHub localmente

Si ha estado probando DataHub localmente, se lanzó una nueva versión de DataHub y desea probar la nueva versión, puede usar los siguientes comandos.

    datahub docker nuke --keep-data
    datahub docker quickstart

Esto mantendrá los datos que ha ingerido hasta ahora en DataHub e iniciará un nuevo inicio rápido con la última versión de DataHub.

## Solución de problemas

### Comando no encontrado: datahub

Si la ejecución de la cli de datahub produce errores de "comando no encontrado" dentro de su terminal, es posible que su sistema esté predeterminado a un
versión anterior de Python. Intenta prefijar tu `datahub` comandos con `python3 -m`:

    python3 -m datahub docker quickstart

Otra posibilidad es que el PATH de su sistema no incluya pip's `$HOME/.local/bin` directorio.  En linux, puede agregar esto a su `~/.bashrc`:

    if [ -d "$HOME/.local/bin" ] ; then
        PATH="$HOME/.local/bin:$PATH"
    fi

### Problemas varios de Docker

Puede haber varios problemas con Docker, como contenedores conflictivos y volúmenes colgantes, que a menudo se pueden resolver mediante
podando el estado de Docker con el siguiente comando. Tenga en cuenta que este comando elimina todos los contenedores no utilizados, redes,
imágenes (tanto colgantes como sin referencia), y opcionalmente, volúmenes.

    docker system prune
