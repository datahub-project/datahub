# Realización de copias de seguridad de DataHub

La estrategia de copia de seguridad recomendada es volcar periódicamente la base de datos `datahub.metadata_aspect_v2` para que se pueda volver a crear desde el volcado que admitirán la mayoría de los servicios de base de datos administrados (por ejemplo, AWS RDS). Luego corre [restaurar índices](./restore-indices.md) para recrear los índices.

Para realizar una copia de seguridad de los aspectos de series temporales (que potencian el uso y los perfiles de conjuntos de datos), tendría que realizar una copia de seguridad de Elasticsearch, lo que es posible a través de AWS OpenSearch. De lo contrario, tendría que volver a seleccionar los perfiles de conjuntos de datos de sus fuentes en caso de un escenario de desastre.
