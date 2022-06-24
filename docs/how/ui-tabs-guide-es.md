# Guía de pestañas de interfaz de usuario

Es posible que algunas de las pestañas de la interfaz de usuario no estén habilitadas de forma predeterminada. Se supone que esta guía le dice a los administradores de DataHub cómo habilitar esas pestañas de la interfaz de usuario.

## Datasets

### Pestaña Estadísticas y consultas

Para habilitar estas pestañas, debe usar una de las fuentes de uso que obtiene los metadatos relevantes de sus fuentes y los ingiere en DataHub. Estas fuentes de uso se enumeran en otras fuentes que las respaldan, por ejemplo. [Fuente de copos de nieve](../../docs/generated/ingestion/sources/snowflake.md), [Fuente bigquery](../../docs/generated/ingestion/sources/bigquery.md)

### Ficha Validación

Esta ficha está habilitada si utiliza [Integración de la calidad de los datos con grandes expectativas](../../metadata-ingestion/integration_docs/great-expectations.md).

## Común a varias entidades

### Ficha Propiedades

Las propiedades son un cajón de sastre para metadatos no capturados en otros aspectos almacenados para un conjunto de datos. Estos se rellenan a través de los diversos conectores de origen cuando [se ingieren metadatos](../../metadata-ingestion/README.md).
