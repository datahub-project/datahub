*   Fecha de inicio: 17/12/2020
*   RFC PR: 2042
*   Pr(s) de implementación: 2044

# Frontend GraphQL (Parte 1)

## Resumen

Este RFC describe una propuesta para implementar una especificación GraphQL en `datahub-frontend`. En última instancia, esta propuesta tiene como objetivo modelar lo siguiente utilizando GraphQL:

1.  Lecturas en el catálogo de metadatos (P1)
2.  Escribe en el catálogo de metadatos (P2)
3.  Autenticar, buscar y navegar en el catálogo de metadatos (P2)

Proponemos que esta iniciativa se lleve a cabo en fases, comenzando con el soporte de lectura estilo CRUD contra las entidades, aspectos y relaciones que componen el catálogo de DataHub. El alcance de este RFC se limita a la Parte 1: lectura contra el catálogo. Cubrirá los temas de la introducción de un punto final graphQL dedicado en `datahub-frontend` y proporcionar una receta para la incorporación de entidades GMS a GQL. Los RFC posteriores abordarán la escritura, la búsqueda y la navegación en el catálogo.

Junto con el RFC, hemos incluido una prueba de concepto que demuestra el soporte de lectura parcial de GQL, mostrando `Dataset` y su relación con `CorpUser`. Los siguientes archivos serán útiles para hacer referencia a medida que lea:

*   `datahub-frontend.graphql` - Donde se define el Esquema GQL.
*   `datahub-frontend/app/conf/routes` - Donde se definen las rutas de la API frontend.
*   `datahub-frontend/app/controllers/GraphQLController.java` - El punto de entrada para ejecutar consultas GQL.
*   `datahub-frontend/app/graphql/resolvers` - Definición de GQL DataFetchers, discutida a continuación.
*   `datahub-dao` - Módulo que contiene los DAO utilizados en la obtención de datos aguas abajo de GMS

Es importante tener en cuenta que hay algunas preguntas que se discutirían mejor entre la comunidad, especialmente las relacionadas con el modelado del Catálogo de metadatos en el frontend. Estos serán cubiertos en el **Preguntas no resueltas** sección a continuación.

## Motivación

Exponer una API GQL para aplicaciones del lado del cliente tiene numerosos beneficios con respecto a la productividad del desarrollador, la capacidad de mantenimiento, el rendimiento, entre otras cosas. Este RFC no intentará enumerar completamente los beneficios de GraphQL como IDL. Para una mirada más profunda a estas ventajas, [éste](https://www.apollographql.com/docs/intro/benefits/) es un buen lugar para comenzar.

Proporcionaremos algunas razones por las que GraphQL es particularmente adecuado para DataHub:

*   **Refleja la realidad**: Los metadatos gestionados por DataHub pueden ser representados naturalmente por un gráfico. Proporcionar la capacidad de consultarlo como tal no solo conducirá a una experiencia más intuitiva para los desarrolladores del lado del cliente, sino que también proporcionará oportunidades más numerosas para la reutilización de código tanto en las aplicaciones del lado del cliente como en el servidor frontend.

*   **Minimiza el área de superficie**: Los diferentes casos de uso de frontend generalmente requieren diferentes cantidades de información. Esto se muestra en los numerosos puntos finales de subrecurso expuestos en la API actual: `/datasets/urn/owners`, `/dataset/urn/institutionalmemory`etc. En lugar de requerir la creación de estos puntos finales individualmente, GraphQL permite al cliente solicitar exactamente lo que necesita, al hacerlo, reduciendo el número de puntos finales que deben mantenerse.

*   **Reduce las llamadas a la API**: Las aplicaciones frontend están orientadas naturalmente alrededor de páginas, cuyos datos normalmente se pueden representar utilizando un solo documento (vista). DataHub no es una excepción. GraphQL permite a los desarrolladores de frontend materializar fácilmente esas vistas, sin requerir una lógica de frontend compleja para coordinar múltiples llamadas a la API.

## Diseño detallado

Esta sección describirá los cambios necesarios para introducir un soporte GQL dentro de `datahub-frontend`, junto con una descripción de cómo podemos modelar entidades GMA en el gráfico.

A un alto nivel, los siguientes cambios se realizarán dentro de `datahub-frontend`:

1.  Definir una especificación de esquema GraphQL (datahub-frontend.graphql)

2.  Configurar un `/graphql` endpoint que acepta solicitudes POST (rutas)

3.  Introduce un `GraphQL` Mando de juego

    a. Configurar el motor graphQL

    b. Analizar, validar y preparar consultas entrantes

    c. Ejecutar consultas

    d. Formatear y enviar la respuesta del cliente

Continuaremos echando un vistazo detallado a cada paso. A lo largo del diseño, haremos referencia a los existentes. `Dataset` entidad, su `Ownership` aspecto, y su relación con el `CorpUser` entidad a efectos ilustrativos.

### Definición del esquema GraphQL

Las API de GraphQL deben tener un esquema correspondiente, que represente los tipos y relaciones presentes en el gráfico. Esto generalmente se define centralmente dentro de un `.graphql` archivo. Hemos introducido `datahub-frontend/app/conf/datahub-frontend.graphql`
a tal efecto:

*datahub-frontend.graphql*

```graphql
schema {
    query: Query
}

type Query {
    dataset(urn: String): Dataset
}

type Dataset {
   urn: String!
   ....
}

type CorpUser {
   urn: String!
   ....
}

...
```

Hay dos clases de tipos de objetos dentro de la especificación GraphQL: **definido por el usuario** tipos y **sistema** Tipos.

**Definido por el usuario** los tipos modelan las entidades y relaciones en su modelo de dominio. En el caso de DataHub: conjuntos de datos, usuarios, métricas, esquemas y más. Estos tipos pueden hacer referencia entre sí en la composición, creando aristas entre los tipos.

**Sistema** los tipos incluyen tipos especiales de "raíz" que proporcionan puntos de entrada al gráfico:

*   `Query`: Se lee en comparación con el gráfico (cubierto en RFC 2042)
*   `Mutation`: Escribe en contra del gráfico
*   `Subscription`: Suscribirse a los cambios dentro del gráfico

En este diseño, nos centraremos en el `Query` tipo. Según los tipos definidos anteriormente, lo siguiente sería una consulta GQL válida:

*Consulta de ejemplo*

```graphql
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        ownership {
           owner {
              username
           }
        }
    }
}
```

Para obtener más información sobre los esquemas y tipos de GraphQL, consulte [aquí](https://graphql.org/learn/schema/).

### Configuración de un extremo de GraphQL

Todas las consultas de GraphQL se atienden a través de un único punto de conexión. Colocamos la nueva ruta POST en `datahub-frontend/conf/routes`:

`POST          /api/v2/graphql                                                 react.controllers.GraphQLController.execute()`

También proporcionamos una implementación de un controlador de juego GraphQL, exponiendo un método de "ejecución". El controlador es responsable de

*   análisis y validación de consultas entrantes
*   delegar la ejecución de la consulta
*   dar formato a la respuesta del cliente

### Ejecución de una consulta

Para ejecutar consultas, usaremos el [graphql-java](https://www.graphql-java.com/) biblioteca.

Hay 2 componentes proporcionados al motor que permiten la ejecución:

1.  **Solucionadores de datos**: Resolver proyecciones individuales de una consulta. Definido para entidades de nivel superior y campos de relación de clave externa

2.  **Cargadores de datos**: Cargue de manera eficiente los datos necesarios para resolver los campos agregando llamadas a orígenes de datos descendentes

**Solucionadores de datos**

Durante las consultas de lectura, la biblioteca "resuelve" cada campo del conjunto de selección de la consulta. Lo hace invocando de forma inteligente las clases proporcionadas por el usuario extendiendo `DataFetcher`. Estos 'resolutores' definen cómo obtener un campo particular en el gráfico. Una vez implementados, los 'resolutores' deben registrarse en el motor de consultas.

En el caso de DataHub, se requerirán resolutores para

*   Entidades disponibles para consulta (campos de tipo de consulta)
*   Relaciones disponibles para el recorrido (campos de clave externa)

*Definición de resolutores*

A continuación encontrará ejemplos de resolutores correspondientes a

*   el `dataset` tipo de consulta definido anteriormente (solucionador de campos de consulta)
*   un `owner` dentro del aspecto Propiedad (resolutor de campo definido por el usuario, referencia de clave externa)

```java
/**
 * Resolver responsible for resolving the 'dataset' field of Query
 */
public class DatasetResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        final DataLoader<String, Dataset> dataLoader = environment.getDataLoader("datasetLoader");
        return dataLoader.load(environment.getArgument("urn"))
                .thenApply(RecordTemplate::data);
    }
}

/**
 * Resolver responsible for resolving the 'owner' field of Ownership.
 */
public class OwnerResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
    @Override
    public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
        final Map<String, Object> parent = environment.getSource();
        final DataLoader<String, CorpUser> dataLoader = environment.getDataLoader("corpUserLoader");
        return dataLoader.load((String) parent.get("owner"))
                .thenApply(RecordTemplate::data);
    }
}
```

Los solucionadores sirven para cargar los datos correctos cuando lo solicita el motor GraphQL utilizando el `get` método. Proporcionado como entrada a los resolutores incluye:

*   Resultado del solucionador de campo primario
*   argumentos opcionales
*   Objeto de contexto opcional
*   mapa de variables de consulta

Para obtener una visión más detallada de los resolutores en graphql-java, consulte el [Obtención de datos](https://www.graphql-java.com/documentation/v11/data-fetching/) documentación.

*Registro de resolutores*

Para registrar a los resolutores, primero construimos un `RuntimeWiring` objeto proporcionado por graphql-java:

```java
private static RuntimeWiring configureResolvers() {
    /*
     * Register GraphQL field Resolvers.
     */
    return newRuntimeWiring()
    /*
     * Query Resolvers
     */
    .type("Query", typeWiring -> typeWiring
        .dataFetcher("dataset", new DatasetResolver())
    )
    /*
     * Relationship Resolvers
     */
    .type("Owner", typeWiring -> typeWiring
        .dataFetcher("owner", new OwnerResolver())
    )
    .build();
}
```

Esto indica al motor qué clases deben invocarse para resolver qué campos.

El `RuntimeWiring` A continuación, se utiliza el objeto para crear un motor GraphQL:

```java
GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, configureResolvers());
GraphQL engine = GraphQL.newGraphQL(graphQLSchema).build();
```

Notarás dentro de los resolutores que utilizamos `DataLoaders` para materializar los datos deseados. Discutiremos esto a continuación.

**Cargadores de datos**

DataLoaders es una abstracción proporcionada por `graphql-java` para hacer que la recuperación de datos de fuentes posteriores sea más eficiente, mediante llamadas por lotes para los mismos tipos de datos.
Los DataLoaders se definen y registran con el motor GraphQL para cada tipo de entidad que se cargará desde un origen remoto.

*Definición de un DataLoader*

A continuación encontrará cargadores de muestra correspondientes a la `Dataset` y `CorpUser` Entidades GMA.

```java
// Create Dataset Loader
BatchLoader<String, com.linkedin.dataset.Dataset> datasetBatchLoader = new BatchLoader<String, com.linkedin.dataset.Dataset>() {
    @Override
    public CompletionStage<List<com.linkedin.dataset.Dataset>> load(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return DaoFactory.getDatasetsDao().getDatasets(keys);
            } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
            }
        });
        }
    };
DataLoader datasetLoader = DataLoader.newDataLoader(datasetBatchLoader);

// Create CorpUser Loader
BatchLoader<String, com.linkedin.identity.CorpUser> corpUserBatchLoader = new BatchLoader<String, com.linkedin.identity.CorpUser>() {
    @Override
    public CompletionStage<List<com.linkedin.identity.CorpUser>> load(List<String> keys) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return DaoFactory.getCorpUsersDao().getCorpUsers(keys);
            } catch (Exception e) {
                throw new RuntimeException("Failed to batch load CorpUsers", e);
            }
        });
    }
};
DataLoader corpUserLoader = DataLoader.newDataLoader(corpUserBatchLoader);
```

En la extensión `BatchLoader`, se debe proporcionar un método de "carga" de un solo lote. Esta API es explotada por el motor GraphQL, que agrega llamadas para entidades del mismo tipo, reduciendo el número de llamadas posteriores realizadas dentro de una sola consulta.

*Registro de un DataLoader*

Los DataLoaders están registrados en el `DataLoaderRegistry` y posteriormente se incluyó como entrada al motor GraphQL:

```java
/*
 * Build DataLoader Registry
 */
DataLoaderRegistry registry = new DataLoaderRegistry();
registry.register("datasetLoader", datasetLoader);
registry.register("corpUserLoader", corpUserLoader);

/*
 * Construct execution input
 */
ExecutionInput executionInput = ExecutionInput.newExecutionInput()
.query(queryJson.asText())
.variables(variables)
.dataLoaderRegistry(register)
.build();

/*
 * Execute GraphQL Query
 */
ExecutionResult executionResult = _engine.execute(executionInput);
```

Para más información sobre `DataLoaders` ver el [Uso de DataLoader](https://www.graphql-java.com/documentation/v15/batching/) Doc.

Para obtener una implementación de referencia completa del proceso de ejecución de la consulta, consulte el  `GraphController.java` clase asociada a este PR.

### Bono: Instrumentación

[graphql-java](https://www.graphql-java.com/) proporciona un [Instrumentación](https://github.com/graphql-java/graphql-java/blob/master/src/main/java/graphql/execution/instrumentation/Instrumentation.java) interfaz que se puede implementar para registrar información sobre los pasos en el proceso de ejecución de la consulta.

Convenientemente `graphql-java` proporciona un `TracingInstrumentation` implementación lista para usar. Esto se puede utilizar para obtener una comprensión más profunda del rendimiento de las consultas, mediante la captura de métricas de seguimiento granulares (es decir, a nivel de campo) para cada consulta. Esta información de seguimiento se incluye en el motor `ExecutionResult`. Desde allí, se puede enviar a un servicio de monitoreo remoto, registrarse o simplemente proporcionarse como parte de la respuesta de GraphQL.

Por ahora, simplemente devolveremos los resultados de seguimiento en la parte de "extensiones" de la respuesta GQL, como se describe en [este documento](https://github.com/apollographql/apollo-tracing). En diseños futuros, podemos considerar proporcionar puntos de extensión más formales para inyectar lógica de monitoreo remoto personalizada.

### Consultas de modelado

La primera fase del despliegue de GQL admitirá búsquedas de clave primaria de entidades de GMA y la proyección de sus aspectos asociados. Para lograrlo,

*   entidades modelo, aspectos y las relaciones entre ellos
*   exponer consultas contra entidades de nivel superior
    usando GQL.

Los pasos propuestos para la incorporación de una entidad GMA, sus aspectos relacionados y las relaciones entre ellos se describirán a continuación.

#### 1. **Modelar una entidad en GQL**

El modelo de entidad debe incluir sus aspectos de GMA, como se muestra a continuación, como campos de objeto simples. El cliente no tendrá que ser íntimamente consciente del concepto de "aspectos". En cambio, debe preocuparse por las entidades y sus relaciones.

*Conjunto de datos de modelado*

```graphql
"""
Represents the GMA Dataset Entity
"""
type Dataset {

    urn: String!

    platform: String!

    name: String!

    origin: FabricType

    description: String

    uri: String

    platformNativeType: PlatformNativeType

    tags: [String]!

    properties: [PropertyTuple]

    createdTime: Long!

    modifiedTime: Long!

    ownership: Ownership
}

"""
Represents Ownership
"""
type Ownership {

    owners: [Owner]

    lastModified: Long!
}

"""
Represents an Owner
"""
type Owner {
    """
     The fully-resolved owner
    """
    owner: CorpUser!

    """
     The type of the ownership
    """
    type: OwnershipType

    """
     Source information for the ownership
    """
    source: OwnershipSource
}
```

Observe que el conjunto de datos `Ownership` aspecto incluye un anidado `Owner` campo que hace referencia a un `CorpUser` tipo. En el modelo GMS, esto se representa como una relación de clave extranjera (urna). En GraphQL, incluimos el *resuelto* , lo que permite al cliente recuperar fácilmente información sobre los propietarios de un conjunto de datos.

Para apoyar el recorrido de esta relación, adicionalmente incluimos un `CorpUser` tipo:

```graphql
"""
Represents the CorpUser GMA Entity
"""
type CorpUser {

    urn: String!

    username: String!

    info: CorpUserInfo

    editableInfo: CorpUserEditableInfo
}
```

#### 2. **Extender el tipo 'Query'**:

GraphQL define un tipo de 'Consulta' de nivel superior que sirve como punto de entrada para las lecturas contra el gráfico. Esto se amplía para admitir la consulta del nuevo tipo de entidad.

```graphql
type Query {
    dataset(urn: String!): Dataset # Add this! 
    datasets(urn: [String]!): [Dataset] # Or if batch support required, add this! 
}
```

#### 3. **Definir y registrar DataLoaders**

Esto se ilustra en la sección anterior. Implica extender `DataLoader` y registrando el cargador en el `DataLoaderRegistry`.

#### 4. **Definir y registrar DataFetcher (Data Resolver)**

Esto se ilustra en la sección anterior. Implica implementar la interfaz DataFetcher y adjuntarla a los campos del gráfico.

#### 5. **Consulta tu nueva entidad**

Implemente y comience a emitir consultas contra el gráfico.

```graphql
// Input
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        urn 
        ownership {
            owners {
                owner {
                    username
                }
            }
        }
    }
}

// Output 
{
    "data": {
        "dataset": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:foo,bar,PROD)",
            "ownership": {
                "owners": [
                    {
                        "owner": {
                            "username": "james joyce",
                        }
                    }
                ]
            }
        }
    }
}
```

**Renuncia**

Es la intención que el sistema de tipos GraphQL sea **generado automáticamente** basado en los modelos PDL declarados en la capa GMA. Esto significa que no es necesario mantener el esquema frontend por separado de la GMA de la que se deriva.

Se requerirán varios cambios para lograr esto:

1.  Introducir `readOnly` anotación en las PDL de GMA utilizadas para identificar qué campo debe *no* ser escribible.
2.  Introducir `relationship` anotación en PDL de GMA utilizada para declarar relaciones de clave externa (alternativa a la convención de relaciones + emparejamientos que existe hoy en día)
3.  Implementar un generador de esquemas GraphQL que se pueda configurar para
    *   Cargar PDL de entidad relevante
    *   Generar `Query` tipos, incluidas las relaciones resueltas
    *   Generar `Mutation` tipos, omitiendo campos específicos
    *   Escribir tipos generados en el archivo de esquema GraphQL
    *   Ejecutar como una tarea de nivel en tiempo de compilación

Omitimos la propuesta de esta parte del diseño de este RFC. Habrá un RFC posterior que proponga la implementación de los puntos anteriores.

\*También se debe explorar la generación de resolutores GQL generados automáticamente. Esto sería posible siempre que los DAO estandarizados estén disponibles en la capa de servicio frontend. Sería increíble si pudiéramos

*   Generar automáticamente objetos "cliente" a partir de una especificación rest
*   Generar automáticamente objetos "dao" que utilizan un cliente
*   Generar automáticamente "resolutores" que utilizan un "dao"

## Cómo enseñamos esto

Crearemos guías de usuario que cubran:

*   Modelado e incorporación de entidades al sistema de tipos GQL
*   Aspectos de la entidad de modelado e incorporación al sistema de tipos GQL
*   Modelado y relaciones de incorporación entre entidades al sistema de tipo GQL

## Alternativas

Mantenga el enfoque orientado a los recursos, con diferentes puntos finales para cada entidad / aspecto.

## Estrategia de implementación / adopción

1.  El estilo CRUD de implementación se lee contra entidades / aspectos en el gráfico
    *   Entidades: Dataset, CorpUser, DataPlatform
    *   Relaciones: Dataset->CorpUser, Dataset->DataPlatform, \[si hay demanda] CorpUser -> Dataset
2.  El estilo CRUD de implementación escribe contra entidades / aspectos en el gráfico
3.  Despliegue de la búsqueda de texto completo contra entidades / aspectos en el gráfico
4.  Despliegue de navegación contra entidades / aspectos en el gráfico
5.  Migrar aplicaciones del lado cliente para aprovechar la nueva API de GraphQL
    *   Cree una capa de efecto de datos paralela, donde los modelos existentes sean rellenados por los clientes antiguos o los nuevos clientes GQL. Coloque esto detrás de un indicador de característica del lado del cliente.
    *   Permita a los usuarios de DataHub configurar con qué API desean ejecutarse, intercambiar a su propio ritmo.

## Preguntas no resueltas

**¿Cómo se deben modelar los aspectos en el GQL Graph?**

Los aspectos deben modelarse como nada más que campos en su entidad matriz. Los clientes frontend no deben requerir la comprensión del concepto de aspecto. En cambio, la obtención de aspectos específicos debería ser una cuestión de consultar a las entidades matrices con proyecciones particulares.

Por ejemplo, recuperando el `Ownership` aspecto de `Datasets` se realiza mediante la siguiente consulta GQL:

```graphql
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        ownership {
            owners {
                owner {
                    username
                }
            }
        }
    }
}
```

a diferencia de lo siguiente:

```graphql
query ownership($datasetUrn: String!) { 
    ownership(datasetUrn: $datasetUrn) { 
        owners {
            owner {
                username
            }
        }
    }
}
```

Los aspectos no deben exponerse en el modelo de 'Consulta' de nivel superior.

**¿Qué debe contener el modelo de gráficos GraphQL? ¿Cómo se debe construir?**

Hay 2 opciones principales:

1.  Exponer directamente los modelos GMS transpuestos (entidades, aspectos tal cual) a través del sistema de tipos GQL. Una alteración sería introducir relaciones resueltas que se extiendan hacia afuera desde los objetos de aspecto.

*   Pros: Más simple porque no es necesario mantener nuevos POJO en el `datahub-frontend` capa (puede reutilizar los modelos de Rest.li proporcionados por GMS). A largo plazo, el sistema de tipo GQL se puede generar directamente a partir de los modelos GMS Pegasus.
*   Contras: Expone a los clientes frontend a todo el gráfico GMA, gran parte del cual puede no ser útil para la presentación

2.  Crear un nuevo modelo de gráfico de capa de presentación

*   Pros: Solo exponga lo que los clientes frontend necesitan del gráfico (simplifica la topología)
*   Contras: Requiere que mantengamos (tal vez generemos) POJOs adicionales específicos para la capa de presentación (¿costo de mantenimiento?)

¡Estamos interesados en obtener comentarios de la comunidad sobre esta pregunta!

**¿Deben incluirse en el esquema GQL las claves externas correspondientes a las relaciones salientes?**

Usando el ejemplo de arriba, eso implicaría un modelo como:

```graphql
"""
Represents an Owner
"""
type Owner {
    """
    Owner URN, e.g. urn:li:corpuser:ldap, urn:li:corpGroup:group_name, and urn:li:product:product_name
    """
    ownerUrn: String!
    
    """
    The fully resolved owner!
    """
    owner: CorpUser!

    """
     The type of the ownership
    """
    type: OwnershipType

    """
     Source information for the ownership
    """
    source: OwnershipSource
}
```

Deberíamos *no* necesidad de incluir dichos campos. Esto se debe a que podemos implementar de manera eficiente la siguiente consulta:

```graphql
query datasets($urn: String!) { 
    dataset(urn: $urn) { 
        urn 
        ownership {
            owners {
                owner {
                    urn
                }
            }
        }
    }
}
```

sin llamar realmente a los servicios descendentes. Al implementar inteligentemente el resolutor de campo "propietario", podemos regresar rápidamente cuando la urna es la única proyección:

```java
/**
* GraphQL Resolver responsible for fetching Dataset owners.
  */
  public class OwnerResolver implements DataFetcher<CompletableFuture<Map<String, Object>>> {
      @Override
      public CompletableFuture<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
          final Map<String, Object> parent = environment.getSource();
          if (environment.getSelectionSet().contains("urn") && environment.getSelectionSet().getFields().size() == 1) {
            if (parent.get("owner") != null) {
               return CompletableFuture.completedFuture(ImmutableMap.of("urn", parent.get("owner"))) 
            }
          }
          ... else load as normal 
      }
  }
}
```

**¿Cómo puedo jugar con estos cambios?**

1.  Aplicar cambios desde esta sucursal localmente
2.  Inicie datahub-gms y dependencias y llene con algunos datos como de costumbre
3.  Lanzar `datahub-frontend` servidor que utiliza `cd datahub-frontend/run && ./run-local-frontend`.
4.  Autentifíquese en http://localhost:9001 (nombre de usuario: datahub) y extraiga la cookie PLAY_SESSION que se establece en su navegador.
5.  Emita una consulta GraphQL utilizando CURL o una herramienta como Postman. Por ejemplo:

````
curl --location --request POST 'http://localhost:9001/api/v2/graphql' \
--header 'X-RestLi-Protocol-Version: 2.0.0' \
--header 'Content-Type: application/json' \
--header 'Cookie: PLAY_SESSION=<your-cookie-here>' \
--data-raw '{"query":"query datasets($urn: String!) { 
\n    dataset(urn: $urn) { 
\n        urn 
\n        ownership {
\n            owners {
\n                owner {
\n                    username
\n                    info {
\n                        manager {
\n                            username 
\n                        }
\n                    }
\n                }
\n                type
\n                source {
\n                    type
\n                    url
\n                }
\n            }\n        }
\n        platform
\n    }
\n}",
"variables":{"urn":"<your dataset urn>"}}'```


````
