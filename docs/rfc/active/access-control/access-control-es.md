# RBAC: Controles de acceso detallados en DataHub

## Abstracto

El control de acceso consiste en administrar qué operaciones puede realizar quién. Hay 2 cubos anchos que comprenden el control de acceso:

*   **Autenticación**: Inicio de sesión. Asociar a un actor con una identidad conocida.
*   **Autorización**: Realizar una acción. Permitir /denegar identidades conocidas para realizar tipos específicos de operaciones.

En los últimos meses, han surgido numerosas solicitudes en torno al control del acceso a los metadatos almacenados dentro de DataHub.
En este documento, propondremos un diseño para admitir la autenticación conectable junto con la autorización detallada dentro del backend (GMS) de DataHub.

## Requisitos

Cubriremos los casos de uso en torno al control de acceso en esta sección, recopilados de una multitud de fuentes.

### Personajes

Esta característica está dirigida principalmente al DataHub **Operador** & **Admin** personas (a menudo la misma persona). Esta función puede ayudar a los administradores de DataHub a cumplir con sus respectivas políticas de empresa.

Los beneficiarios secundarios son **Usuarios de datos** ellos mismos. Los controles de acceso detallados permitirán a los propietarios de datos, administradores de datos controlar más estrictamente la evolución de los metadatos bajo administración. También hará que sea más difícil cometer errores al cambiar los metadatos, como anular / eliminar accidentalmente buenos metadatos creados por otra persona.

### Pregunta la comunidad

Sheetal Pratik (Saxo Bank)

**Pregunta**

*   Modele metadatos "dominios" (es decir, ámbitos de recursos, espacios de nombres) utilizando DataHub
*   Definir directivas de acceso que tengan un ámbito para un dominio determinado
*   Capacidad para definir políticas contra los recursos de DataHub en las siguientes granularidades:
    *   recurso individual (basado en clave principal)
    *   tipo de recurso (por ejemplo, todos los 'conjuntos de datos')
    *   acción (por ejemplo. VER, ACTUALIZAR)
        que se puede asociar con solicitudes contra el backend de DataHub a través de la asignación de la información resuelta del actor (principal / nombre de usuario, grupos, etc.).
        Los recursos pueden incluir entidades, sus aspectos, políticas de acceso, etc.
*   Capacidad para componer y reutilizar grupos de políticas de acceso.
*   Compatibilidad con la integración con Active Directory (usuarios, grupos y asignaciones para acceder a directivas)

Alasdair McBride (G-Research)

**Pregunta**

*   Capacidad para organizar varios activos en grupos y asignar políticas de clúster a estos grupos.
*   Capacidad para definir políticas READ / UPDATE / DELETE contra recursos de DataHub en las siguientes granularidades:
    *   recurso individual (basado en clave principal)
    *   tipo de recurso (por ejemplo, todos los 'conjuntos de datos')
    *   grupo de recursos
        que se puede asociar con solicitudes contra el backend de DataHub a través de la asignación de la información resuelta del actor (principal / nombre de usuario, grupos, etc.).
        Los recursos pueden incluir entidades, sus aspectos, roles, políticas, etc.
*   Soporte para entidades de servicio
*   Compatibilidad con la integración con Active Directory (usuarios, grupos y asignaciones para acceder a directivas)

Como habrás notado, los conceptos de "dominio" y "grupo" descritos en cada conjunto de requisitos son bastante similares. Desde aquí
en la parte superior, nos referiremos a un grupo de entidades relacionadas que deben administrarse juntas como un "dominio" de metadatos.

### Historias de usuario

| Como...          | Quiero..                                                                                         | Porque..                                                                                                                                       |
|-----------------|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| | del operador DataHub Restringir los tipos de metadatos que ciertos equipos / individuos pueden cambiar.                         | Reducir los cambios de errores o cambios maliciosos en los metadatos. Mejore la calidad de los metadatos poniéndolos en manos de los más conocedores|
| | del operador DataHub Restrinja los tipos de metadatos que ciertos equipos / individuos pueden ver.                           | Reduzca el riesgo de incumplimiento de las normas mostrando datos confidenciales en la interfaz de usuario de metadatos (valores de datos de muestra y más) |
| | del operador DataHub Conceda la capacidad de administrar políticas de acceso a otros usuarios de DataHub.                              | Quiero delegar esta tarea a los gerentes de equipo individuales. (Organización grande) |
| | del operador DataHub Definir contextos acotados, o "dominios", de metadatos relacionados que se puedan controlar junto | Quiero capacitar a los equipos con la mayor cantidad de conocimiento del dominio para que administren sus propios controles de acceso.                                                         |
| | del operador DataHub Asigne usuarios y grupos de proveedores de identidad de terceros a políticas de acceso resueltas | Quiero reutilizar las definiciones de identidad que mi organización ya tiene |
| | del operador DataHub Cree identidades para los servicios y asócielos a las directivas. (entidades de servicio) | Quiero acceder a DataHub mediante programación mientras cumplo con los controles de acceso restringido.                                                       |
| | de usuario de DataHub Actualizar metadatos que conozco íntimamente. Por ejemplo, descripciones de tablas.                            | Quiero proporcionar metadatos de alta calidad a mis consumidores.                                                                                        |

### Requisitos concretos

#### Imprescindibles

a. una noción central de "usuario autenticado" en el backend de DataHub (GMS).

b. autenticación conectable responsable de resolver los usuarios de DataHub

*   en el ámbito: complemento de contraseña de nombre de usuario basado en archivos (para roles integrados), siga siendo compatible con OIDC
*   en el futuro: saml, ldap / ad, clave de API, complementos de autenticación nativos

c. capacidad para definir políticas de control de acceso detalladas basadas en una combinación de

*   actores: los usuarios + grupos a los que se debe aplicar la directiva (con la capacidad de especificar "todos los usuarios" o "todos los grupos")
*   tipo de recurso: el tipo de recurso al que se accede en la plataforma DataHub (por ejemplo, entidad del conjunto de datos, aspecto del conjunto de datos, roles, privilegios, etc.) (coincidencia exacta o ALL)
*   identificador de recursos: el identificador de clave principal para un recurso (por ejemplo, urna de conjunto de datos) (compatibilidad con la coincidencia de patrones)
*   acción (enlazada al tipo de recurso, por ejemplo, leer + escribir)
*   dominios \[en el futuro]

con soporte para conjunciones opcionales de filtrado en tipo de recurso, & identificador (por ejemplo, tipo de recurso = "entity:dataset:ownership", identificador de recurso = "urn:li:dataset:1", action = "UPDATE")
e incluso con soporte para los siguientes tipos de recursos:

*   entidades de metadatos: conjuntos de datos, gráficos, paneles, etc.
*   aspectos de metadatos: propiedad del conjunto de datos, información de gráficos, etc.
*   objetos de control de acceso: políticas de acceso, etc.

d. capacidad para resolver a los usuarios de DataHub a un conjunto de políticas de acceso

*   donde los metadatos de usuario incluyen nombre principal, nombres de grupo, propiedades de cadena de forma libre

e. capacidad para administrar las políticas de acceso mediante programación a través de la API de Rest

f. capacidad para hacer cumplir políticas de control de acceso detalladas (ref.b) (Implementación del autorizador)

*   Entradas: directivas de acceso resueltas, tipo de recurso, clave de recurso

#### Agradable de tener

a. directivas vinculadas a atributos arbitrarios de un objeto de recurso de destino. (ABAC completo)

b. capacidad para administrar políticas de acceso a través de React UI

c. controles de acceso particionados por dominio (asignar dominios a todos los activos dh + luego permitir políticas que incluyen predicados basados en dominios)

### Cómo es el éxito

Basándonos en los requisitos recopilados al hablar con la gente de la comunidad, decidimos unirnos en torno al siguiente objetivo. Debería ser posible

1.  Definir una directiva de control de acceso con nombre
    *   Granularidad de recursos: individual, tipo de activo
    *   Granularidad de la acción: VER, ACTUALIZAR
        contra un individuo o grupo de recursos de DataHub (entidades, aspectos, roles, políticas)
2.  Definir condiciones de asignación de un usuario autenticado (usuario de DataHub, grupos) a una o más directivas de acceso

En 15 minutos o menos.

## Implementación

Esta sección describirá la solución técnica propuesta para abordar los requisitos establecidos.

### En el ámbito

*   Conectable **Autenticación** en la capa GMS.
*   **Gestión de accesos** en la capa GMS.
*   **Autorización** en la capa GMS.

#### Administración de roles basada en API

Nuestro objetivo es proporcionar una API enriquecida para definir políticas de control de acceso. Una directiva de administración predeterminada será la `datahub` cuenta.
Los nuevos usuarios se asignarán automáticamente a una política "predeterminada" configurable.

### Fuera del alcance

#### Administración de roles basada en la interfaz de usuario

Eventualmente, nuestro objetivo es proporcionar una experiencia en la aplicación para definir políticas de acceso. Esto, sin embargo, no está en el alcance del primer hito entregable.

#### Soporte para autenticación dinámica de nombre de usuario / contraseña local

Inicialmente, nuestro objetivo es admitir la autenticación limitada de nombre de usuario / contraseña local impulsada por un archivo de configuración proporcionado a GMS. No admitiremos sesiones persistentes, contraseñas hash, grupos a una tienda nativa dentro de DataHub (todavía).

#### Soporte para LDAP y AD Nombre de usuario / Autenticación de contraseña

A través de las API que estamos construyendo *será* Al ser susceptibles de admitir tanto Active Directory como la autenticación LDAP (discutida más adelante) no incluiremos la implementación de estos complementos como parte del alcance del impl inicial, ya que usaremos esto como una oportunidad para centrarnos en obtener los aspectos fundamentales de la gestión de acceso correctos.

#### Modelado de dominios en DataHub

Como parte de *éste* iniciativa particular, omitiremos del alcance la implementación de los dominios, o sub-alcances / espacios de nombres
vinculado a recursos en DataHub. Sin embargo, nuestro objetivo es diseñar un sistema adecuado que pueda acomodar políticas basadas en el dominio.
predicados en el futuro.

### Conceptos

Proponemos la introducción de los siguientes conceptos en la plataforma DataHub.

1.  **Actor**: Un usuario o actor del sistema reconocido por DataHub. Definido por un único **principal** nombre y un conjunto opcional de *grupo* Nombres. En la práctica, un actor autenticado será identificado a través de una urna CorpUser. (`urn:li:corpuser:johndoe`)
    1.  **Principal**: un nombre de usuario único asociado a un actor. (Capturado a través de una urna CorpUser)
    2.  **Grupos**: conjunto de grupos a los que pertenece un usuario. (Capturado a través de urnas de CorpGroup)
2.  **Recurso**: Cualquier recurso al que se pueda acceder en la plataforma DataHub. Los ejemplos incluyen entidades, relaciones, roles, etc. Los recursos pueden incluir
    *   Tipo: el tipo único del recurso en la plataforma de DataHub.
3.  **Política**: Una regla de control de acceso detallada compuesta por actores de destino, tipo de recurso, una referencia de recurso y una acción (específica para un tipo de recurso, por ejemplo. Leer, leer / escribir)
    *   Actores: a quién se aplica la política (usuarios + grupos)
    *   Acción: CREAR, LEER, ACTUALIZAR, ELIMINAR
    *   Criterios de coincidencia: tipo de recurso, filtro de referencia

### Componentes

#### Backend de DataHub (datahub-gms)

GMS se aumentará para incluir

1.  un conjunto de tablas de almacén principal relacionadas con autenticación. (SQL)
2.  un conjunto de API de REST relacionadas con Auth.
3.  un filtro de autenticación ejecutado en cada solicitud a GMS.
4.  un componente Autorizador ejecutado dentro de los extremos para autorizar acciones concretas.

**Tablas de autenticación y puntos de conexión**

1.  *Políticas*: Crear, leer y actualizar políticas de acceso detalladas.

<!---->

    // Create a policy.
    POST /gms/policy

    {  
        name: "manage_datasets_msd",
        users: ["urn:li:corpuser:johndoe", "urn:li:corpuser:test"],
        groups: ["urn:li:corpGroup:eng_all"],
        actions: ["VIEW_AND_UPDATE"],
        resource: {
            type: "ENTITY",
            attributes: {
                entity: "dataset",
                urn: ["*"],
            }
        },
        // optional, defaults to "true"
        allow: "true"
    }

En el ejemplo anterior, estamos creando una política de acceso que permite lecturas y escrituras contra el
aspecto de "propiedad" de la entidad "dataset". Hay algunas piezas importantes a tener en cuenta:

1.  Nombre: todas las directivas se denominan
2.  Usuarios / Grupos: los usuarios y grupos a los que debe aplicarse la directiva. Puede ser comodín para todos.
3.  Acción - La acción que debe ser permitida o denegada. Inicialmente enviaremos con ("VIEW", "VIEW_AND_UPDATE")
4.  Recurso: los filtros de recursos. El recurso contra el que se solicita la acción. Los ejemplos pueden ser activos de metadatos específicos,
    políticas, estadísticas de operadores y más.
5.  Permitir: un indicador que determina si se debe permitir la acción en una coincidencia de filtros de usuario / grupo, acción y recursos.

Observe el uso de un tipo de recurso junto con atributos específicos del tipo de recurso. Estos
los atributos servirán como criterios de coincidencia para las especificaciones de recursos pasadas a un componente Authorizer en tiempo de ejecución.
Tenga en cuenta también que los campos de atributos de directiva admitirán la coincidencia de comodines.

La sección de atributos de las directivas proporciona un mecanismo para la extensión en el futuro. Por ejemplo, agregar una calificación de "dominio"
a un recurso, y definir políticas que aprovechen el atributo de dominio sería simplemente una cuestión de agregar a los atributos de recurso.

Se admitirá un indicador "permitir" adicional para determinar si la acción contra el recurso especificado debe permitirse o denegarse.
Esto se establecerá de forma predeterminada en "verdadero", lo que significa que la acción debe permitirse dado que el actor, la acción y los tipos de recursos coinciden con la directiva.

En el momento de la autorización, los usuarios se resolverán con las directivas haciendo coincidir el usuario + grupos especificados en la directiva con el usuario autenticado.
Además, las especificaciones de recursos se construirán mediante el código que invoca el componente de autorización y se compararán con el recurso.
filtros definidos dentro de las políticas.

2.  *Fichas*:

Los tokens se utilizan para **autenticación** a GMS. Se pueden recuperar dada la autenticación a través de otro medio, como la autenticación de nombre de usuario / contraseña.

*   `/generateTokenForActor`: Genera un par de tokens de acceso GMS + actualizar firmado y compatible con Oauth basado en **proporcionado principal, grupo, metadatos**. El autor de la llamada debe estar autorizado para utilizar esta funcionalidad.
*   `/generateToken`: Genera un token de acceso GMS firmado y compatible con OAuth + par de tokens de actualización **Basado en el actor autenticado actualmente**.

**Filtro de autenticación**

El filtro de autenticación será un filtro Rest configurable que se ejecuta en cada solicitud a GMS.

Responsabilidad 1: Autenticación

*Cadena de autenticadores*

Dentro del filtro vivirá una cadena configurable de "Autenticadores" que se ejecutarán en secuencia con el objetivo de resolver un modelo de objetos "Actor" estandarizado, que contendrá los siguientes campos:

1.  `principal` (obligatorio): un identificador único utilizado en DataHub, representado como una urna CorpUser
2.  `groups` (opcional): una lista de grupos asociados al usuario, representados como un conjunto de urnas corpgroup

Tras la resolución de un objeto "Actor", la etapa de autenticación se considerará completa.

Responsabilidad 2: Guardar en el contexto del hilo

Después de resolver el usuario autenticado, el estado del objeto Actor se escribirá en el ThreadContext local, desde donde se recuperará para realizar authorization.

**Autorizador**

El autorizador es un componente al que llamarán los endpoints + servicios internos a GMS para autorizar una acción en particular, por ejemplo, editar una entidad, relación o permiso.

Aceptará los siguientes argumentos:

1.  La especificación del recurso:
    *   tipo de recurso
    *   atributos de recurso
2.  La acción que se está intentando en el recurso
3.  El actor intentando la acción

y realice los siguientes pasos:

1.  Resolver el actor a un conjunto de directivas de acceso relevantes
2.  Evaluar las directivas obtenidas con respecto a las entradas
3.  Si el Actor está autorizado a realizar la acción, permita la acción.
4.  Si el Actor no está autorizado a realizar la acción, deniegue la acción.

![authorizer](./authorizer.png)

Además, el autorizador estará diseñado para admitir múltiples filtros de autorización en una sola cadena de autorizadores.
Esto permite la adición de lógica de autorización personalizada en el futuro, por ejemplo, para resolver "políticas virtuales" basadas en
bordes en el gráfico de metadatos (que se analizan más adelante)

#### Frontend de DataHub (datahub-frontend)

El frontend de DataHub continuará manejando gran parte del trabajo pesado cuando se trata de OIDC SSO por el momento. Sin embargo, los detalles específicos de la autenticación OIDC y de nombre de usuario / contraseña serán ligeramente diferentes en el futuro.

##### Caso 1: OIDC

El frontend de DataHub continuará controlando la autenticación OIDC realizando redireccionamientos al proveedor de identidades y controlando la devolución de llamada desde el proveedor de identidades para la compatibilidad con versiones anteriores. Lo que ocurre después de la autenticación en el proveedor de identidades es lo que cambiará.

Después de una autenticación exitosa con un IdP, el frontend de DataHub realizará los siguientes pasos en `/callback` :

1.  Póngase en contacto con un extremo protegido "generateTokenForUser" expuesto por GMS para generar un token de acceso y actualizar el token de un principal y un conjunto de grupos extraídos del IdP UserInfo. En esta convocatoria, `datahub-frontend` se identificará utilizando una entidad de servicio que vendrá preconfigurada en GMS, lo que le permitirá generar un token en nombre de un usuario a petición. En este mundo, `datahub-frontend` es considerada una parte altamente confiable por GMS.
2.  Establezca los tokens de acceso + actualización en las cookies devueltas al cliente de la interfaz de usuario.

Para todas las llamadas posteriores, `datahub-frontend` se espera que valide la autenticidad del token de acceso emitido por GMS utilizando una clave pública proporcionada en su configuración. Esta clave pública debe coincidir con la clave privada que GMS utiliza para generar el token de acceso original.

Al expirar el token de acceso, `datahub-frontend` será responsable de obtener un nuevo token de acceso de GMS y actualizar las cookies del lado del cliente. (En la etapa de autenticación de datahub-frontend)

![oidc](./oidc.png)

##### Caso 2: Nombre de usuario / Contraseña

En el caso de autenticación de nombre de usuario / contraseña, `datahub-frontend` hará algo nuevo: llamará a un punto final "generateToken" en GMS con un encabezado de autorización especial que contiene autenticación básica: el nombre de usuario y la contraseña proporcionados por el usuario en la interfaz de usuario.

Este punto final validará la combinación de nombre de usuario y contraseña mediante un **Autenticador** (de forma predeterminada existirá uno para validar la cuenta de superusuario "datahub") y devolver un par de tokens de acceso, actualizar token a datahub-frontend. El frontend de DataHub los establecerá como cookies en la interfaz de usuario y los validará utilizando el mismo mecanismo que se discutió anteriormente.

Esto nos permite evolucionar GMS para incluir AUTENTICADORES LDAP, AD y nombre de usuario / contraseña nativos mientras mantenemos **datahub-frontend** Igualmente.

![local-up-auth](./local-up-auth.png)

En el futuro, podremos agregar fácilmente soporte para la autenticación remota de nombre de usuario / contraseña, como el uso de un servicio de directorio LDAP / AD. El flujo de llamadas en tales casos se muestra a continuación

![remote-up-auth](./remote-up-auth.png)

##### Resumen

Al iniciar sesión, datahub-frontend *siempre* llame a GMS para obtener un token de acceso + token de actualización. Estos servirán como credenciales tanto para datahub-frontend, que realizará una validación ligera en el token, como para GMS, que manejará la autorización basada en los grupos principales + asociados con el token.

Es la intención de que GMS eventualmente tome el trabajo pesado de *todo* autenticación, incluida la autenticación OIDC y SAML, que requieren un componente de interfaz de usuario. Tendrá un conjunto de API que `datahub-frontend` podrá utilizarse para realizar los redireccionamientos de SSO correctos y los puntos finales de validación para crear tokens GMS al iniciar sesión correctamente.

Debido a que esto significa implementar la especificación OpenID Connect (OIDC) en la capa GMS, así como agregar una gran cantidad de nuevas API entre datahub-frontend y GMS, hemos decidido retrasar el traslado de la responsabilidad total de OIDC a GMS en este momento. Esto será parte de un hito de la fase 2 de seguimiento en la pista de autenticación.

En el futuro, imaginamos 3 casos que el `frontend` El servidor tendrá que manejar de diferentes maneras:

*   OIDC
*   SAML
*   nombre de usuario / contraseña

Por el contrario, GMS tendrá que conocer los detalles más finos de cada uno, por ejemplo, la capacidad de autenticar nombres de usuario y contraseñas utilizando LDAP / AD, nativo (base de datos local) o almacenes de credenciales basados en archivos.

### Hitos

**Hito 1**

*   Se implementa la primera versión de todos los componentes descritos anteriormente. Autenticadores básicos implementados
    a. gms emitió tokens oauth
    b. combinaciones de nombre de usuario / contraseña basadas en archivos
*   Compatibilidad con la granularidad de la política a nivel de entidad (aún no hay nivel de aspecto)
*   Admite la aplicación de directivas del lado de escritura. (acciones VIEW_AND_UPDATE)
*   Admite la directiva de control de acceso "predeterminada" con capacidad de personalización limitada (permite VIEW en todos los recursos como la directiva de control de acceso predeterminada)
*   Autorizador configurable basado en la propiedad

**Hito 2**

*   Apoyar la aplicación de directivas de lectura. (VER acciones)
*   Agregue compatibilidad para asociar cada recurso almacenado en DataHub con un dominio en particular. Permitir predicados basados en dominios en las directivas.
*   Agregar interfaz de usuario para administrar directivas de acceso

**Hito 3**

un. OpenLDAP authenticator impl
b. Autenticador de Active Directory impl
c. Autenticador SAML impl

## Bono 1: Modelado - Para graficar o no

### Riesgos con el modelado en el gráfico

*   No hay consistencia sólida de lectura después de escritura basada en claves no primarias
    *   No pienses que esto será común dado que el nombre es la clave principal y probablemente por lo que la gente consultará.
*   Filosofía: ¿Son realmente las "políticas" *metadatos*? ¿Pertenecen al gráfico de metadatos?
    *   ¿Deben las directivas ser recuperables en el extremo "/entidades"
    *   ¿Debería haber un gráfico del sistema DataHub interno separado? ¿Almacenado en una tabla mysql separada?
*   Patrón de consulta: ¿El patrón de consulta es drásticamente diferente al de otras entidades? Las políticas a menudo se "leerán todas" y se almacenarán en caché.
*   Si necesitamos migrar fuera del gráfico, tendríamos que hacer una migración de datos de la tabla de aspectos a otra tabla.

### Beneficios del modelado en el gráfico

*   Puede reutilizar las API existentes para buscar, buscar, buscar, crear, etc.
*   Menos código repetitivo. No hay tablas codificadas. Sin embargo, seguiremos queriendo una API REST específica para administrar directivas.

## Bono 2: La cuestión de la propiedad

La cuestión de la propiedad: ¿Cómo podemos apoyar el requisito de que "se debe permitir a los propietarios editar los recursos que poseen"? Podemos hacerlo utilizando autorizadores conectables.

Posible solución: Cadena de autorizador conectable.

![authorizer_chain](./authorizer_chain.png)

Planeamos implementar un "Autorizador de Propiedad" que es responsable de resolver una entidad a su
información de propiedad y generación de una "política virtual" que permita a los propietarios de activos realizar cambios en el
entidades de su propiedad.

Una solución alternativa es permitir una bandera adicional dentro de una política que la marque como aplicable a los "propietarios"
del recurso de destino. El desafío aquí es que no todos los tipos de recursos están garantizados para tener propietarios en primer lugar.

## Referencias

En el proceso de escribir este ERD, investigué los siguientes sistemas para aprender y tomar inspiración:

*   Elasticsearch
*   Pinot
*   Corriente de aire
*   Apache Atlas
*   Guardabosques Apache
