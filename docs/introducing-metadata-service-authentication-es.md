# Autenticación del servicio de metadatos

## Introducción

Recientemente introdujimos la autenticación en la capa de servicio de metadatos. Este documento proporcionará una visión general técnica de la característica dirigida a los desarrolladores.
evaluar u operar DataHub. Incluirá una caracterización de las motivaciones para la característica, los componentes clave en su diseño, las nuevas capacidades que proporciona y las instrucciones de configuración.

## Fondo

Recordemos 2 componentes críticos de la arquitectura de DataHub:

*   **DataHub Frontend Proxy** (datahub-frontend): servidor de recursos que enruta las solicitudes al servicio de metadatos descendente
*   **Servicio de metadatos de DataHub** (datahub-gms) - Fuente de verdad para almacenar y servir DataHub Metadata Graph.

Anteriormente, la autenticación era manejada exclusivamente por el proxy frontend. Este servicio realizaría los pasos siguientes
cuando un usuario navegaba a `http://localhost:9002/`:

un. Compruebe la presencia de un especial `PLAY_SESSION` Galleta.

b. Si la cookie estaba presente + válida, redirija a la página de inicio

c. Si la cookie no era válida, redirija a a) la pantalla de inicio de sesión de DataHub (para [Autenticación JAAS](https://datahubproject.io/docs/how/auth/jaas/) o b) a [Proveedor de identidad OIDC configurado](https://datahubproject.io/docs/how/auth/sso/configure-oidc-react/) para realizar la autenticación.

Una vez que la autenticación se hubiera realizado correctamente en la capa de proxy frontend, se establecería una cookie de sesión sin estado (basada en tokens) (PLAY_SESSION) en el navegador de los usuarios.
Todas las solicitudes posteriores, incluidas las solicitudes de GraphQL emitidas por la interfaz de usuario de React, se autenticarán mediante esta cookie de sesión. Una vez que una solicitud había llegado más allá
la capa de servicio frontend, se suponía que ya había sido autenticada. Por lo tanto, hubo **No hay autenticación nativa dentro del servicio de metadatos**.

### Problemas con este enfoque

El principal desafío con esta situación es que las solicitudes al servicio de metadatos de back-end no estaban completamente autenticadas. Había 2 opciones para las personas que requerían autenticación en la capa del servicio de metadatos:

1.  Configurar un proxy delante del servicio de metadatos que realizó la autenticación
2.  \[Una posibilidad más reciente] Enrutar solicitudes al servicio de metadatos a través del proxy frontend de DataHub, incluido el PLAY_SESSION
    Cookie con cada solicitud.

Ninguno de los dos es ideal. Configurar un proxy para realizar la autenticación requiere tiempo y experiencia. Extraer y configurar una cookie de sesión del navegador para programática es
torpe e inescalable. Además de eso, extender el sistema de autenticación fue difícil, lo que requirió la implementación de un nuevo [Módulo de reproducción](https://www.playframework.com/documentation/2.8.8/api/java/play/mvc/Security.Authenticator.html) dentro de DataHub Frontend.

## Introducción a la autenticación en el servicio de metadatos de DataHub

Para abordar estos problemas, introdujimos la autenticación configurable dentro del **Servicio de metadatos** se
lo que significa que las solicitudes ya no se consideran de confianza hasta que el servicio de metadatos las autentica.

¿Por qué presionar la autenticación hacia abajo? Además de los problemas descritos anteriormente, queríamos planificar un futuro.
donde la autenticación de escrituras basadas en Kafka podría realizarse de la misma manera que las escrituras Rest.

A continuación, cubriremos los componentes que se introducen para admitir la autenticación dentro del servicio de metadatos.

### Conceptos y componentes clave

Introdujimos algunos conceptos importantes en el servicio de metadatos para que la autenticación funcione:

1.  Actor
2.  Autenticador
3.  AuthenticatorChain
4.  AuthenticationFilter
5.  Token de acceso a DataHub
6.  Servicio de token DataHub

En las siguientes secciones, echaremos un vistazo más de cerca a cada uno individualmente.

![](./imgs/metadata-service-auth.png)
*Información general de alto nivel sobre la autenticación del servicio de metadatos*

#### ¿Qué es un actor?

Un **Actor** es un concepto dentro del nuevo subsistema de autenticación para representar una identidad / entidad de seguridad única que está iniciando acciones (por ejemplo, solicitudes de lectura y escritura)
en la plataforma.

Un actor se puede caracterizar por 2 atributos:

1.  **Tipo**: El "tipo" del actor que hace una solicitud. El propósito es, por ejemplo, distinguir entre un actor de "usuario" y "servicio". Actualmente, el tipo de actor "usuario" es el único
    formalmente apoyado.
2.  **Identificación**: Un identificador único para el actor dentro de DataHub. Esto se conoce comúnmente como "principal" en otros sistemas. En el caso de los usuarios, esto
    representa un "nombre de usuario" único. Este nombre de usuario se utiliza a su vez al convertir el concepto "Actor" en una Urna de Entidad de Metadatos (por ejemplo, CorpUserUrn).

Por ejemplo, el superusuario raíz "datahub" tendría los siguientes atributos:

    {
       "type": "USER",
       "id": "datahub"
    }

Que se asigna a la urna CorpUser:

    urn:li:corpuser:datahub

para la recuperación de metadatos.

#### ¿Qué es un autenticador?

Un **Autenticador** es un componente conectable dentro del servicio de metadatos que es responsable de autenticar un contexto proporcionado de solicitud entrante sobre la solicitud (actualmente, los encabezados de solicitud).
La autenticación se reduce a resolver con éxito un **Actor** para asociarse con la solicitud entrante.

Puede haber muchos tipos de Authenticator. Por ejemplo, puede haber autenticadores que

*   Verifique la autenticidad de los tokens de acceso (es decir, emitidos por el propio DataHub o un IdP de 3ª parte)
*   Autenticar las credenciales de nombre de usuario / contraseña en una base de datos remota (es decir. LDAP)

¡y más! Un objetivo clave de la abstracción es *extensibilidad*: se puede desarrollar un autenticador personalizado para autenticar las solicitudes
basado en las necesidades únicas de una organización.

DataHub se envía con 2 autenticadores de forma predeterminada:

*   **DataHubSystemAuthenticator**: Comprueba que las solicitudes entrantes se han originado desde dentro del propio DataHub mediante un identificador de sistema compartido
    y secreto. Este autenticador siempre está presente.

*   **DataHubTokenAuthenticator**: Comprueba que las solicitudes entrantes contienen un token de acceso emitido por DataHub (que se describe con más detalle en la sección "Token de acceso de DataHub" a continuación) en su
    Encabezado 'Autorización'. Este autenticador es necesario si la autenticación del servicio de metadatos está habilitada.

#### ¿Qué es un AuthenticatorChain?

Un **AuthenticatorChain** es una serie de **Autenticadores** que están configurados para ejecutarse uno tras otro. Esto permite
para configurar varias formas de autenticar una solicitud determinada, por ejemplo, a través de LDAP O a través de un archivo de clave local.

Solo si cada autenticador dentro de la cadena no puede autenticar una solicitud, se rechazará.

La cadena de autenticadores se puede configurar en el `application.yml` en `authentication.authenticators`:

    authentication:
      .... 
      authenticators:
        # Configure the Authenticators in the chain 
        - type: com.datahub.authentication.Authenticator1
          ...
        - type: com.datahub.authentication.Authenticator2 
        .... 

#### ¿Qué es AuthenticationFilter?

El **AuthenticationFilter** es un [filtro de servlets](http://tutorials.jenkov.com/java-servlets/servlet-filters.html) que autentica cada uno y solicita al servicio de metadatos.
Lo hace construyendo e invocando un **AuthenticatorChain**, descrito anteriormente.

Si authenticatorChain no puede resolver un actor, el filtro devolverá una excepción 401 no autorizada.

#### ¿Qué es un servicio de token de DataHub? ¿Qué son los tokens de acceso?

Junto con la autenticación del servicio de metadatos viene un nuevo componente importante llamado **Servicio de token DataHub**. El propósito de esto
el componente es doble:

1.  Generar tokens de acceso que otorguen acceso al servicio de metadatos
2.  Comprobar la validez de los tokens de acceso presentados al servicio de metadatos

**Tokens de acceso** otorgado por el Servicio de Token toma la forma de [Json Web Tokens](https://jwt.io/introduction), un tipo de token sin estado que
tiene una vida útil finita y se verifica utilizando una firma única. Los JWT también pueden contener un conjunto de notificaciones incrustadas en ellos. Tokens emitidos por el Token
El servicio contiene las siguientes reclamaciones:

*   exp: el tiempo de caducidad del token
*   versión: versión del token de acceso de DataHub para fines de evolubilidad (actualmente 1)
*   tipo: El tipo de token, actualmente SESSION (utilizado para sesiones basadas en UI) o PERSONAL (utilizado para tokens de acceso personal)
*   actorType: El tipo de la **Actor** asociado con el token. Actualmente, USER es el único tipo admitido.
*   actorId: El identificador del **Actor** asociado con el token.

Hoy en día, el servicio de token concede tokens a los tokens en dos escenarios:

1.  **Inicio de sesión de la interfaz de**: Cuando un usuario inicia sesión en la interfaz de usuario de DataHub, por ejemplo, a través de [JaaS](https://datahubproject.io/docs/how/auth/jaas/) o
    [OIDC](https://datahubproject.io/docs/how/auth/sso/configure-oidc-react/)el `datahub-frontend` El servicio emite un
    solicitar al servicio de metadatos que genere un token SESSION *en nombre de* del usuario que inicia sesión. (\*Solo el servicio frontend está autorizado para realizar esta acción).
2.  **Generación de tokens de acceso personal**: cuando un usuario solicita generar un token de acceso personal (que se describe a continuación) desde la interfaz de usuario.

> En la actualidad, el servicio de token admite el método de firma simétrica `HS256` para generar y verificar tokens.

Ahora que estamos familiarizados con los conceptos, hablaremos concretamente sobre qué nuevas capacidades se han construido en la parte superior.
de autenticación de servicio de metadatos.

### Nuevas capacidades

#### Tokens de acceso personal

Con estos cambios, introdujimos una forma de generar un "Token de acceso personal" adecuado para uso programático con DataHub GraphQL
y las API de Rest.li (ingestión) de DataHub.

Los tokens de acceso personal tienen una vida útil finita (predeterminada de 3 meses) y actualmente no se pueden revocar sin cambiar la clave de firma que
DataHub utiliza para generar estos tokens (a través del TokenService descrito anteriormente). Lo más importante es que heredan los permisos
otorgado al usuario que los genera.

##### Generación de tokens de acceso personal

Para generar un token de acceso personal, los usuarios deben haber recibido el privilegio "Generar tokens de acceso personales" (GENERATE_PERSONAL_ACCESS_TOKENS) a través de un [Política de DataHub](./policies.md). Una vez
tienen este permiso, los usuarios pueden navegar a **'Configuración'** > **'Tokens de acceso'** > **'Generar token de acceso personal'** para generar un token.

![](./imgs/generate-personal-access-token.png)

La expiración del token dicta durante cuánto tiempo será válido el token. Recomendamos establecer la duración más corta posible, ya que los tokens no están actualmente
revocable una vez concedida (sin cambiar la clave de firma).

#### Uso de un token de acceso personal

Posteriormente, el usuario podrá realizar solicitudes autenticadas al proxy frontend de DataHub o DataHub GMS directamente proporcionando
el token de acceso generado como token portador en el `Authorization` encabezado:

    Authorization: Bearer <generated-access-token> 

Por ejemplo, usando un rizo al proxy frontend (preferido en producción):

`curl 'http://localhost:9002/api/gms/entities/urn:li:corpuser:datahub' -H 'Authorization: Bearer <access-token>`

o al Servicio de metadatos directamente:

`curl 'http://localhost:8080/entities/urn:li:corpuser:datahub' -H 'Authorization: Bearer <access-token>`

Sin un token de acceso, la realización de solicitudes de programación dará como resultado un resultado 401 del servidor si la autenticación del servicio de metadatos
está habilitado.

### Configuración de la autenticación del servicio de metadatos

La autenticación del servicio de metadatos es actualmente **opt-in**. Esto significa que puede seguir utilizando DataHub sin autenticación del servicio de metadatos sin interrupción.
Para habilitar la autenticación del servicio de metadatos:

*   Establezca el `METADATA_SERVICE_AUTH_ENABLED` variable de entorno a "true" para el `datahub-gms` Y `datahub-frontend` contenedores / vainas.

O

*   Cambiar el servicio de metadatos `application.yml` archivo de configuración para establecer `authentication.enabled` a "verdadero" Y
*   Cambiar el servicio de proxy front-end `application.config` archivo de configuración para establecer `metadataService.auth.enabled` a "verdadero"

Después de establecer el indicador de configuración, simplemente reinicie el servicio de metadatos para comenzar a aplicar la autenticación.

Una vez habilitadas, todas las solicitudes al servicio de metadatos deberán autenticarse; Si usa los autenticadores predeterminados
que se envían con DataHub, esto significa que todas las solicitudes deberán presentar un token de acceso en el encabezado de autorización de la siguiente manera:

    Authorization: Bearer <access-token> 

Para los usuarios que inician sesión en la interfaz de usuario, este proceso se manejará por usted. Al iniciar sesión, se establecerá una cookie en su navegador que internamente
contiene un token de acceso válido para el servicio de metadatos. Al examinar la interfaz de usuario, este token se extraerá y se enviará al servicio de metadatos
para autenticar cada solicitud.

Para los usuarios que desean acceder al servicio de metadatos mediante programación, es decir, para ejecutar la ingesta, la recomendación actual es generar
un **Token de acceso personal** (descrito anteriormente) desde la cuenta de usuario raíz "datahub" y utilizando este token al configurar su [Recetas de ingestión](https://datahubproject.io/docs/metadata-ingestion/#recipes).
Para configurar el token para su uso en la ingesta, simplemente rellene la configuración de "token" para el `datahub-rest` hundir:

    source:
      # source configs
    sink:
      type: "datahub-rest"
      config:
        ...
        token: <your-personal-access-token-here!> 

> Tenga en cuenta que la ingestión que se produce a través de `datahub-kafka` El fregadero seguirá sin autenticarse *Por ahora*. Pronto, presentaremos
> soporte para proporcionar un token de acceso en la carga útil del evento para autenticar las solicitudes de ingesta a través de Kafka.

### El papel del proxy frontend de DataHub en el futuro

Con estos cambios, DataHub Frontend Proxy continuará desempeñando un papel vital en la compleja danza de la autenticación. Servirá como el lugar
donde se origina la autenticación de sesión basada en la interfaz de usuario y seguirá siendo compatible con la configuración de SSO de 3.ª parte (OIDC)
y la configuración de JAAS como lo hace hoy.

La principal mejora es que el servicio Frontend validará las credenciales proporcionadas en el momento del inicio de sesión de la interfaz de usuario
y generar un DataHub **Token de acceso**, incrustándolo en la cookie de sesión tradicional (que seguirá funcionando).

En resumen, DataHub Frontend Service seguirá desempeñando un papel vital para la autenticación. Su alcance, sin embargo, probablemente
siguen limitados a las preocupaciones específicas de la interfaz de usuario de React.

## A dónde ir desde aquí

Estos cambios representan el primer hito en la autenticación del servicio de metadatos. Servirán como base sobre la cual podemos construir nuevas características, priorizadas en función de la demanda de la Comunidad:

1.  **Plugins de Autenticación Dinámica**: Configure + registre implementaciones personalizadas de Authenticator, sin bifurcar DataHub.
2.  **Cuentas de servicio**: Cree cuentas de servicio y genere tokens de acceso en su nombre.
3.  **Autenticación de ingestión de Kafka**: autenticar las solicitudes de ingesta procedentes del receptor de ingesta de Kafka dentro del servicio de metadatos.
4.  **Administración de tokens de acceso**: Capacidad para ver, administrar y revocar tokens de acceso que se han generado. (Actualmente, los tokens de acceso no incluyen ningún estado del lado del servidor y, por lo tanto, no se pueden revocar una vez concedidos)

... ¡y más! Para abogar por estas características u otras, comuníquese con [Flojo](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email).

## Preguntas y respuestas

### ¿Qué sucede si no quiero usar la autenticación del servicio de metadatos?

Eso está perfectamente bien, por ahora. La autenticación del servicio de metadatos está deshabilitada de forma predeterminada, solo se habilita si proporciona el
variable de entorno `METADATA_SERVICE_AUTH_ENABLED` al `datahub-gms` contenedor o cambiar el `authentication.enabled` a "verdadero"
dentro de la configuración del servicio de metadatos de DataHub (`application.yml`).

Dicho esto, le recomendaremos que habilite la autenticación para casos de uso de producción, para evitar
actores arbitrarios de la ingestión de metadatos en DataHub.

### Si habilito la autenticación del servicio de metadatos, ¿dejará de funcionar la ingesta?

Si habilita la autenticación del servicio de metadatos, querrá proporcionar un valor para el valor de configuración "token"
cuando se utiliza el `datahub-rest` hundirse en su [Recetas de ingestión](https://datahubproject.io/docs/metadata-ingestion/#recipes). Ver
el [Rest Sink Docs](https://datahubproject.io/docs/metadata-ingestion/sink_docs/datahub#config-details) para los detalles de configuración.

Recomendamos generar un token de acceso personal (descrito anteriormente) a partir de una cuenta de DataHub de confianza (por ejemplo, usuario raíz de 'datahub') al configurar
sus fuentes de ingestión.

Tenga en cuenta que también puede proporcionar la configuración "extraHeaders" en `datahub-rest` sink para especificar un encabezado personalizado a
pase con cada solicitud. Esto se puede usar junto con la autenticación mediante un autenticador personalizado, por ejemplo.

### ¿Cómo genero un token de acceso para una cuenta de servicio?

No existe un concepto formal de "cuenta de servicio" o "bot" en DataHub (todavía). Por ahora, te recomendamos configurar cualquier
clientes programáticos de DataHub para utilizar un token de acceso personal generado a partir de un usuario con los privilegios correctos, por ejemplo
la cuenta de usuario raíz "datahub".

### ¿Quiero autenticar las solicitudes con un autenticador personalizado? ¿Cómo lo hago?

Puede configurar DataHub para agregar su personalizado **Autenticador** al **Cadena de autenticación** cambiando el `application.yml` archivo de configuración para el servicio de metadatos:

```yml
authentication:
  enabled: true # Enable Metadata Service Authentication 
  ....
  authenticators: # Configure an Authenticator Chain 
    - type: <fully-qualified-authenticator-class-name> # E.g. com.linkedin.datahub.authentication.CustomAuthenticator
      configs: # Specific configs that should be passed into 'init' method of Authenticator
        customConfig1: <value> 
```

Tenga en cuenta que necesitará tener una clase que implemente el `Authenticator` interfaz con un constructor de argumento cero disponible en la ruta de clases
del proceso java del servicio de metadatos.

¡Nos encantan las contribuciones! Siéntase libre de plantear un PR para contribuir con un Authenticator si generalmente es útil.

### Ahora que puedo realizar solicitudes autenticadas al servicio proxy de DataHub y al servicio de metadatos de DataHub, ¿cuál debo usar?

Anteriormente, recomendábamos que las personas se pusieran en contacto directamente con el Servicio de metadatos cuando hicieran cosas como

*   ingerir metadatos a través de recetas
*   emitir solicitudes programáticas a las API de Rest.li
*   emitir solicitudes programáticas a las API de GraphQL

Con estos cambios, cambiaremos a la recomendación de que las personas dirijan todo el tráfico, ya sea programático o no.
al **DataHub Frontend Proxy**, ya que el enrutamiento a los extremos del servicio de metadatos está disponible actualmente en la ruta de acceso `/api/gms`.
Esta recomendación es en un esfuerzo por minimizar el área de superficie expuesta de DataHub para asegurar, operar, mantener y desarrollar
la plataforma más simple.

En la práctica, esto requerirá la migración de metadatos [Recetas de ingestión](https://datahubproject.io/docs/metadata-ingestion/#recipes) utilice el botón `datahub-rest` sumérgete para apuntar a un signo ligeramente diferente
host + ruta.

Receta de ejemplo que se envía a través de DataHub Frontend

```yml
source:
  # source configs
sink:
  type: "datahub-rest"
  config:
    ...
    token: <your-personal-access-token-here!> 
```

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, comuníquese con [Flojo](https://datahubspace.slack.com/join/shared_invite/zt-nx7i0dj7-I3IJYC551vpnvvjIaNRRGw#/shared-invite/email)!
