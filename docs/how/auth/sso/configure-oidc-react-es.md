# Autenticación OIDC

La aplicación DataHub React admite la autenticación OIDC construida sobre el [Juego Pac4j](https://github.com/pac4j/play-pac4j) biblioteca.
Esto permite a los operadores de DataHub integrarse con proveedores de identidad de terceros como Okta, Google, Keycloak y más para autenticar a sus usuarios.

Cuando se configure, la autenticación OIDC se habilitará entre los clientes de la interfaz de usuario de DataHub y `datahub-frontend` servidor. Más allá de este punto se considera
para ser un entorno seguro y, como tal, la autenticación se valida y se aplica solo en la "puerta principal" dentro del frontend de datahub.

## Guías específicas del proveedor

1.  [Configuración de OIDC mediante Google](configure-oidc-react-google.md)
2.  [Configuración de OIDC mediante Okta](configure-oidc-react-okta.md)
3.  [Configuración de OIDC mediante Azure](configure-oidc-react-azure.md)

## Configuración de OIDC en React

### 1. Registre una aplicación con su proveedor de identidad

Para configurar OIDC en React, la mayoría de las veces deberá registrarse como cliente con su proveedor de identidad (Google, Okta, etc.). Cada proveedor puede
tienen sus propias instrucciones. A continuación se proporcionan vínculos a ejemplos de Okta, Google, Azure AD y Keycloak.

*   [Registrar una aplicación en Okta](https://developer.okta.com/docs/guides/add-an-external-idp/apple/register-app-in-okta/)
*   [OpenID Connect en Google Identity](https://developers.google.com/identity/protocols/oauth2/openid-connect)
*   [Autenticación de OpenID Connect con Azure Active Directory](https://docs.microsoft.com/en-us/azure/active-directory/fundamentals/auth-oidc)
*   [Keycloak - Guía de seguridad de aplicaciones y servicios](https://www.keycloak.org/docs/latest/securing_apps/)

Durante el proceso de registro, deberá proporcionar un URI de redireccionamiento de inicio de sesión al proveedor de identidades. Esto indica al proveedor de identidades
a dónde redirigir una vez que hayan autenticado al usuario final.

De forma predeterminada, la URL se construirá de la siguiente manera:

> "http://your-datahub-domain.com/callback/oidc"

Por ejemplo, si aloja DataHub en `datahub.myorg.com`éste
el valor sería `http://datahub.myorg.com/callback/oidc`. Para fines de prueba, también puede especificar localhost como nombre de dominio
directamente: `http://localhost:9002/callback/oidc`

El objetivo de este paso debe ser obtener los siguientes valores, que deberán configurarse antes de implementar DataHub:

1.  **ID de cliente** - Un identificador único para su aplicación con el proveedor de identidad
2.  **Secreto del cliente** - Un secreto compartido para usar para el intercambio entre usted y su proveedor de identidad
3.  **URL de descubrimiento** - Una URL donde se puede descubrir la API OIDC de su proveedor de identidades. Esto debe ser sufijo por
    `.well-known/openid-configuration`. A veces, los proveedores de identidad no incluirán explícitamente esta URL en sus guías de configuración, aunque
    este extremo *será* existen según la especificación OIDC. Para obtener más información, consulte http://openid.net/specs/openid-connect-discovery-1\_0.html.

### 2. Configurar el servidor frontend de DataHub

El segundo paso para habilitar OIDC implica configurar `datahub-frontend` para habilitar la autenticación OIDC con su proveedor de identidades.

Para ello, debe actualizar el `datahub-frontend` [docker.esv](../../../../docker/datahub-frontend/env/docker.env) con el archivo
valores recibidos de su proveedor de identidades:

    # Required Configuration Values:
    AUTH_OIDC_ENABLED=true
    AUTH_OIDC_CLIENT_ID=your-client-id
    AUTH_OIDC_CLIENT_SECRET=your-client-secret
    AUTH_OIDC_DISCOVERY_URI=your-provider-discovery-url
    AUTH_OIDC_BASE_URL=your-datahub-url

*   `AUTH_OIDC_ENABLED`: Habilitar la delegación de autenticación al proveedor de identidades OIDC
*   `AUTH_OIDC_CLIENT_ID`: Identificador de cliente único recibido del proveedor de identidades
*   `AUTH_OIDC_CLIENT_SECRET`: Secreto de cliente único recibido del proveedor de identidades
*   `AUTH_OIDC_DISCOVERY_URI`: Ubicación de la API de detección OIDC del proveedor de identidades. Sufijo con `.well-known/openid-configuration`
*   `AUTH_OIDC_BASE_URL`: La URL base de su implementación de DataHub, por ejemplo, https://yourorgdatahub.com (prod) o http://localhost:9002 (pruebas)

Proporcionar estas configuraciones hará que DataHub delegue la autenticación en su identidad
proveedor, solicitando los ámbitos "perfil de correo electrónico oidc" y analizando la notificación "preferred_username" de
el perfil autenticado como identidad dataHub CorpUser.

> De forma predeterminada, el extremo de devolución de llamada de inicio de sesión expuesto por DataHub se ubicará en `${AUTH_OIDC_BASE_URL}/callback/oidc`. Esto debe **exactamente** coincide con la URL de redireccionamiento de inicio de sesión que has registrado con tu proveedor de identidades en el paso 1.

En kubernetes, puede agregar las variables env anteriores en values.yaml de la siguiente manera.

```yaml
datahub-frontend:
  ...
  extraEnvs:
    - name: AUTH_OIDC_ENABLED
      value: "true"
    - name: AUTH_OIDC_CLIENT_ID
      value: your-client-id
    - name: AUTH_OIDC_CLIENT_SECRET
      value: your-client-secret
    - name: AUTH_OIDC_DISCOVERY_URI
      value: your-provider-discovery-url
    - name: AUTH_OIDC_BASE_URL
      value: your-datahub-url
```

También puede empaquetar secretos de cliente OIDC en un secreto k8s ejecutando

`kubectl create secret generic datahub-oidc-secret --from-literal=secret=<<OIDC SECRET>>`

A continuación, establezca el secreto env de la siguiente manera.

```yaml
    - name: AUTH_OIDC_CLIENT_SECRET
      valueFrom:
        secretKeyRef:
          name: datahub-oidc-secret
          key: secret
```

#### Avanzado

Opcionalmente, puede personalizar aún más el flujo utilizando configuraciones avanzadas. Estos permiten
debe especificar los ámbitos OIDC solicitados, cómo se analiza el nombre de usuario de DataHub a partir de las notificaciones devueltas por el proveedor de identidades y cómo se extraen y aprovisionan los usuarios y grupos del conjunto de notificaciones OIDC.

    # Optional Configuration Values:
    AUTH_OIDC_USER_NAME_CLAIM=your-custom-claim
    AUTH_OIDC_USER_NAME_CLAIM_REGEX=your-custom-regex
    AUTH_OIDC_SCOPE=your-custom-scope
    AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD=authentication-method

*   `AUTH_OIDC_USER_NAME_CLAIM`: atributo que contendrá el nombre de usuario utilizado en la plataforma DataHub. De forma predeterminada, se proporciona "preferred_username"
    como parte de la norma `profile` alcance.
*   `AUTH_OIDC_USER_NAME_CLAIM_REGEX`: cadena regex utilizada para extraer el nombre de usuario del atributo userNameClaim. Por ejemplo, si
    el campo userNameClaim contendrá una dirección de correo electrónico, y queremos omitir el sufijo de nombre de dominio del correo electrónico, podemos especificar una custom
    regex para hacerlo. (por ejemplo, `([^@]+)`)
*   `AUTH_OIDC_SCOPE`: una cadena que representa los ámbitos que se van a solicitar al proveedor de identidades, concedida por el usuario final. Para más información,
    ver [Ámbitos de conexión de OpenID](https://auth0.com/docs/scopes/openid-connect-scopes).
*   `AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD`: una cadena que representa el método de autenticación de token que se va a usar con el proveedor de identidades. Valor predeterminado
    es `client_secret_basic`, que utiliza la autenticación HTTP Basic. Otra opción es `client_secret_post`, que incluye el client_id y el secret_id
    como parámetros de formulario en la solicitud HTTP POST. Para obtener más información, consulte [Autenticación de cliente OAuth 2.0](https://darutk.medium.com/oauth-2-0-client-authentication-4b5f929305d4)

##### Aprovisionamiento de usuarios y grupos (aprovisionamiento JIT)

De forma predeterminada, DataHub intentará con optimismo aprovisionar usuarios y grupos que aún no existen en el momento del inicio de sesión.
Para los usuarios, extraemos información como nombre, apellido, nombre para mostrar y correo electrónico para construir un perfil de usuario básico. Si hay una notificación de grupo presente,
simplemente extraemos sus nombres.

El comportamiento de aprovisionamiento predeterminado se puede personalizar mediante las siguientes configuraciones.

    # User and groups provisioning
    AUTH_OIDC_JIT_PROVISIONING_ENABLED=true
    AUTH_OIDC_PRE_PROVISIONING_REQUIRED=false
    AUTH_OIDC_EXTRACT_GROUPS_ENABLED=false
    AUTH_OIDC_GROUPS_CLAIM=<your-groups-claim-name>

*   `AUTH_OIDC_JIT_PROVISIONING_ENABLED`: Si los usuarios y grupos de DataHub deben aprovisionarse al iniciar sesión si no existen. El valor predeterminado es true.
*   `AUTH_OIDC_PRE_PROVISIONING_REQUIRED`: Si el usuario ya debería existir en DataHub cuando inicia sesión, fallando el inicio de sesión si no lo está. Esto es apropiado para situaciones en las que los usuarios y grupos se ingieren por lotes y se controlan estrictamente dentro de su entorno. El valor predeterminado es false.
*   `AUTH_OIDC_EXTRACT_GROUPS_ENABLED`: Sólo se aplica si `AUTH_OIDC_JIT_PROVISIONING_ENABLED` se establece en true. Esto determina si debemos intentar extraer una lista de nombres de grupo de una notificación concreta en los atributos OIDC. Tenga en cuenta que si esto está habilitado, cada inicio de sesión volverá a sincronizar la pertenencia a grupos con los grupos de su proveedor de identidades, borrando la pertenencia a grupos que se ha asignado a través de la interfaz de usuario de DataHub. ¡Habilite con cuidado! El valor predeterminado es false.
*   `AUTH_OIDC_GROUPS_CLAIM`: Sólo se aplica si `AUTH_OIDC_EXTRACT_GROUPS_ENABLED` se establece en true. Esto determina qué notificaciones OIDC contendrán una lista de nombres de grupos de cadenas. Acepta varios nombres de notificación con valores separados por comas. Es decir: `groups, teams, departments`. El valor predeterminado es 'grupos'.

Una vez actualizada la configuración, `datahub-frontend-react` deberá reiniciarse para recoger las nuevas variables de entorno:

    docker-compose -p datahub -f docker-compose.yml -f docker-compose.override.yml  up datahub-frontend-react

> Tenga en cuenta que, de forma predeterminada, habilitar OIDC *no* Deshabilite la ruta de autenticación JAAS ficticia, a la que se puede acceder en el `/login`
> ruta de la aplicación React. Para deshabilitar esta ruta de autenticación, especifique adicionalmente la siguiente configuración:
> `AUTH_JAAS_ENABLED=false`

### Resumen

Una vez configurado, implementando el `datahub-frontend-react` Container habilitará un flujo de autenticación indirecta en el que DataHub delega
autenticación al proveedor de identidad especificado.

Una vez que un usuario es autenticado por el proveedor de identidad, DataHub extraerá un nombre de usuario de las notificaciones proporcionadas.
y conceder acceso a DataHub al usuario mediante la configuración de un par de cookies de sesión.

Un breve resumen de los pasos que se producen cuando el usuario navega a la aplicación React son los siguientes:

1.  Un `GET` al `/authenticate` punto final en `datahub-frontend` se inicia el servidor
2.  El `/authenticate` intenta autenticar la solicitud a través de cookies de sesión
3.  Si se produce un error en la autenticación, el servidor emite una redirección a la experiencia de inicio de sesión del proveedor de identidades
4.  El usuario inicia sesión con el proveedor de identidades
5.  El proveedor de identidades autentica al usuario y redirige de nuevo a la URL de redirección de inicio de sesión registrada de DataHub, proporcionando un código de autorización que
    Se puede utilizar para recuperar información en nombre del usuario autenticado
6.  DataHub obtiene el perfil del usuario autenticado y extrae un nombre de usuario para identificar al usuario en DataHub (por ejemplo, urn:li:corpuser:username)
7.  DataHub establece cookies de sesión para el usuario recién autenticado
8.  DataHub redirige al usuario a la página de inicio ("/")

### Usuario raíz

Incluso si OIDC está configurado, el usuario root aún puede iniciar sesión sin OIDC yendo
Para `/login` Extremo de URL. Se recomienda que no utilice el valor predeterminado
mediante el montaje de un archivo diferente en el contenedor front-end. Para ello
por favor vea [jaas](https://datahubproject.io/docs/how/auth/jaas/#mount-a-custom-userprops-file-docker-compose) -
"Montar un archivo user.props personalizado".
