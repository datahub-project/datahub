# Agregar usuarios a DataHub

Los usuarios pueden iniciar sesión en DataHub de 3 maneras:

1.  Invitar a usuarios a través de la interfaz de usuario
2.  Credenciales estáticas
3.  Inicio de sesión único a través de [Conexión OpenID](https://www.google.com/search?q=openid+connect\&oq=openid+connect\&aqs=chrome.0.0i131i433i512j0i512l4j69i60l2j69i61.1468j0j7\&sourceid=chrome\&ie=UTF-8) (Para uso de producción)

que se puede habilitar simultáneamente. Las opciones 1 y 2 son útiles para ejecutar ejercicios de prueba de concepto, o simplemente para poner en marcha DataHub rápidamente. La opción 3 es muy recomendable para implementar DataHub en producción.

# Método 1: Invitar a usuarios a través de la interfaz de usuario de DataHub

## Enviar a los usuarios potenciales un enlace de invitación

Con los permisos adecuados (`MANAGE_USER_CREDENTIALS`), puede invitar a nuevos usuarios a la instancia de DataHub implementada desde la interfaz de usuario. ¡Es tan simple como enviar un enlace!

Primero navegue hasta la pestaña Usuarios y grupos (en Acceso) en la página Configuración. A continuación, verá un `Invite Users` botón. Tenga en cuenta que solo se podrá hacer clic en esto
si tiene los permisos correctos.

![](../../imgs/invite-users-button.png)

Si hace clic en este botón, verá una ventana emergente donde puede copiar un enlace de invitación para enviar a los usuarios o generar uno nuevo.

![](../../imgs/invite-users-popup.png)

Cuando un nuevo usuario visita el enlace, se le dirigirá a una pantalla de registro. Tenga en cuenta que si un nuevo enlace se ha regenerado desde entonces, ¡el nuevo usuario no podrá registrarse!

![](../../imgs/user-sign-up-screen.png)

## Restablecer contraseña para usuarios nativos

Si un usuario olvida su contraseña, un usuario administrador con el `MANAGE_USER_CREDENTIALS` puede ir a la pestaña Usuarios y grupos y hacer clic en el usuario respectivo
`Reset user password` botón.

![](../../imgs/reset-user-password-button.png)

Al igual que el enlace de invitación, puede generar un nuevo enlace de restablecimiento y enviar un enlace a ese usuario que puede usar para restablecer sus credenciales.

![](../../imgs/reset-user-password-popup.png)

Cuando ese usuario visite el enlace, será directo a una pantalla donde podrá restablecer sus credenciales. Si el enlace tiene más de 24 horas de antigüedad u otro enlace tiene desde entonces
¡Generados, no podrán restablecer sus credenciales!

![](../../imgs/reset-credentials-screen.png)

# Método 2: Configuración de credenciales estáticas

## Crear un archivo user.props

Para definir un conjunto de combinaciones de nombre de usuario y contraseña a las que se debe permitir iniciar sesión en DataHub, cree un nuevo archivo llamado `user.props` en la ruta de acceso del archivo `${HOME}/.datahub/plugins/frontend/auth/user.props`.
Este archivo debe contener combinaciones de nombre de usuario:contraseña, con 1 usuario por línea. Por ejemplo, para crear 2 nuevos usuarios,
con los nombres de usuario "janesmith" y "johndoe", definiríamos el siguiente archivo:

    janesmith:janespassword
    johndoe:johnspassword

Una vez que haya guardado el archivo, simplemente inicie los contenedores de DataHub y navegue hasta `http://localhost:9002/login`
para comprobar que sus nuevas credenciales funcionan.

Para cambiar o eliminar las credenciales de inicio de sesión existentes, edite y guarde el `user.props` archivo. A continuación, reinicie los contenedores de DataHub.

Si desea personalizar la ubicación de la `user.props` o si está implementando DataHub a través de Helm, continúe con el paso 2.

## (Avanzado) Montar archivo user.props personalizado en el contenedor

Este paso solo es necesario cuando se montan credenciales personalizadas en un pod de Kubernetes (por ejemplo, Helm) **o** si desea cambiar
La ubicación predeterminada del sistema de archivos desde la que DataHub monta una configuración personalizada `user.props` archivo (`${HOME}/.datahub/plugins/frontend/auth/user.props)`.

Si está implementando con `datahub docker quickstart`, o ejecutándose con Docker Compose, lo más probable es que pueda omitir este paso.

### Docker Componer

Tendrás que modificar el `docker-compose.yml` para montar un volumen de contenedor que asigna el user.props personalizado a la ubicación estándar dentro del contenedor
(`/etc/datahub/plugins/frontend/auth/user.props`).

Por ejemplo, para montar un archivo user.props que está almacenado en mi sistema de archivos local en `/tmp/datahub/user.props`, modificaríamos el YAML para el
`datahub-web-react` config para que tenga el siguiente aspecto:

```aidl
  datahub-frontend-react:
    build:
      context: ../
      dockerfile: docker/datahub-frontend/Dockerfile
    image: linkedin/datahub-frontend-react:${DATAHUB_VERSION:-head}
    .....
    # The new stuff
    volumes:
      - ${HOME}/.datahub/plugins:/etc/datahub/plugins
      - /tmp/datahub:/etc/datahub/plugins/frontend/auth
```

Una vez que haya realizado este cambio, al reiniciar DataHub, habilite la autenticación para los usuarios configurados.

### Timón

Deberá crear un secreto de Kubernetes y, a continuación, montar el archivo como un volumen en el `datahub-frontend` vaina.

Primero, crea un secreto a partir de tu local `user.props` archivo

```shell
kubectl create secret generic datahub-users-secret --from-file=user.props=./<path-to-your-user.props>
```

A continuación, configure su `values.yaml` Para agregar el volumen al cuadro de diálogo `datahub-frontend` contenedor.

```YAML
datahub-frontend:
  ...
  extraVolumes:
    - name: datahub-users
      secret:
        defaultMode: 0444
        secretName:  datahub-users-secret
  extraVolumeMounts:
    - name: datahub-users
      mountPath: /etc/datahub/plugins/frontend/auth/user.props
      subPath: user.props
```

Tenga en cuenta que si actualiza el secreto, deberá reiniciar el `datahub-frontend` vainas para que se reflejen los cambios. Para actualizar el secreto en el lugar, puede ejecutar algo como esto.

```shell
kubectl create secret generic datahub-users-secret --from-file=user.props=./<path-to-your-user.props> -o yaml --dry-run=client | kubectl apply -f -
```

## Urnas

Las URN son identificadores que identifican de forma única a una entidad en DataHub. Los nombres de usuario definidos en el `user.props` Se utilizará para generar la "urna" de usuario de DataHub, que identifica de forma única
el usuario en DataHub. La urna se calcula como:

    urn:li:corpuser:{username}

## Advertencias

### Agregar detalles de usuario

Si agrega un nuevo nombre de usuario / contraseña al `user.props` , no existirá ninguna otra información sobre el usuario
sobre el usuario en DataHub (nombre completo, correo electrónico, biografía, etc.). Esto significa que no podrá buscar para encontrar al usuario.

Para que el usuario pueda realizar búsquedas, simplemente navegue a la página de perfil del nuevo usuario (esquina superior derecha) y haga clic en
**Editar perfil**. Agregue algunos detalles como un nombre para mostrar, un correo electrónico y más. A continuación, haga clic en **Salvar**. Ahora deberías ser capaz
para encontrar al usuario a través de la búsqueda.

> También puede usar nuestro SDK de emisor de Python para producir información personalizada sobre el nuevo usuario a través de la entidad de metadatos CorpUser.

Para obtener una visión general más completa de cómo se administran los usuarios y grupos dentro de DataHub, consulte [este video](https://www.youtube.com/watch?v=8Osw6p9vDYY).

### Cambiar el usuario predeterminado de 'datahub'

El usuario administrador 'datahub' se crea para usted de forma predeterminada. No hay forma de invalidar la contraseña predeterminada para esta cuenta a continuación
los pasos descritos anteriormente para agregar un archivo user.props personalizado. Esto se debe a la forma en que funciona la configuración de autenticación: admitimos un user.props "predeterminado"
que contiene el usuario raíz del centro de datos y un archivo personalizado independiente, que no sobrescribe el primero.

Sin embargo, todavía es posible cambiar la contraseña por el valor predeterminado `datahub user`. Para cambiarlo, siga estos pasos:

1.  Actualizar el `docker-compose.yaml` Para montar el archivo User.props predeterminado en la siguiente ubicación dentro del cuadro de diálogo `datahub-frontend-react` contenedor que utiliza un volumen:
    `/datahub-frontend/conf/user.props`

2.  Reinicie los contenedores de datahub para recoger las nuevas configuraciones

Si va a implementar mediante el inicio rápido de la CLI, simplemente puede descargar una copia del [archivo docker-compose utilizado en inicio rápido](https://github.com/datahub-project/datahub/blob/master/docker/quickstart/docker-compose.quickstart.yml),
y modificar el `datahub-frontend-react` para contener el montaje de volumen adicional. Luego simplemente ejecute

    datahub docker quickstart —quickstart-compose-file <your-modified-compose>.yml

# Método 3: Configuración de SSO a través de OpenID Connect

La configuración de SSO a través de OpenID Connect significa que los usuarios podrán iniciar sesión en DataHub a través de un proveedor de identidad central, como

*   Azure AD
*   Okta
*   Keycloak
*   ¡Señal!
*   Identidad de Google

y más.

Esta opción se recomienda para implementaciones de producción de DataHub. Para obtener información detallada acerca de cómo configurar DataHub para usar OIDC para
realizar la autenticación, retirar [Autenticación OIDC](./sso/configure-oidc-react.md).

## Urnas

Las URN son identificadores que identifican de forma única a una entidad en DataHub. El nombre de usuario recibido de un proveedor de identidades
cuando un usuario inicia sesión en DataHub a través de OIDC se utiliza para construir un identificador único para el usuario en DataHub. La urna se calcula como:

    urn:li:corpuser:<extracted-username>

Para obtener información sobre cómo configurar qué notificación OIDC se debe usar como nombre de usuario para Datahub, consulte el [Autenticación OIDC](./sso/configure-oidc-react.md) Doc.

## PREGUNTAS MÁS FRECUENTES

1.  ¿Puedo habilitar la autenticación OIDC y de nombre de usuario / contraseña (JaaS) al mismo tiempo?

¡SÍ! Si no ha deshabilitado explícitamente JaaS a través de una variable de entorno en el contenedor datahub-frontend (AUTH_JAAS_ENABLED),
entonces puedes *siempre* Acceda al flujo de inicio de sesión estándar en `http://your-datahub-url.com/login`.

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, ¡comunícate con Slack!
