# Guía de políticas

## Introducción

DataHub proporciona la capacidad de declarar políticas de control de acceso detalladas a través de la interfaz de usuario y la API graphQL.
Las directivas de acceso en DataHub definen *Quién* enlatar *Hacer qué* Para *qué recursos*. Algunas políticas en inglés sencillo incluyen

*   Los propietarios de conjuntos de datos deben poder editar la documentación, pero no las etiquetas.
*   Jenny, nuestra administradora de datos, debería poder editar etiquetas para cualquier panel, pero no otros metadatos.
*   A James, un analista de datos, se le debe permitir editar los enlaces para una canalización de datos específica de la que es un consumidor posterior.
*   El equipo de la plataforma de datos debe poder administrar usuarios y grupos, ver análisis de la plataforma y administrar políticas por sí mismos.

En este documento, analizaremos más a fondo las políticas de DataHub y cómo usarlas de manera efectiva.

## ¿Qué es una política?

Hay 2 tipos de políticas dentro de DataHub:

1.  Políticas de la plataforma
2.  Directivas de metadatos

Describiremos brevemente cada uno.

### Políticas de la plataforma

**Plataforma** las directivas determinan quién tiene privilegios de nivel de plataforma en DataHub. Estos privilegios incluyen

*   Gestión de usuarios y grupos
*   Visualización de la página de análisis de DataHub
*   Gestión de las propias políticas

Las políticas de la plataforma se pueden dividir en 2 partes:

1.  **Actores**: A quién se aplica la política (Usuarios o Grupos)
2.  **Privilegios**: Qué privilegios deben asignarse a los Actores (por ejemplo, "Ver análisis")

Tenga en cuenta que las directivas de plataforma no incluyen un "recurso de destino" específico contra el que se aplican las directivas. En lugar de
simplemente sirven para asignar privilegios específicos a los usuarios y grupos de DataHub.

### Directivas de metadatos

**Metadatos** las directivas determinan quién puede hacer qué a qué entidades de metadatos. Por ejemplo

*   ¿Quién puede editar la documentación y los enlaces del conjunto de datos?
*   ¿Quién puede agregar propietarios a un gráfico?
*   ¿Quién puede agregar etiquetas a un panel?

y así sucesivamente.

Una política de metadatos se puede dividir en 3 partes:

1.  **Actores**: El 'quién'. Usuarios específicos, grupos a los que se aplica la directiva.
2.  **Privilegios**: El 'qué'. Qué acciones está permitida por una política, por ejemplo, "Agregar etiquetas".
3.  **Recursos**: El 'cual'. Recursos a los que se aplica la política, por ejemplo, "Todos los conjuntos de datos".

#### Actores

Actualmente admitimos 3 formas de definir el conjunto de actores a los que se aplica la política: a) lista de usuarios b) lista de grupos, y
c) propietarios de la entidad. También tiene la opción de aplicar la directiva a todos los usuarios.

#### Privilegios

Echa un vistazo a la lista de
Privilegios [aquí](https://github.com/datahub-project/datahub/blob/master/metadata-utils/src/main/java/com/linkedin/metadata/authorization/PoliciesConfig.java)
. Tenga en cuenta que los privilegios son semánticos por naturaleza y no se vinculan de 1 a 1 con el modelo de aspecto.

Todas las ediciones en la interfaz de usuario están cubiertas por un privilegio, para asegurarnos de que tenemos la capacidad de restringir el acceso de escritura.

Actualmente apoyamos lo siguiente:

**Nivel de plataforma** privilegios para que los operadores de DataHub accedan y administren la funcionalidad administrativa del sistema.

| Privilegios de plataforma | Descripción |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Administrar directivas | Permitir que el actor cree y elimine directivas de control de acceso. Tenga cuidado: los actores con este privilegio son efectivamente súper usuarios. |
| Administrar | de ingesta de metadatos Permitir que el actor cree, elimine y actualice orígenes de ingesta de metadatos.                                                          |
| Administrar secretos | Permite al actor crear y eliminar secretos almacenados dentro de DataHub.                                                                  |
| Administrar usuarios y grupos | Permitir que el actor cree, elimine y actualice usuarios y grupos en DataHub.                                                          |
| Administrar todos los tokens de acceso | Permita que el actor cree, elimine y enumere tokens de acceso para todos los usuarios en DataHub.                                                |
| Administrar dominios | Permitir que el actor cree y elimine dominios de activos.                                                                                |
| Ver | de Analytics Permita que el actor acceda al panel de análisis de DataHub.                                                                      |
| Generar tokens de acceso personal | Permita que el actor genere tokens de acceso para uso personal con las API de DataHub.                                                  |
| Administrar credenciales de usuario | Permita que el actor genere enlaces de invitación para nuevos usuarios nativos de DataHub y enlaces de restablecimiento de contraseña para usuarios nativos existentes.   |

**Privilegios de metadatos comunes** para ver y modificar cualquier entidad dentro de DataHub.

| Privilegios comunes | Descripción |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------|
| Ver | de página de entidad Permita que el actor acceda a la página de entidad para el recurso en la interfaz de usuario. Si no se concede, los redirigirá a una página no autorizada. |
| Editar etiquetas | Permitir que el actor agregue y elimine etiquetas a un activo.                                                                                  |
| Editar términos del glosario | Permitir que el actor agregue y elimine términos del glosario a un activo.                                                                        |
| Editar | propietarios Permitir que el actor agregue y elimine propietarios de una entidad.                                                                               |
| Editar descripción | Permitir que el actor edite la descripción (documentación) de una entidad.                                                                |
| Editar enlaces | Permitir que el actor edite los vínculos asociados a una entidad.                                                                             |
| Editar | de estado Permitir que el actor edite el estado de una entidad (eliminado suavemente o no).                                                               |
| Editar | de dominio Permitir que el actor edite el dominio de una entidad.                                                                                     |
| Editar | en desuso Permitir que el actor edite el estado de obsolescencia de una entidad.                                                                         |
| Editar aserciones | Permitir que el actor agregue y elimine aserciones de una entidad.                                                                         |
| Editar todos los | Permitir que el actor edite cualquier información sobre una entidad. Privilegios de superusuario.                                                      |

**Privilegios específicos a nivel de entidad** que no son generalizables.

| | de entidad | de privilegios Descripción |
|--------------|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| | del conjunto de datos Editar etiquetas de columna del conjunto de datos | Permitir que el actor edite las etiquetas de columna (campo) asociadas a un esquema de conjunto de datos.                                                                                              |
| | del conjunto de datos Editar términos del glosario de columnas del conjunto de datos | Permitir que el actor edite los términos del glosario de columna (campo) asociados a un esquema de conjunto de datos.                                                                                    |
| | del conjunto de datos Editar descripciones de columnas del conjunto de datos | Permitir que el actor edite las descripciones de columna (campo) asociadas a un esquema de conjunto de datos.                                                                                      |
| | del conjunto de datos Ver | de uso del conjunto de datos Permitir que el actor acceda a los metadatos de uso sobre un conjunto de datos tanto en la interfaz de usuario como en la API de GraphQL. Esto incluye consultas de ejemplo, número de consultas, etc.                         |
| | del conjunto de datos Ver | de perfil de conjunto de datos Permitir que el actor acceda al perfil de un conjunto de datos tanto en la interfaz de usuario como en la API de GraphQL. Esto incluye estadísticas de instantáneas como #rows, #columns, porcentaje nulo por campo, etc. |
| | de etiquetas Editar | de color de etiqueta Permitir que el actor cambie el color de una etiqueta.                                                                                                                                  |
| Grupo | Editar | de miembros del grupo Permitir que el actor agregue y elimine miembros a un grupo.                                                                                                                          |
| | de usuario Editar | de perfil de usuario Permitir que el actor cambie el perfil del usuario, incluido el nombre para mostrar, la biografía, el título, la imagen de perfil, etc.                                                                           |
| Usuario + grupo | Editar información de contacto | Permita que el actor cambie la información de contacto, como el correo electrónico y los identificadores de chat.                                                                                                |

#### Recursos

El filtro de recursos define el conjunto de recursos al que se aplica la directiva y se define mediante una lista de criterios. Cada
El criterio define un tipo de campo (como resource_type, resource_urn, dominio), una lista de valores de campo para comparar y un
condición (como IGUAL). Esencialmente verifica si el campo de un determinado recurso coincide con alguno de los valores de entrada.
Tenga en cuenta que si no hay criterios o no se establece el recurso, la directiva se aplica a TODOS los recursos.

Por ejemplo, el siguiente filtro de recursos aplicará la directiva a conjuntos de datos, gráficos y paneles del dominio 1.

```json
{
  "resource": {
    "criteria": [
      {
        "field": "resource_type",
        "values": [
          "dataset",
          "chart",
          "dashboard"
        ],
        "condition": "EQUALS"
      },
      {
        "field": "domain",
        "values": [
          "urn:li:domain:domain1"
        ],
        "condition": "EQUALS"
      }
    ]
  }
}
```

Los campos admitidos son los siguientes

| Tipo de campo | Descripción | Ejemplo |
|---------------|------------------------|-------------------------|
| resource_type | Tipo de | de recursos dataset, gráfico, dataJob |
| resource_urn | Urna del recurso | urn:li:dataset:... |
| | de dominio Dominio del | de recursos urn:li:domain:domainX |

## Administración de directivas

Las políticas se pueden administrar bajo el `/policies` o se accede a través de la barra de navegación superior. El `Policies` pestaña sólo
ser visible para aquellos usuarios que tengan el `Manage Policies` privilegio.

Listo para usar, DataHub se implementa con un conjunto de políticas pre-horneadas. El conjunto de directivas predeterminadas se crea en la implementación
tiempo y se puede encontrar dentro de la `policies.json` archivo dentro `metadata-service/war/src/main/resources/boot`. Este conjunto de políticas sirve a la
siguientes propósitos:

1.  Asigna privilegios de superusuario inmutables para la raíz `datahub` cuenta de usuario (inmutable)
2.  Asigna todos los privilegios de la plataforma para todos los usuarios de forma predeterminada (editable)

La razón de # 1 es evitar que las personas eliminen accidentalmente todas las políticas y se bloqueen (`datahub` La cuenta de superusuario puede ser una copia de seguridad)
La razón de # 2 es permitir que los administradores inicien sesión a través de OIDC u otro medio fuera del `datahub` cuenta raíz
cuando están arrancando con DataHub. De esta manera, aquellos que configuran DataHub pueden comenzar a administrar políticas sin fricción.
Tenga en cuenta que estos privilegios *enlatar* y probablemente *deber* ser alterado dentro de la **Políticas** de la interfaz de usuario.

> Pro-Tip: Para iniciar sesión utilizando el botón `datahub` cuenta, simplemente navegue a `<your-datahub-domain>/login` e introduzca `datahub`, `datahub`. Tenga en cuenta que la contraseña se puede personalizar para su
> implementación cambiando el `user.props` dentro del archivo `datahub-frontend` módulo. Observe que la autenticación JaaS debe estar habilitada.

## Configuración

De forma predeterminada, la característica Directivas es *Habilitado*. Esto significa que la implementación admitirá la creación, edición, eliminación y
lo más importante es hacer cumplir políticas de acceso detalladas.

En algunos casos, estas capacidades no son deseables. Por ejemplo, si los usuarios de tu empresa ya están acostumbrados a tener rienda suelta,
puede que quiera mantenerlo así. O tal vez solo su equipo de la plataforma de datos utiliza activamente DataHub, en cuyo caso las políticas pueden ser excesivas.

Para estos escenarios, hemos proporcionado una puerta trasera para deshabilitar las directivas en su implementación de DataHub. Esto ocultará por completo
la interfaz de usuario de administración de directivas y de forma predeterminada permitirá todas las acciones en la plataforma. Será como si
cada usuario tiene *todo* privilegios, ambos **Plataforma** & **Metadatos** sabor.

Para deshabilitar las directivas, simplemente puede establecer el `AUTH_POLICIES_ENABLED` variable de entorno para el `datahub-gms` contenedor de servicios
Para `false`. Por ejemplo en su `docker/datahub-gms/docker.env`, colocarías

    AUTH_POLICIES_ENABLED=false

## Próximamente

El equipo de DataHub está trabajando duro para tratar de mejorar la función Políticas. Estamos planeando construir lo siguiente:

*   Ocultar botones de acción de edición en las páginas de entidad para reflejar los privilegios de usuario

Considerado

*   Capacidad para definir políticas de metadatos contra múltiples reosurces con ámbito de "contenedores" particulares (por ejemplo, un "esquema", "base de datos" o "colección")

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, ¡comunícate con Slack!
