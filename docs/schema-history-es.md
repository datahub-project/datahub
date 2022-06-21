# Guía del historial de esquemas

## Introducción

A medida que sus datos evolucionan con el tiempo, con campos que se agregan o eliminan, con cambios de tipo, con su documentación
y las clasificaciones de términos del glosario cambian, es posible que se encuentre haciendo la pregunta: "¿Qué hizo el esquema de este conjunto de datos?
¿Parece que hace tres meses? ¿Seis meses? ¿Qué pasa hace un año?" Nos complace anunciar que ahora puede usar DataHub para
¡responda a estas preguntas! Actualmente mostramos una vista de culpa de esquema donde puede examinar cómo se veía un esquema en un
punto específico en el tiempo, y cuáles fueron los cambios más recientes en cada uno de estos campos en ese esquema.

## Visualización del historial de esquemas de un conjunto de datos

Cuando vaya a una página de conjunto de datos, se le ofrecerá una vista que describe cuál es la versión más reciente del conjunto de datos y cuándo
el esquema se informó/modificó por última vez. Además, hay un selector de versiones que describe cuáles son las versiones anteriores de
el esquema del conjunto de datos son, y cuándo se informaron. Aquí hay un ejemplo de nuestra demostración con el
[Conjunto de datos de mascotas de copos de nieve](https://demo.datahubproject.io/dataset/urn:li:dataset:\(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pets,PROD\)/Schema?is_lineage_mode=false).

![](./imgs/schema-blame-latest-version.png)

Si hace clic en una versión anterior en el selector, viajará en el tiempo y verá cómo se veía el schena en ese entonces. Notar
los cambios aquí en los términos del glosario para el `status` y a las descripciones de la `created_at` y `updated_at`
Campos.

![](./imgs/schema-blame-older-version.png)

Además de esto, también puede activar la vista de culpa que le muestra cuándo se realizaron los cambios más recientes en cada campo.
Puede activar esto seleccionando el botón `Blame` que se ve encima de la parte superior derecha de la tabla.

![](./imgs/schema-blame-blame-activated.png)

Puede ver aquí que algunos de estos campos se agregaron en la versión más antigua del conjunto de datos, mientras que otros se agregaron solo en esta última versión
Versión. ¡Algunos campos fueron modificados en la última versión!

Lo bueno de esta vista de culpa es que puede activarla para cualquier versión y ver cuáles fueron los cambios más recientes.
en ese momento para cada campo. Similar a lo que git culpa te mostraría, ¡pero para entender mejor tus esquemas!

## Próximamente: Vista de línea de tiempo de esquema

En el futuro, planeamos agregar una vista de línea de tiempo donde pueda ver qué cambios se realizaron en varios campos de esquema a lo largo del tiempo.
de manera lineal. ¡Estén atentos a las actualizaciones!

## Comentarios / Preguntas / Inquietudes

¡Queremos saber de ti! Para cualquier consulta, incluidos comentarios, preguntas o inquietudes, ¡comunícate con Slack!
