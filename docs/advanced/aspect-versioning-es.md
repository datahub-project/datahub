# Control de versiones de aspectos

Como cada versión de [aspecto de metadatos](../what/aspect.md) es inmutable, cualquier actualización a un aspecto existente da como resultado la creación de una nueva versión. Por lo general, uno esperaría que el número de versión aumente secuencialmente y el mayor número de versión sea la última versión, es decir, `v1` (más antiguo), `v2` (segundo más antiguo), ..., `vN` (más reciente). Sin embargo, este enfoque resulta en grandes desafíos tanto en el modelado rest.li como en el aislamiento de transacciones y, por lo tanto, requiere un replanteamiento.

## modelado Rest.li

Como es común crear rest.li subrecursos dedicados para un aspecto específico, por ejemplo. `/datasets/{datasetKey}/ownership`, el concepto de versiones se convierte en una interesante cuestión de modelado. ¿Debe el sub-recurso ser un [Sencillo](https://linkedin.github.io/rest.li/modeling/modeling#simple) o un [Colección](https://linkedin.github.io/rest.li/modeling/modeling#collection) ¿tipo?

Si es simple, el [OBTENER](https://linkedin.github.io/rest.li/user_guide/restli_server#get) Se espera que el método devuelva la versión más reciente y la única forma de recuperar versiones que no sean las más recientes es a través de una versión personalizada [ACCIÓN](https://linkedin.github.io/rest.li/user_guide/restli_server#action) método, que va en contra de la [REPOSO](https://en.wikipedia.org/wiki/Representational_state_transfer) principio. Como resultado, un sub-recurso Simple no parece ser una buena opción.

Si Colección, el número de versión se convierte naturalmente en la clave, por lo que es fácil recuperar un número de versión específico utilizando el método GET típico. También es fácil enumerar todas las versiones utilizando el estándar [GET_ALL](https://linkedin.github.io/rest.li/user_guide/restli_server#get_all) método u obtener un conjunto de versiones a través de [BATCH_GET](https://linkedin.github.io/rest.li/user_guide/restli_server#batch_get). Sin embargo, los recursos de recopilación no admiten una forma sencilla de obtener la clave más reciente/más grande directamente. Para lograr eso, uno debe hacer uno de los siguientes

*   un GET_ALL (suponiendo un orden de clave descendente) con un tamaño de página de 1
*   un [BUSCADOR](https://linkedin.github.io/rest.li/user_guide/restli_server#finder) con parámetros especiales y un tamaño de página de 1
*   Un método ACTION personalizado de nuevo

Ninguna de estas opciones parece una forma natural de pedir la última versión de un aspecto, que es uno de los casos de uso más comunes.

## Aislamiento de transacciones

[Aislamiento de transacciones](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)) es un tema complejo, así que asegúrese de familiarizarse primero con los conceptos básicos.

Para admitir la actualización simultánea de un aspecto de metadatos, las siguientes operaciones de pseudo base de datos deben ejecutarse en una sola transacción,

    1. Retrieve the current max version (Vmax)
    2. Write the new value as (Vmax + 1)

La operación 1 anterior puede sufrir fácilmente de [Lecturas fantasma](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Phantom_reads). Esto posteriormente lleva a la Operación 2 a calcular la versión incorrecta y, por lo tanto, sobrescribe una versión existente en lugar de crear una nueva.

Una forma de resolver esto es haciendo cumplir [Serializable](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Serializable) nivel de aislamiento en la base de datos en el [costo de rendimiento](https://logicalread.com/optimize-mysql-perf-part-2-mc13/#.XjxSRSlKh1N). En realidad, muy pocas bases de datos admiten este nivel de aislamiento, especialmente para almacenes de documentos distribuidos. Es más común apoyar [Lecturas repetibles](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Repeatable_reads) o [Leer Comprometido](https://en.wikipedia.org/wiki/Isolation_\(database_systems\)#Read_committed) niveles de aislamiento, lamentablemente ninguno de los dos ayudaría en este caso.

Otra posible solución es realizar un seguimiento transaccional de `Vmax` directamente en una tabla separada para evitar la necesidad de calcular eso a través de un `select` (evita así las lecturas fantasmas). Sin embargo, la transacción entre tablas/documentos/entidades no es una característica compatible con todos los almacenes de documentos distribuidos, lo que impide esto como una solución generalizada.

## Solución: Versión 0

La solución a ambos desafíos resulta ser sorprendentemente simple. En lugar de usar un número de versión "flotante" para representar la última versión, se puede usar un número de versión "fijo / centinela" en su lugar. En este caso, elegimos la versión 0, ya que queremos que todas las versiones que no sean las más recientes sigan aumentando secuencialmente. En otras palabras, sería `v0` (más reciente), `v1` (más antiguo), `v2` (segundo más antiguo), etc. Alternativamente, también puede simplemente ver todas las versiones distintas de cero como una pista de auditoría.

Examinemos cómo la versión 0 puede resolver los desafíos antes mencionados.

### modelado Rest.li

Con la versión 0, obtener la última versión se convierte en llamar al método GET de un subrecurso específico del aspecto de la colección con una clave determinista, por ejemplo. `/datasets/{datasetkey}/ownership/0`, que es mucho más natural que usar GET_ALL o FINDER.

### Aislamiento de transacciones

Las operaciones pseudo DB cambian al siguiente bloque de transacciones con la versión 0,

    1. Retrieve v0 of the aspect
    2. Retrieve the current max version (Vmax)
    3. Write the old value back as (Vmax + 1)
    4. Write the new value back as v0

Si bien la Operación 2 todavía sufre de posibles lecturas fantasma y, por lo tanto, corrompe la versión existente en la Operación 3, el nivel de aislamiento de lecturas repetibles asegurará que la transacción falle debido a [Actualización perdida](https://codingsight.com/the-lost-update-problem-in-concurrent-transactions/) detectado en la Operación 4. Tenga en cuenta que esto también es el [nivel de aislamiento predeterminado](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html) para InnoDB en MySQL.
