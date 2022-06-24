# Relaciones de alta cardinalidad

Como se explica en [Qué es una relación](../what/relationship.md), los metadatos sin procesar para formar relaciones se capturan directamente dentro de un [Aspecto de metadatos](../what/aspect.md). La forma más natural de modelar esto es usando una matriz, por ejemplo, un aspecto de pertenencia a un grupo contiene una matriz de usuario [Urnas](../what/urn.md). Sin embargo, esto plantea algunos desafíos cuando se espera que la cardinalidad de la relación sea grande (digamos, mayor que 10,000). El aspecto se vuelve grande en tamaño, lo que conduce a una actualización y recuperación lentas. Incluso puede exceder el límite subyacente del almacén de documentos, que a menudo está en el rango de unos pocos MB. Además, el envío de mensajes grandes (> 1 MB) a través de Kafka requiere una afinación especial y generalmente se desaconseja.

Dependiendo del tipo de relaciones, existen diferentes estrategias para lidiar con la alta cardinalidad.

### 1:N Relaciones

Cuando `N` es grande, simplemente almacene la relación como un puntero inverso en el `N` lado, en lugar de un `N`-matriz de elementos en el `1` lado. En otras palabras, en lugar de hacer esto.

    record MemberList {
      members: array[UserUrn]
    }

Haga esto

    record Membership {
      group: GroupUrn
    }

Un inconveniente con este enfoque es que la actualización por lotes de la lista de miembros se convierte en múltiples operaciones de base de datos y no atómicas. Si la lista es proporcionada por un proveedor de metadatos externo a través de [MCEs](../what/mxe.md), esto también significa que se requerirán varios MCE para actualizar la lista, en lugar de tener una matriz gigante en un solo MCE.

### Relaciones M:N

Cuando un lado de la relación (`M` o `N`) tiene cardinalidad baja, puede aplicar el mismo truco en \[Relación 1:N] creando la matriz en el lado con cardinalidad baja. Por ejemplo, suponiendo que un usuario solo puede ser parte de un pequeño número de grupos, pero cada grupo puede tener un gran número de usuarios, el siguiente modelo será más eficiente que el inverso.

    record Membership {
      groups: array[GroupUrn]
    }

Cuando ambos `M` y `N` son de alta cardinalidad (por ejemplo, millones de usuarios, cada uno pertenece a millones de grupos), la única forma de almacenar tales relaciones de manera eficiente es creando una nueva "Entidad de mapeo" con un solo aspecto como este

    record UserGroupMap {
      user: UserUrn
      group: GroupUrn
    }

Esto significa que la relación ahora solo se puede crear y actualizar con una única granularidad de par origen-destino.
