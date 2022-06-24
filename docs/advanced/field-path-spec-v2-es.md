# Especificación de SchemaFieldPath (versión 2)

Este documento describe la especificación formal para el miembro fieldPath de
el [Campo de esquema](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/schema/SchemaField.pdl)
modelo. Esta especificación (versión 2) tiene en cuenta los requisitos únicos de admitir una amplia variedad de anidados
tipos, uniones y campos opcionales y supone una mejora sustancial respecto a la implementación actual (versión 1).

## Requisitos

El `fieldPath` Datahub utiliza actualmente el campo no solo para representar los campos de esquema en la interfaz de usuario, sino también como un
identificador primario de un campo en otros lugares, como
como [EditableSchemaFieldInfo](https://github.com/datahub-project/datahub/blob/master/metadata-models/src/main/pegasus/com/linkedin/schema/EditableSchemaFieldInfo.pdl#L12),
estadísticas de uso y perfiles de datos. Por lo tanto, debe cumplir con los siguientes requisitos.

*   debe ser único en todos los campos de un esquema.
*   hacer que la navegación del esquema en la interfaz de usuario sea más intuitiva.
*   Permitir identificar el tipo de esquema del que forma parte el campo, como un `key-schema` o un `value-schema`.
*   permitir la evolución futura

## Convenio vigente(v1)

La convención existente es simplemente usar el nombre del campo como el `fieldPath` para campos simples, y utilice el botón `dot`
nombres delimitados para campos anidados. Este régimen no satisface el [Requisitos](#requirements) mencionado anteriormente. El
En el ejemplo siguiente se muestra dónde se muestra el `uniqueness` no se cumple el requisito.

### Ejemplo: Ruta de campo ambigua

Considere lo siguiente `Avro` esquema que es un `union` de dos tipos de registro `A` y `B`, cada uno con un campo simple con
el mismo nombre `f` que es de tipo `string`. El esquema de nomenclatura v1 no puede diferenciar si un `fieldPath=f` se refiere a
el tipo de registro `A` o `B`.

    [
        {
            "type": "record",
            "name": "A",
            "fields": [{ "name": "f", "type": "string" } ]
        }, {
            "type": "record",
            "name": "B",
            "fields": [{ "name": "f", "type": "string" } ]
        }
    ]

## El esquema de codificación FieldPath(v2)

La sintaxis para la codificación V2 de la `fieldPath` se captura en la siguiente gramática. El `FieldPathSpec` es esencialmente
el tipo ruta anotada del miembro, con cada token a lo largo de la ruta que representa un nivel de miembro anidado,
comenzando desde el tipo más encerrado, hasta el miembro. En el caso de `unions` que tienen `one-of` semántica
el campo correspondiente se emitirá una vez por cada uno `member` de la unión como su `type`, junto con un camino
correspondiente a la `union` se.

### Especificaciones formales:

    <SchemaFieldPath> := <VersionToken>.<PartOfKeySchemaToken>.<FieldPathSpec>  // when part of a key-schema
                       | <VersionToken>.<FieldPathSpec> // when part of a value schema
    <VersionToken> := [version=<VersionId>] // [version=2.0] for v2
    <PartOfKeySchemaToken> := [key=True]  // when part of a key schema
    <FieldPathSpec> := <FieldToken>+  // this is the type prefixed path field (nested if repeats).
    <FieldToken> := <TypePrefixToken>.<name_of_the_field> // type prefixed path of a field.
    <TypePrefixToken> := <NestedTypePrefixToken>.<SimpleTypeToken> | <SimpleTypeToken>
    <NestedTypePrefixToken> := [type=<NestedType>]
    <SimpleTypeToken> := [type=<SimpleType>]
    <NestedType> := <name of a struct/record> | union | array | map
    <SimpleType> := int | float | double | string | fixed | enum

Para el [ejemplo anterior](#example-ambiguous-field-path), esta codificación produciría las siguientes 2 rutas de acceso únicas
correspondiente a la `A.f` y `B.f` Campos.

```python
unique_v2_field_paths = [
 "[version=2.0].[type=union].[type=A].[type=string].f",
 "[version=2.0].[type=union].[type=B].[type=string].f"
]
```

NOTA:

*   Esta codificación siempre garantiza la unicidad dentro de un esquema, ya que se codifica la anotación de tipo completo que conduce a un campo
    en el propio fieldPath.
*   El procesamiento de un fieldPath, como desde la interfaz de usuario, se simplifica simplemente caminando cada token a lo largo de la ruta desde
    de izquierda a derecha.
*   agregar PartOfKeySchemaToken permite identificar si el campo es parte de key-schema.
*   Agregar VersionToken permite la evolubilidad futura.
*   para representar `optional` campos, que a veces se modelan como `unions` en formatos como `Avro`, en lugar de tratarlo
    como `union` miembro, establezca el `nullable` miembro de `SchemaField` Para `True`.

## Ejemplos

### Tipos primitivos

```python
avro_schema = """
{
  "type": "string"
}
"""
unique_v2_field_paths = [
  "[version=2.0].[type=string]"
]
```

### Archivo

**Registro simple**

```python
avro_schema = """
{
  "type": "record",
  "name": "some.event.E",
  "namespace": "some.event.N",
  "doc": "this is the event record E"
  "fields": [
    {
      "name": "a",
      "type": "string",
      "doc": "this is string field a of E"
    },
    {
      "name": "b",
      "type": "string",
      "doc": "this is string field b of E"
    }
  ]
}
"""

unique_v2_field_paths = [
    "[version=2.0].[type=E].[type=string].a",
    "[version=2.0].[type=E].[type=string].b",
]
```

**Registro anidado**

```python
avro_schema = """
{
    "type": "record",
    "name": "SimpleNested",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "nestedRcd",
        "type": {
            "type": "record",
            "name": "InnerRcd",
            "fields": [{
                "name": "aStringField",
                 "type": "string"
            } ]
        }
    }]
}
"""

unique_v2_field_paths = [
  "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd",
  "[version=2.0].[key=True].[type=SimpleNested].[type=InnerRcd].nestedRcd.[type=string].aStringField",
]
```

**Registro recursivo**

```python
avro_schema = """
{
    "type": "record",
    "name": "Recursive",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "r",
        "type": {
            "type": "record",
            "name": "R",
            "fields": [
                { "name" : "anIntegerField", "type" : "int" },
                { "name": "aRecursiveField", "type": "com.linkedin.R"}
            ]
        }
    }]
}
"""

unique_v2_field_paths = [
  "[version=2.0].[type=Recursive].[type=R].r",
  "[version=2.0].[type=Recursive].[type=R].r.[type=int].anIntegerField",
  "[version=2.0].[type=Recursive].[type=R].r.[type=R].aRecursiveField"
]
```

```python
avro_schema ="""
{
    "type": "record",
    "name": "TreeNode",
    "fields": [
        {
            "name": "value",
            "type": "long"
        },
        {
            "name": "children",
            "type": { "type": "array", "items": "TreeNode" }
        }
    ]
}
"""
unique_v2_field_paths = [
 "[version=2.0].[type=TreeNode].[type=long].value",
 "[version=2.0].[type=TreeNode].[type=array].[type=TreeNode].children",
]
```

### Uniones

```python
avro_schema = """
{
    "type": "record",
    "name": "ABUnion",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "a",
        "type": [{
            "type": "record",
            "name": "A",
            "fields": [{ "name": "f", "type": "string" } ]
            }, {
            "type": "record",
            "name": "B",
            "fields": [{ "name": "f", "type": "string" } ]
            }
        ]
    }]
}
"""
unique_v2_field_paths: List[str] = [
    "[version=2.0].[key=True].[type=ABUnion].[type=union].a",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=A].a",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=A].a.[type=string].f",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=B].a",
    "[version=2.0].[key=True].[type=ABUnion].[type=union].[type=B].a.[type=string].f",
]
```

### Matrices

```python
avro_schema = """
{
    "type": "record",
    "name": "NestedArray",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "ar",
        "type": {
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    "null",
                    {
                        "type": "record",
                        "name": "Foo",
                        "fields": [ {
                            "name": "a",
                            "type": "long"
                        } ]
                    }
                ]
            }
        }
    }]
}
"""
unique_v2_field_paths: List[str] = [
  "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar",
  "[version=2.0].[type=NestedArray].[type=array].[type=array].[type=Foo].ar.[type=long].a",
]
```

### Mapas

```python
avro_schema = """
{
  "type": "record",
  "name": "R",
  "namespace": "some.namespace",
  "fields": [
    {
      "name": "a_map_of_longs_field",
      "type": {
        "type": "map",
        "values": "long"
      }
    }
  ]
}
"""
unique_v2_field_paths = [
  "[version=2.0].[type=R].[type=map].[type=long].a_map_of_longs_field",
]


```

### Ejemplos de tipos complejos mixtos

```python
# Combines arrays, unions and records.
avro_schema = """
{
    "type": "record",
    "name": "ABFooUnion",
    "namespace": "com.linkedin",
    "fields": [{
        "name": "a",
        "type": [ {
            "type": "record",
            "name": "A",
            "fields": [{ "name": "f", "type": "string" } ]
            }, {
            "type": "record",
            "name": "B",
            "fields": [{ "name": "f", "type": "string" } ]
            }, {
            "type": "array",
            "items": {
                "type": "array",
                "items": [
                    "null",
                    {
                        "type": "record",
                        "name": "Foo",
                        "fields": [{ "name": "f", "type": "long" }]
                    }
                ]
            }
    }]
    }]
}
"""

unique_v2_field_paths: List[str] = [
  "[version=2.0].[type=ABFooUnion].[type=union].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a",
  "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f",
]
```

Para obtener más ejemplos, consulte
el [pruebas unitarias para AvroToMceSchemaConverter](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/tests/unit/test_schema_util.py).

### Compatibilidad con versiones anteriores

Si bien este formato no es directamente compatible con el formato v1, el equivalente v1 se puede construir fácilmente a partir del v2
codificación eliminando todos los tokens v2 encerrados entre corchetes `[<new_in_v2>]`.
