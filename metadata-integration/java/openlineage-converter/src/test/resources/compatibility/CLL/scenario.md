# Description

scenario events represents following sql code executed in spark sql:

```
CREATE TABLE cll_source1 (a int, b string)
CREATE TABLE cll_source2 (a int, c int)

CREATE TABLE tbl1
USING hive
LOCATION '/tmp/cll_test/tbl1'
AS SELECT
    t1.a as ident,
    CONCAT(b, 'test') as trans,
    SUM(c) as agg
FROM
    (SELECT a, c from cll_source2 where a > 1) t2
JOIN cll_source1 t1 on t1.a = t2.a
GROUP BY t1.a, b
```

# Entities

input entities are hive tables
cll_source1
cll_source2

output entity is hive table tbl1

# Facets

Facets present in the events:

- ColumnLineageDatasetFacet
- DatasourceDatasetFacet
- JobTypeJobFacet
- LifecycleStateChangeDatasetFacet
- ParentRunFacet
- SQLJobFacet
- SchemaDatasetFacet
- SymlinksDatasetFacet
