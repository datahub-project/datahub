
https://www.elastic.co/guide/index.html

```
export ELASTICSEARCH_SERVER_URL=http://localhost:9200
```

create index and put mappings
```
curl -XPUT '$ELASTICSEARCH_SERVER_URL/wherehows_v1' --data @index_mapping.json

```

create index alias 
Using aliases has allowed us to continue using elasticsearch without a huge operational nightmare
this allows switching transparently between one index and another on a running cluster
Here we use wherehows as alias for different versions, newer indexes such as wherehows_v2 can be created, populated and then points to wherehows, as a public index interface
```
curl -XPUT '$ELASTICSEARCH_SERVER_URL/_aliases' -d '
{
  "actions": [
    {
      "add": {
        "index": "wherehows_v1",
        "alias": "wherehows"
      }
    }
  ]
}'
```

query index/type mapping
```
$ELASTICSEARCH_SERVER_URL/wherehows/_mapping/dataset
$ELASTICSEARCH_SERVER_URL/wherehows/_mapping/comment
$ELASTICSEARCH_SERVER_URL/wherehows/_mapping/flow_jobs
$ELASTICSEARCH_SERVER_URL/wherehows/_mapping/field
$ELASTICSEARCH_SERVER_URL/wherehows/_mapping/metric

$ELASTICSEARCH_SERVER_URL:9200/wherehows/_mapping/
```

delete an index
```
curl -XDELETE '$ELASTICSEARCH_SERVER_URL:9200/wherehows'
```