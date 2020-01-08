
### Table of Contents
1. <a href="#datasets">Datasets</a>
 * <a href="#datasets-by-urn">Find Dataset by URN</a>
 * <a href="#datasets-by-id">Find Dataset by ID</a>
 * <a href="#dataset-schema">Dataset Schema (Columns)</a>
    1. <a href="#dataset-column-comments-get">Get Dataset Column Comments</a>
    1. <a href="#dataset-column-comments-create">Create Dataset Column Comments</a>
    1. <a href="#dataset-column-comments-update">Update Dataset Column Comments</a>
    1. <a href="#dataset-column-comments-delete">Delete Dataset Column Comments</a>
    1. <a href="#dataset-column-comments-similar">Similar Dataset Column Comments</a>
    1. <a href="#dataset-column-comments-promotion">Dataset Column Comment Promotion</a>
    1. <a href="#dataset-column-comments-propagation">Dataset Column Comment Propagation</a>
 * <a href="#dataset-properties">Dataset Properties</a>
 * <a href="#dataset-sample-data">Dataset Sample Data</a>
 * <a href="#dataset-comments">Dataset Comments</a>
    1. <a href="#dataset-comments-get">Get Dataset Comments</a>
    1. <a href="#dataset-comments-create">Create Dataset Comments</a>
    1. <a href="#dataset-comments-update">Update Dataset Comments</a>
    1. <a href="#dataset-comments-delete">Delete Dataset Comments</a>
 * <a href="#dataset-favorite">Dataset Favorite</a>
 * <a href="#dataset-watch">Dataset Watching</a>
    1. <a href="#dataset-watch-create">Watch A Dataset</a>
    1. <a href="#dataset-watch-update">Updating Watched Notifications</a>
    1. <a href="#dataset-watch-delete">Un-watch A Dataset</a>
1. <a href="#metrics">Metrics</a>
 * <a href="#metric-all">Paginated Metrics</a>
 * <a href="#metric-by-dashboard">Metrics By Dashboard</a>
 * <a href="#metric-by-dashboard-and-group">Metrics By Dashboard and Group</a>
 * <a href="#metric-by-id">Metrics By ID</a>
 * <a href="#metric-update">Updating Metric Info</a>
 * <a href="#metric-watch">Watching Metrics</a>
    1. <a href="#metric-watch-create">Watch A Metric</a>
    1. <a href="#metric-watch-delete">Un-watch A Metric</a>
1. <a href="#flows">Flows</a>
 * <a href="#flows-by-application">Flows By Application</a>
 * <a href="#flows-by-application-project">Flows By Application and Project</a>
 * <a href="#flows-by-application-project-flow">Flows By Application, Project and Flow</a>
1. <a href="#basic-search">Basic Search</a>
 * <a href="#search-auto-complete">Auto-complete</a>
 * <a href="#search">Search</a>
1. <a href="#advanced-search">Advanced Search</a>
 * <a href="#advanced-search-scopes">Scopes</a>
 * <a href="#advanced-search-tables">Tables</a>
 * <a href="#advanced-search-fields">Fields</a>
 * <a href="#advanced-search-comments">Comments</a>
 * <a href="#advanced-search-sources">Sources</a>
 * <a href="#advanced-search-search">Search</a>

<a name="datasets" />
## Datatsets

<a name="datasets-by-urn" />
### Datasets By URN
##### Accepted Query Params
| Name | Function | Default | Required |
|:---|---|---|:---:|
| size | Page Size | 10 | Y |
| page | Page | 1 | Y |
| urn | URN to search |  | Y |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets?size=1&page=1&urn=hdfs://
```

<a name="datasets-by-urn" />
### Datasets By ID

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId
```

<a name="dataset-schema" />
### Dataset Schema

##### Example
```bash
curl $WHEREHOWS/api/v1/datasets/:datasetId/columns
```

<a name="dataset-column-comments-get" />
#### Get Dataset Column Comments

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId/columns/:columnId/comments
```

<a name="dataset-column-comments-create" />
#### Create Dataset Column Comments

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/columns/:columnId/comments',
    method: 'POST',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      text: 'Blarg',
      csrfToken: token
    }
  })
})(jQuery)
```

<a name="dataset-column-comments-update" />
#### Update Dataset Column Comments

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/columns/:columnId/comments',
    method: 'POST',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      id: 35,
      text: 'Blarg',
      csrfToken: token
    }
  })
})(jQuery)
```


<a name="dataset-column-comments-delete" />
#### Delete Dataset Column Comments

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/columns/:columnId/comments/:commentId',
    method: 'DELETE',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      csrfToken: token
    }
  })
})(jQuery)
```
<a name="dataset-column-comments-similar" />
#### Similar Dataset Column Comments
The endpoint provides a list of comments that belong to columns of a similar
name. This can be helpful in the re-use of comments. Allows Datasets that
share a field (or Foreign Key) to pass comments along to that datasets column.

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId/columns/:columnId/comments/similar
```

<a name="dataset-column-comments-promotion" />
#### Promote Dataset Column Comments
The endpoint provides means with which a user can promote a column comment to
be the default comment for that column.

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    method: 'PATCH',
    url: '/api/v1/datasets/:datasetId/columns/columnId/comments',
    headers: {
      'Csrf-Token': token    
    }
    dataType: 'json',
    contentType: 'application/json',
    data: JSON.stringify({
      commentId: 3599,
      csrfToken: token
    })
  })
})(jQuery)
```

<a name="dataset-column-comments-propagation" />
#### Dataset Column Comment Propagation
The endpoint provides means with which a user can propagate a column comment to
be the default column comment of multiple similar comments

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    method: 'PATCH',
    url: '/api/v1/datasets/:datasetId/columns/columnId/comments?csrfToken=' + token,
    headers: {
      'Csrf-Token': token    
    }
    dataType: 'json',
    contentType: 'application/json',
    data: JSON.stringify
    ( [ { commentId: 3599
        , datasetId: 531451
        , columnId: 6545345
        }
      , { commentId: 3599
        , datasetId: 53158
        , columnId: 548648
        }
      ]
    )
  })
})(jQuery)
```

<a name="dataset-properties" />
### Dataset Properties

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId/properties
```

<a name="dataset-sample-data" />
### Dataset Sample Data

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId/sample
```

<a name="dataset-impact-analysis" />
### Dataset Impact Analysis

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId/impacts
```

<a name="dataset-comments" />
### Dataset Comments

<a name="dataset-comments-get"/>
#### Get Dataset Comments
##### Accepted Query Params for GET
| Name | Function | Default | Required |
|:--|---|---|:--:|
| size | Size of response per page | 3 | N |
| page | Page of response to return | 1 | Y |

##### GET Example
```bash
curl $WHEREHOWS_URL/api/v1/datasets/:datasetId/comments
```

<a name="dataset-comments-create"/>
#### Create Dataset Comment
##### Parameters
| Name | Function | Required |
|:--|---|:--:|
| datasetId | ID of dataset for comment | Y |
| text | Comment Text (text or markdown) | Y |
| type | Comment Type | Comment | Y |
| csrfToken | Csrf Token | Y |

##### POST Headers
| Name | Function | Required |
|:--|---|:--:|
| Csrf-Token | CSRF Token | Y |

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datatsets/:datasetId/comments',
    method: 'POST',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      datasetId: 1,
      text: "test test blargs",
      type: "Comment",
      csrfToken: token
    }   
  })
})(jQuery)
```

<a name="dataset-comments-update"/>
#### Update Dataset Comment
##### Parameters
| Name | Function | Required |
|:--|---|:--:|
| datasetId | ID of dataset for comment |  Y |
| text | Comment Text (text or markdown) | Y |
| type | Comment Type | comment | Y |
| csrfToken | Csrf Token | Y |
| id | Comment ID | Y |

##### Headers
| Name | Function | Required |
|:--|---|:--:|
| Csrf-Token | CSRF Token | Y |

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datatsets/:datasetId/comments',
    method: 'POST',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      datasetId: 1,
      id: 234304, // comment id
      text: "test test blargs",
      type: "Comment",
      csrfToken: token
    }   
  })
})(jQuery)
```

<a name="dataset-comments-delete"/>
#### Delete Dataset Comment
Note: Currently this actually deletes the record. Only the user that created the comment can delete it.

##### Headers
| Name | Function | Required |
|:--|---|:--:|
| Csrf-Token | CSRF Token | Y |

##### Params
| Name | Function | Required |
|:--|---|:--:|
| Csrf-Token | CSRF Token | Y |

##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datatsets/:datasetId/comments/:commentId',
    method: 'DELETE',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      csrfToken: token
    }   
  })
})(jQuery)
```

<a name="dataset-favorite" />
### Dataset Favoriting
A shortcut system for datasets a user finds useful.

##### Favoriting
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/favorite',
    method: 'POST',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      csrfToken: token
    }
  })
})(jQuery)
```

##### Un-Favoriting
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/favorite',
    method: 'DELETE',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      csrfToken: token
    }
  })
})(jQuery)
```

<a name="dataset-watch" />
### Dataset Watching
Watch a dataset for changes. Allow for email notification of changes (Coming Soon)

<a name="dataset-watch-create" />
#### Watch A Dataset
##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/watch',
    method: 'POST',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      csrfToken: token,
      item_type: 'dataset',
      notification_type: 'weekly'
    }
  })
})(jQuery)
```
<a name="dataset-watch-update" />
#### Updating the Watch of a Dataset
Currently there is no way to update the notification_type. This will likely
come with the email feature.

<a name="dataset-watch-delete" />
#### Un-watch A Dataset
##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/','')
  $.ajax({
    url: '/api/v1/datasets/:datasetId/watch/:watchId',
    method: 'DELETE',
    headers: {
      'Csrf-Token': token
    },
    dataType: 'json',
    data: {
      csrfToken: token
    }
  })
})(jQuery)
```


<a name="metrics" />
## Metrics

<a name="metric-all" />
### All Metrics

##### Accepted Query Parameters
| Name | Function               | Default | Required |
|:-----|------------------------|---------|:--------:|
| Size | Size of response array | 10      |   N      |
| Page | Page For Response      | 1       | N        |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/metrics?size=4&page=1
```

<a name="metric-by-dashboard" />
### Metrics By Dashboard

##### Accepted Query Parameters
| Name | Function | Default | Required |
|:--|---|---|:--:|
| Size | Size of response array | 10 | N |
| Page | Page For Response | 1 | N |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/metrics/name/:dashboardName?size=4&page=1
```

<a name="metric-by-dashboard-and-group" />
### Metrics By Dashboard and Group

##### Accepted Query Parameters
| Name | Function | Default | Required |
|:--|---|---|:--:|
| Size | Size of response array | 10 | N |
| Page | Page For Response | 1 | N |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/metrics/name/:dashboardName/:groupName?size=4&page=1
```

<a name="metric-by-id" />
### Metric By ID

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/metrics/:id
```

<a name="metric-update" />
### Updating Metrics

This API allows for full or partial update of fields within a specific metric.

##### Required Headers
| Name | Function |
|:--|---|
| Csrf-Token | CSRF-Token |

##### Accepted Input
| Name | Function |
|:---|:---|
| id | Metric ID |
| name | Metric Name |
| dashboardName | Associated Dashboard |
| group | Associated Dashboard Group |
| category | Business Category |
| description | Metric Description |
| formula | Metric Formula (SQL, PIG, Hive) |
| displayFactor | |
| displayFactorSym | |
| grain | |
| productPageKeyGroupSK | |
| refID | |
| refIDType | |
| schema | |
| sourceType | |
| urn | Metric Urn |


##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    url: '/api/v1/metrics/:metricID/update',
    method: 'POST',
    headers: {
      'Csrf-Token': token    
    },
    dataType: 'json',
    data: {
      csrfToken: token,
      formula: "Pig: /r/n set mapred.min.spite.size 1073741824",
      id: 23
    }
  })
})(jQuery)
```


<a name="metric-watch" />
### Watching Metrics

<a name="metric-watch-create" />
#### Watching Metrics
##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    url: '/api/v1/metrics/:id/watch',
    method: 'POST',
    headers: {
      'Csrf-Token': token    
    },
    dataType: 'json',
    data: {
      csrfToken: token,
      item_type: "metric",
      notification_type: "weekly"
    }
  })
})(jQuery)
```

<a name="metric-watch-delete" />
#### Unwatch Metric
##### Example
```javascript
(function($){
  var token = $("#csrfToken").val().replace('/', '')
  $.ajax({
    url: '/api/v1/metrics/:id/watch/:watchId',
    method: 'DELETE',
    headers: {
      'Csrf-Token': token   
    },
    dataType: 'json',
    data: {
      csrfToken: token
    }
  })
})(jQuery)
```


<a name="flows" />
## Flows

<a name="flows-by-application" />
### Flows by Application (cluster)

##### Accepted Query Parameters
| name | function | default | required |
|:--|---|---|:--:|
| size | size of response array | 10 | n |
| page | page number | 1 | n |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/flows/:application?page=1&size=5
```

<a name="flows-by-application-project" />
### Flows By Application and Project

##### Accepted Query Parameters
| name | function | default | required |
|:--|---|---|:--:|
| size | size of response array | 10 | n |
| page | page number | 1 | n |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/flows/:application/:project?page=1&size=5
```

<a name="flows-by-application-project-flow" />
### Flows by Application, Project, and Flow

##### Accepted Query Parameters
| name | function | default | required |
|:--|---|---|:--:|
| size | size of response array | 10 | n |
| page | page number | 1 | n |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/flows/:application/:project/:flow?page=1&size=5
```

<a name="basic-search" />
## Basic Search

<a name="search-auto-complete" />
### Search Auto-complete

Returns list of datasets, metrics, and flows for auto-complete

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/autocomplete/search
```

<a name="search" />
### Search

#### Accepted Query Parameters
| Name | Function |
|:--|---|
| page | Page of results to return |
| size | Number of results to return |
| category | Category: [dataset, metric, comment] |
| source | Source (Dataset Source ex. Oracle, Teradata, HDFS) |

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/search/:keyword?page=1&category=dataset&source=all
```

<a name="advanced-search" />
## Advanced Search

<a name="advanced-search-scopes" />
### Scopes

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/advsearch/scopes
```

<a name="advanced-search-tables" />
### Tables
##### Example
```bash
curl $WHEREHOWS_URL/api/v1/advsearch/tables
```

<a name="advanced-search-fields" />
### Fields
##### Example
```bash
curl $WHEREHOWS_URL/api/v1/advsearch/fields
```

<a name="advanced-search-comments" />
### Comments
##### Example
```bash
curl $WHEREHOWS_URL/api/v1/advsearch/comments
```

<a name="advanced-search-sources" />
### Sources
##### Example
```bash
curl $WHEREHOWS_URL/api/v1/advsearch/sources
```

<a name="advanced-search-search" />
### Search

##### Accepted Query Parameters
| Name | Function |
|:--|:--|
| searchOpts | stringified JSON object |

##### Sample Search Opts
```javascript
{ "scope":
  { "in": "ADDRESS_BOOK"
  , "not": ""
  }
, "table":
  { "in": "ADDRESS_BOOK_DATA"
  , "not": ""
  }
, "fields":
  { "any": "member_id"
  , "all": ""
  , "not": ""
  }
, "comments": ""
, "sources": "Teradata, Hdfs, Oracle"
}
```

##### Example
```bash
curl $WHEREHOWS_URL/api/v1/advsearch/search?searchOpts=$SEARCH_OPTS
```
