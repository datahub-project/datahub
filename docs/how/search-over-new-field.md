# Onboarding to GMA Search - searching over a new field

If you need to onboard a new entity to search, refer to [How to onboard to GMA Search](./search-onboarding.md).

For this exercise, we'll add a new field to an existing aspect of corp users and search over this field. Your use case might require searching over an existing field of an aspect or create a brand new aspect and search over it's field(s). For such use cases, similar steps should be followed.

This document will also guide you on how to leverage an existing field for faceted search i.e. use the field in aggregations, sorting or in a script.

## 1: Add field to aspect (skip this step if the field already exists in an aspect)

For this example, we will add new field `courses` to [CorpUserEditableInfo](../../metadata-models/src/main/pegasus/com/linkedin/identity/CorpUserEditableInfo.pdl) which is an aspect of corp user entity.

```
namespace com.linkedin.identity

/**
 * Linkedin corp user information that can be edited from UI
 */
@Aspect.EntityUrns = [ "com.linkedin.common.CorpuserUrn" ]
record CorpUserEditableInfo {

  ...

  /**
   * Courses that the user has taken e.g. AI200: Introduction to Artificial Intelligence
   */
  courses: array[string] = [ ]

}
```

## 2: Add field to search document model

For this example, we will add field `courses` to [CorpUserInfoDocument.pdl](../../metadata-models/src/main/pegasus/com/linkedin/metadata/search/CorpUserInfoDocument.pdl) which is the search document model for corp user entity.

```
namespace com.linkedin.metadata.search

/**
 * Data model for CorpUserInfo entity search
 */
record CorpUserInfoDocument includes BaseDocument {

  ...

  /**
   * Courses that the user has taken e.g. AI200: Introduction to Artificial Intelligence
   */
  courses: optional array[string]

}
```

## 3: Modify the mapping of search index

Now, we will modify the mapping of corp user search index. Use the following Elasticsearch command to add new field to an existing index.

```json
curl http://localhost:9200/corpuserinfodocument/doc/_mapping? --data '
{
  "properties": {
    "courses": {
      "type": "text"
    }
  }
}'
```

If this field needs to be a facet i.e. you want to enable sorting, aggregations on this field or use it in scripts, then your mapping may be different depending on the type of field. For **text** fields you will need to enable _fielddata_ (disabled by default), as shown below

```json
curl http://localhost:9200/corpuserinfodocument/doc/_mapping? --data '
{
  "properties": {
    "courses": {
      "type": "text",
      "fielddata": true
    }
  }
}'
```

However _fielddata_ enablement could consume significant heap space. If possible, use unanalyzed **keyword** field as a facet. For the current example, you could either choose keyword type for the field _courses_ or create a subfield of type keyword under _courses_ and use the same for sorting, aggregations, etc (second approach described below)

```json
curl http://localhost:9200/corpuserinfodocument/doc/_mapping? --data '
{
  "properties": {
    "courses": {
      "type": "text",
      "fields": {
        "subfield": {
          "type": "keyword"
        }
      }
    }
  }
}'
```

More on this is explained in [ES guides](https://www.elastic.co/guide/en/elasticsearch/reference/current/fielddata.html).

## 4: Modify index config, so that the new mapping is picked up next time

If you want corp user search index to contain this new field `courses` next time docker containers are brought up, we need to add this field to [corpuser-index-config.json](../../docker/elasticsearch-setup/corpuser-index-config.json).

```
{
  "settings": {
    "index": {
      "analysis": {
       ...
      }
    }
  },
  "mappings": {
    "doc": {
      "properties": {

        ...

        "courses": {
          "type": "text"
        }
      }
    }
  }
}
```

Choose your analyzer wisely. For this example, we store the field `courses` as an array of string and hence use `text` data type. Default analyzer is `standard` and it provides grammar based tokenization.

## 5: Update the index builder logic

Index builder is where the logic to transform an aspect to search document model is defined. For this example, we will add the logic in [CorpUserInfoIndexBuilder](../../metadata-builders/src/main/java/com/linkedin/metadata/builders/search/CorpUserInfoIndexBuilder.java).

```java
package com.linkedin.metadata.builders.search;

@Slf4j
public class CorpUserInfoIndexBuilder extends BaseIndexBuilder<CorpUserInfoDocument> {

  public CorpUserInfoIndexBuilder() {
    super(Collections.singletonList(CorpUserSnapshot.class), CorpUserInfoDocument.class);
  }

  ...

  @Nonnull
  private CorpUserInfoDocument getDocumentToUpdateFromAspect(@Nonnull CorpuserUrn urn,
      @Nonnull CorpUserEditableInfo corpUserEditableInfo) {
    final String aboutMe = corpUserEditableInfo.getAboutMe() == null ? "" : corpUserEditableInfo.getAboutMe();
    return new CorpUserInfoDocument()
        .setUrn(urn)
        .setAboutMe(aboutMe)
        .setTeams(corpUserEditableInfo.getTeams())
        .setSkills(corpUserEditableInfo.getSkills())
        .setCourses(corpUserEditableInfo.getCourses());
  }

  ...

}

```

## 6: Update search query template, to start searching over the new field

For this example, we will modify [corpUserESSearchQueryTemplate.json](../../gms/impl/src/main/resources/corpUserESSearchQueryTemplate.json) to start searching over the field `courses`. Here is an example.

```json
{
  "function_score": {
    "query": {
      "query_string": {
        "query": "$INPUT",
        "fields": [
          "fullName^4",
          "ldap^2",
          "managerLdap",
          "skills",
          "courses"
          "teams",
          "title"
        ],
        "default_operator": "and",
        "analyzer": "standard"
      }
    },
    "functions": [
      {
        "filter": {
          "term": {
            "active": true
          }
        },
        "weight": 2
      }
    ],
    "score_mode": "multiply"
  }
}
```

As you can see in the above query template, corp user search is performed across multiple fields, to which the field `courses` has been added.

## 7: (_Optional_) For a field that is a facet, modify the search config.

We define the list of facets in search config. If your field needs to be a facet, add it to the set of facets defined in method _getFacetFields_. For this example, we will add the logic in [CorpUserSearchConfig](../../gms/impl/src/main/java/com/linkedin/metadata/configs/CorpUserSearchConfig.java).

```java
package com.linkedin.metadata.configs;

public class CorpUserSearchConfig extends BaseSearchConfig<CorpUserInfoDocument> {
  @Override
  @Nonnull
  public Set<String> getFacetFields() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("courses"));
  }

  ...
}
```

## 8: Test your changes

Make sure relevant docker containers are rebuilt before testing the changes.
If this is a new field that has been added to an existing snapshot, then you can test by ingesting data that contains this new field. Here is an example of ingesting to `/corpUsers` endpoint, with the new field `courses`.

```
curl 'http://localhost:8080/corpUsers?action=ingest' -X POST -H 'X-RestLi-Protocol-Version:2.0.0' --data '
{
  "snapshot": {
    "aspects": [
      {
        "com.linkedin.identity.CorpUserEditableInfo": {
          "courses": [
            "Docker for Data Scientists",
            "AI100: Introduction to Artificial Intelligence"
          ],
          "skills": [

          ],
          "pictureLink": "https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png",
          "teams": [

          ]
        }
      }
    ],
    "urn": "urn:li:corpuser:datahub"
  }
}'
```

Once the ingestion is done, you can test your changes by issuing search queries. Here is an example query with response.

```sh
curl "http://localhost:8080/corpUsers?q=search&input=ai100" -H 'X-RestLi-Protocol-Version: 2.0.0' -s | jq

Response:
{
  "metadata": {
    "urns": [
      "urn:li:corpuser:datahub"
    ],
    "searchResultMetadatas": [

    ]
  },
  "elements": [
    {
      "editableInfo": {
        "skills": [

        ],
        "courses": [
          "Docker for Data Scientists",
          "AI100: Introduction to Artificial Intelligence"
        ],
        "pictureLink": "https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web/packages/data-portal/public/assets/images/default_avatar.png",
        "teams": [

        ]
      },
      "username": "datahub",
      "info": {
        "active": true,
        "fullName": "Data Hub",
        "title": "CEO",
        "displayName": "Data Hub",
        "email": "datahub@linkedin.com"
      }
    }
  ],
  "paging": {
    "count": 10,
    "start": 0,
    "total": 1,
    "links": [

    ]
  }
}
```

# Appendix: MidTier and UI changes

## 1: Check if facets are enabled for the entity (optional)

Inside the `PersonEntity` [render-props.ts](../../datahub-web/@datahub/data-models/addon/entity/person/render-props.ts)

```json
{
  "search": {
    "showFacets": true
  }
}
```

make sure `showFacets` property is set to `true`.

## 2: Add fields to facets in MidTier if desired (optional)

In [Search.java](../../datahub-frontend/app/controllers/api/v2/Search.java) add the desired fields here:

```java
private static final Set<String> CORP_USER_FACET_FIELDS = ImmutableSet.of("courses");
```

## 3: Add field in the Person entity

In [person-entity.ts](../../datahub-web/%40datahub/data-models/addon/entity/person/person-entity.ts), add your new property

```ts
@alias('entity.courses')
courses?: Array<string>;
```

## 4: Add fields in the Person configuration json

Inside the `PersonEntity` [render-props.ts](../../datahub-web/@datahub/data-models/addon/entity/person/render-props.ts), add your new property:

```json
{
  "showInAutoCompletion": true,
  "fieldName": "courses",
  "showInResultsPreview": true,
  "displayName": "Courses",
  "showInFacets": true,
  "desc": "Courses description of the field",
  "example": "courses:value"
}
```
