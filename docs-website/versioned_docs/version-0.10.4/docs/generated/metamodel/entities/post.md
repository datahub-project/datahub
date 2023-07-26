---
sidebar_position: 28
title: Post
slug: /generated/metamodel/entities/post
custom_edit_url: >-
  https://github.com/datahub-project/datahub/blob/master/docs/generated/metamodel/entities/post.md
---

# Post

## Aspects

### postInfo

Information about a DataHub Post.

<details>
<summary>Schema</summary>

```javascript
{
  "type": "record",
  "Aspect": {
    "name": "postInfo"
  },
  "name": "PostInfo",
  "namespace": "com.linkedin.post",
  "fields": [
    {
      "type": {
        "type": "enum",
        "symbolDocs": {
          "HOME_PAGE_ANNOUNCEMENT": "The Post is an Home Page announcement."
        },
        "name": "PostType",
        "namespace": "com.linkedin.post",
        "symbols": [
          "HOME_PAGE_ANNOUNCEMENT"
        ],
        "doc": "Enum defining types of Posts."
      },
      "name": "type",
      "doc": "Type of the Post."
    },
    {
      "type": {
        "type": "record",
        "name": "PostContent",
        "namespace": "com.linkedin.post",
        "fields": [
          {
            "Searchable": {
              "fieldType": "TEXT_PARTIAL"
            },
            "type": "string",
            "name": "title",
            "doc": "Title of the post."
          },
          {
            "type": {
              "type": "enum",
              "symbolDocs": {
                "LINK": "Link content",
                "TEXT": "Text content"
              },
              "name": "PostContentType",
              "namespace": "com.linkedin.post",
              "symbols": [
                "TEXT",
                "LINK"
              ],
              "doc": "Enum defining the type of content held in a Post."
            },
            "name": "type",
            "doc": "Type of content held in the post."
          },
          {
            "type": [
              "null",
              "string"
            ],
            "name": "description",
            "default": null,
            "doc": "Optional description of the post."
          },
          {
            "java": {
              "class": "com.linkedin.common.url.Url",
              "coercerClass": "com.linkedin.common.url.UrlCoercer"
            },
            "type": [
              "null",
              "string"
            ],
            "name": "link",
            "default": null,
            "doc": "Optional link that the post is associated with."
          },
          {
            "type": [
              "null",
              {
                "type": "record",
                "name": "Media",
                "namespace": "com.linkedin.common",
                "fields": [
                  {
                    "type": {
                      "type": "enum",
                      "symbolDocs": {
                        "IMAGE": "The Media holds an image."
                      },
                      "name": "MediaType",
                      "namespace": "com.linkedin.common",
                      "symbols": [
                        "IMAGE"
                      ],
                      "doc": "Enum defining the type of content a Media object holds."
                    },
                    "name": "type",
                    "doc": "Type of content the Media is storing, e.g. image, video, etc."
                  },
                  {
                    "java": {
                      "class": "com.linkedin.common.url.Url",
                      "coercerClass": "com.linkedin.common.url.UrlCoercer"
                    },
                    "type": "string",
                    "name": "location",
                    "doc": "Where the media content is stored."
                  }
                ],
                "doc": "Carries information about which roles a user is assigned to."
              }
            ],
            "name": "media",
            "default": null,
            "doc": "Optional media that the post is storing"
          }
        ],
        "doc": "Content stored inside a Post."
      },
      "name": "content",
      "doc": "Content stored in the post."
    },
    {
      "Searchable": {
        "fieldType": "COUNT"
      },
      "type": "long",
      "name": "created",
      "doc": "The time at which the post was initially created"
    },
    {
      "Searchable": {
        "fieldType": "COUNT"
      },
      "type": "long",
      "name": "lastModified",
      "doc": "The time at which the post was last modified"
    }
  ],
  "doc": "Information about a DataHub Post."
}
```

</details>

## Relationships

## [Global Metadata Model](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)

![Global Graph](https://raw.githubusercontent.com/acryldata/static-assets-test/master/imgs/datahub-metadata-model.png)
