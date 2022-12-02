- Start Date: 2021-02-17
- RFC PR: https://github.com/datahub-project/datahub/pull/2112
- Discussion Issue: (GitHub issue this was discussed in before the RFC, if any)
- Implementation PR(s): (leave this empty)

# Tags

## Summary

We suggest a generic, global tagging solution for Datahub. As the solution is quite generic and flexible, it can also
hopefully serve as an stepping stone for new, cool features in the future.

## Motivation

Currently some entities, such as Datasets, can be tagged using strings, but unfortunately this solution is quite
limited.

A general tag implementation will allow us to define and attach a new and simple type of metadata to all type of
entities. As the tags would be defined globally, tagging multiple objects with the same tag will give us the possibility
to define and search based on a new kind of relationship, for example which datasets and ML Models that are tagged to
include PII data. This allows for describing relationships between object that would otherwise not have a direct lineage
relationship. Moreover, tags would lower that bar to add simple metadata to any object in the Datahub instance and open
the door to crowd-sourcing metadata. Remembering that tags themselves are entities, it would also be possible to tag
tags, enabling a hierarchy of sorts.

The solution is meant to be quite generic and flexible, and we're not trying to be too opinionated about how a user
should use the feature. We hope that this initial generic solution can serve as a stepping stone for cool futures in the
future.

## Requirements

- Ability to associate tags with any type of entity, even other tags!
- Ability to tag the same entity with multiple tags.
- Ability to tag multiple objects with the same tag instance.
- To the point above, ability to make easy tag-based searches later on.
- Metadata on tags is TBD

### Extensibility

The normal new-entity-onboarding work is obviously required.

Hopefully this can serve as a stepping stone to work on special cases such as the tag-based privacy tagging mentioned in
the roadmap.

## Non-Requirements

Let's leave the UI work required for this to another time.

## Detailed design

We want to introduce some new under `datahub/metadata-models/src/main/pegasus/com/linkedin/common/`.

### `Tag` entity

First we create a `TagMetadata` entity, which defines the actual tag-object.

The edit property defines the edit rights of the tag, as some tags (like sensitivity tags) should be read-only for a
majority of users

```
/**
 * Tag information
 */
record TagMetadata {
   /**
   * Tag URN, e.g. urn:li:tag:<name>
   */
   urn: TagUrn

   /**
   * Tag value.
   */
   value: string

   /**
   * Optional tag description
   */
   description: optional string

   /**
   * Audit stamp associated with creation of this tag
   */
   createStamp: AuditStamp
}
```

### `TagAttachment`

We define a `TagAttachment`-model, which describes the application of a tag to a entity

```
/**
 * Tag information
 */
record TagAttachment {

  /**
   * Tag in question
   */
  tag: TagUrn

  /**
   * Who has edit rights to this employment.
   * WIP, pending access-control support in Datahub.
   * Relevant for privacy tags at least.
   * We might also want to add view rights?
   */
  edit: union[None, any, role-urn]

   /**
   * Audit stamp associated with employment of this tag to this entity
   */
   attachmentStamp: AuditStamp
}
```

### `Tags` container

Then we define a `Tags`-aspect, which is used as a container for tag employments.

```
namespace com.linkedin.common

/**
 * Tags information
 */
record Tags {

   /**
   * List of tag employments
   */
   elements: array[TagAttachment] = [ ]
}
```

This can easily be taken into use with wall entities that we want to be able to use tags, e.g. `Datasets`. As we see a
lot of potential in tagging individual dataset fields as well, we can either add a reference to a Tags-object in the
`SchemaField` object, or alternative create a new `DatasetFieldTags`, similar to `DatasetFieldMapping`.

## How we teach this

We should create/update user guides to educate users for:

- Suggestions on how to use tags: low threshold metadata-addition, and the possibility of doing new types of searches

## Drawbacks

This is definitely more complex than just adding strings to an array.

## Alternatives

An array of string is a simple solution but does allow for the same functionality as suggested here.

Another alternative would be simplify the models by removing some of the metadata in the `TagMetadata` and
`TagAttachment` entities, such as the the edit/view permission field, the audit stamps, and the descriptions.

Apache Atlas uses a similar approach. The require you to create a Tag instance before it can be associated with an
"asset", and the attachment is done using a dropdown list. The tags can also have attributes and a description. See
[here](https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.5.3/bk_data-governance/content/ch_working_with_atlas_tags.html)
for an example. The tags are a central piece in the UI and readably searchable, as easily as datasets.

Atlas also has concept very closely related to tags, called _classification_. Classifications are similar to tags in
that they need to be created separately, can have attributes (but no description?) and are attached to assets is done
using a dropdown list. Classifications have the added functionality of propagation, which means that they are
automatically applied to downstream assets, unless specifically set to not do so. Any change to a classification (say an
attribute change) also flows downstream, and in downstream assets you're able to see from where the classification
propagated from. See
[here](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/using-atlas/content/propagate_classifications_to_derived_entities.html)
for an example.

## Rollout / Adoption Strategy

Using the functionality is optional and does not break other functionality as is. The solution is generic enough that
the users can easily take into use. It can be take into use as any other entity and aspect.

## Future Work

- add `Tags` to aspects for entities.
- Implement relationship builders as needed.
- The implementation of and need for access control to tags is an open question
- As this is first and foremost a tool for discovery, the UI work is extensible:
  - Creating tags in a way that makes duplication and spelling mistakes difficult.
  - Attaching tags to entities: autocomplete, dropdown, etc.
  - Visualizing existing tags, and which are most popular?
- Explore the idea about a special "classification" type, that propagates downstream, as in Atlas.

## Unresolved questions

- How do we want to map dataset fields to tags?
- Do we want to implement edit/view rights?
