- Start Date: 2021-02-17
- RFC PR: (after opening the RFC PR, update this with a link to it and update the file name)
- Discussion Issue: (GitHub issue this was discussed in before the RFC, if any)
- Implementation PR(s): (leave this empty)

# Tags

## Summary

Adding general support for tag lowers the bar to add simple metadata to entities. Tags can be used as part of the
business logic (e.g. sensitivity tag on a data set) or more ad hoc use cases, such as a data analyst's favorite
dashboard. A general tag implementation will also allow us to define and search based on a new kind relationship, like
which datasets and ML models are marked to contain PII data.

> needs work

## Basic example

> If the proposal involves a new or changed API, include a basic code example. Omit this section if it's not applicable.

## Motivation

> Why are we doing this? What use cases does it support? What is the expected outcome?
>
> Please focus on explaining the motivation so that if this RFC is not accepted, the motivation could be used to develop
> alternative solutions. In other words, enumerate the constraints you are trying to solve without coupling them too
> closely to the solution you have in mind.

## Requirements

> What specific requirements does your design need to meet? This should ideally be a bulleted list of items you wish to
> achieve with your design. This can help everyone involved (including yourself!) make sure your design is robust enough
> to meet these requirements.
>
> Once everyone has agreed upon the set of requirements for your design, we can use this list to review the detailed
> design.

### Extensibility

> Please also call out extensibility requirements. Is this proposal meant to be extended in the future? Are you adding a
> new API or set of models that others can build on in later? Please list these concerns here as well.

## Non-Requirements

> Call out things you don't want to discuss in detail during this review here, to help focus the conversation. This can
> include things you may build in the future based off this design, but don't wish to discuss in detail, in which case
> it may also be wise to explicitly list that extensibility in your design is a requirement.
>
> This list can be high level and not detailed. It is to help focus the conversation on what you want to focus on.

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
  tag: Urn

   /**
   * Audit stamp associated with creation of this tag
   */
   createStamp: AuditStamp
}
```

### `TagEmployment`

We define a `TagEmployment`-model, which describes the application of a tag to a entity

```
/**
 * Tag information
 */
record TagEmployment {

  /**
   * Tag in question
   */
  tag: TagMetadata

  /**
   * Who has edit rights to this employment. WIP
   */
  edit: union[None, any, role-urn]

   /**
   * Audit stamp associated with employment of this tag to this entity
   */
   applicationStamp: AuditStamp
}
```

### `Tags` container entity

Then we define a `Tags`-model, which is used as a container for tag employments. This container is taken into use in
aspects.

```
namespace com.linkedin.common

/**
 * Tags information
 */
record Tags {

   /**
   * List of tag employments
   */
   elements: array[TagEmployment] = [ ]
}
```

As `TagMetadata` is an entity in itself, it can be tagged as well, allowing for hierarchies of tags to be constructed.

> This is the bulk of the RFC.

> Explain the design in enough detail for somebody familiar with the framework to understand, and for somebody familiar
> with the implementation to implement. This should get into specifics and corner-cases, and include examples of how the
> feature is used. Any new terminology should be defined here.

## How we teach this

> What names and terminology work best for these concepts and why? How is this idea best presented? As a continuation of
> existing DataHub patterns, or as a wholly new one?

> What audience or audiences would be impacted by this change? Just DataHub backend developers? Frontend developers?
> Users of the DataHub application itself?

> Would the acceptance of this proposal mean the DataHub guides must be re-organized or altered? Does it change how
> DataHub is taught to new users at any level?

> How should this feature be introduced and taught to existing audiences?

## Drawbacks

> Why should we _not_ do this? Please consider the impact on teaching DataHub, on the integration of this feature with
> other existing and planned features, on the impact of the API churn on existing apps, etc.

> There are tradeoffs to choosing any path, please attempt to identify them here.

## Alternatives

> What other designs have been considered? What is the impact of not doing this?

> This section could also include prior art, that is, how other frameworks in the same domain have solved this problem.

## Rollout / Adoption Strategy

> If we implemented this proposal, how will existing users / developers adopt it? Is it a breaking change? Can we write
> automatic refactoring / migration tools? Can we provide a runtime adapter library for the original API it replaces?

## Future Work

> Describe any future projects, at a very high level, that will build off this proposal. This does not need to be
> exhaustive, nor does it need to be anything you work on. It just helps reviewers see how this can be used in the
> future, so they can help ensure your design is flexible enough.

## Unresolved questions

> Optional, but suggested for first drafts. What parts of the design are still TBD?
