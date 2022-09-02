- Start Date: 2022-09-02
- RFC PR: (after opening the RFC PR, update this with a link to it and update the file name)
- Discussion Issue: N/A
- Implementation PR(s): (leave this empty)

# Serialisation of Updates via GMS

## Summary

Make it possible for the GMS to serialise updates by rejecting an update if an aspect has changed between a client 
reading the state and writing a proposed new state.

## Basic example

When a client connects to DataHub and wants to make changes to an existing aspect, their update may depend on the 
current state of that aspect. An example would be adding items to a list or adding a new aspect on the basis that 
one doesn't yet exist.

Because the update endpoint requires client to write the full state of the aspect they wish to update, this can lead 
to race conditions. If Client A and Client B are both writing to the same aspect concurrently they will (silently) find 
that only one of their updates will have worked.

See the very basic example below:

### Current State
```json
{
  "myList": ["red", "blue", "green"]
}
```

Both clients legitimately read the starting state as above.

### Client A

Wants to add "yellow" to the list, so the target state is
```json
{
  "myList": ["red", "blue", "green", "yellow"]
}
```

### Client B

Wants to add "purple" to the list, so the target state is
```json
{
  "myList": ["red", "blue", "green", "purple"]
}
```

If they both run their updates in a similar timeframe, both will succeed but either "yellow" or "purple" will be 
added to the list, not both.

I would like a way for a client to request for an update to be rejected if the initial state differs from its 
assumptions, i.e. only update the state IFF the starting state is still the same.

## Motivation

> Why are we doing this? What use cases does it support? What is the expected outcome?
>
> Please focus on explaining the motivation so that if this RFC is not accepted, the motivation could be used to develop
> alternative solutions. In other words, enumerate the constraints you are trying to solve without coupling them too
> closely to the solution you have in mind.

## Requirements

> What specific requirements does your design need to meet? This should ideally be a bulleted list of items you wish
> to achieve with your design. This can help everyone involved (including yourself!) make sure your design is robust
> enough to meet these requirements.
>
> Once everyone has agreed upon the set of requirements for your design, we can use this list to review the detailed
> design.

### Extensibility

> Please also call out extensibility requirements. Is this proposal meant to be extended in the future? Are you adding
> a new API or set of models that others can build on in later? Please list these concerns here as well.

## Non-Requirements

> Call out things you don't want to discuss in detail during this review here, to help focus the conversation. This can
> include things you may build in the future based off this design, but don't wish to discuss in detail, in which case
> it may also be wise to explicitly list that extensibility in your design is a requirement.
>
> This list can be high level and not detailed. It is to help focus the conversation on what you want to focus on.

## Detailed design

> This is the bulk of the RFC.

> Explain the design in enough detail for somebody familiar with the framework to understand, and for somebody familiar
> with the implementation to implement. This should get into specifics and corner-cases, and include examples of how the
> feature is used. Any new terminology should be defined here.

## How we teach this

> What names and terminology work best for these concepts and why? How is this idea best presented? As a continuation
> of existing DataHub patterns, or as a wholly new one?

> What audience or audiences would be impacted by this change? Just DataHub backend developers? Frontend developers?
> Users of the DataHub application itself?

> Would the acceptance of this proposal mean the DataHub guides must be re-organized or altered? Does it change how
> DataHub is taught to new users at any level?

> How should this feature be introduced and taught to existing audiences?

## Drawbacks

> Why should we *not* do this? Please consider the impact on teaching DataHub, on the integration of this feature with
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