- Start Date: 2022-09-02
- RFC PR: https://github.com/datahub-project/datahub/pull/5818
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

We wish to avoid losing data silently when two clients make updates to the same aspect. This can be quite likely in 
an event-driven world driving downstream operations on Datasets. We should offer clients the chance to conditionally 
update an aspect on the basis that what they recently observed is still the case and signal to the client if the 
state changed before the update was possible. Essentially we need a long "compare-and-swap" operation 
for atomic updates.

## Requirements

> What specific requirements does your design need to meet? This should ideally be a bulleted list of items you wish
> to achieve with your design. This can help everyone involved (including yourself!) make sure your design is robust
> enough to meet these requirements.
>
> Once everyone has agreed upon the set of requirements for your design, we can use this list to review the detailed
> design.

* A client needs to be able to identify the unique state or version of a given aspect when querying for it.
* The client needs to be able to reference the same state to a GMS "update" endpoint if it wants to ensure the 
  aspect has not changed between fetching it and mutating it.
* A client could include multiple aspects in its precondition state, in case one update relies on the state of many 
  other aspects.

### Extensibility

> Please also call out extensibility requirements. Is this proposal meant to be extended in the future? Are you adding
> a new API or set of models that others can build on in later? Please list these concerns here as well.

1. The proposal could be extended to include a list of aspect versions which must hold true in order for a single 
   aspect update to occur.
2. We could extend to include the state version in a batch update, though this may be complicated if handling 
   multiple updates to the same aspect in a single batch.

## Non-Requirements

It's not important for us to discuss complex prerequisites here. The only thing we need to enforce is the state of 
an aspect has not changed when making an update to it.

There are some potential spin-offs of this design which could involve writing some kind of PATCH update where a client 
supplies a diff instead of a complete new state, but this is out of scope of this particular RFC.

## Detailed design

> This is the bulk of the RFC.

> Explain the design in enough detail for somebody familiar with the framework to understand, and for somebody familiar
> with the implementation to implement. This should get into specifics and corner-cases, and include examples of how the
> feature is used. Any new terminology should be defined here.

There are many ways to achieve this, but most of them involve DataHub aspects having some kind of consistent 
versioning as currently the LATEST version of an aspect is version zero and is a mutable placeholder for a "real" 
fixed version.

TODO: Different design options to be enumerated.

## How we teach this

> What names and terminology work best for these concepts and why? How is this idea best presented? As a continuation
> of existing DataHub patterns, or as a wholly new one?

> What audience or audiences would be impacted by this change? Just DataHub backend developers? Frontend developers?
> Users of the DataHub application itself?

> Would the acceptance of this proposal mean the DataHub guides must be re-organized or altered? Does it change how
> DataHub is taught to new users at any level?

> How should this feature be introduced and taught to existing audiences?

We would have two potential additions to terminology:
1. Previous state: The version(s) of aspects required in order for a GM update to succeed.
2. Preconditions: A wider set of assertions which must be true in order for a GM update to succeed.

We don't really cover "Preconditions" in this RFC, and in fact I argue we should never do, as this leaks business 
knowledge into the GMS code. I would therefore use "Previous State" to describe the aspect versions required for 
updates.

### Documentation

This should be added as an extra section of the following:
* REST API examples
* Java/Python REST Emitter
* GraphQL mutations?

## Drawbacks

> Why should we *not* do this? Please consider the impact on teaching DataHub, on the integration of this feature with
> other existing and planned features, on the impact of the API churn on existing apps, etc.

> There are tradeoffs to choosing any path, please attempt to identify them here.

There are two major reasons this will be difficult to achieve: Aspect Versioning and Performance.

### Aspect Versioning

Currently, [aspect versioning](/docs/advanced/aspect-versioning.md) uses 0 as the latest aspect version. We would 
have to find a way to get around this or provide a different, deterministic identifier for a specific aspect version 
if we aren't able to reference a single fixed integer for the aspect versions.

### Performance

There are some significant performance drawbacks for most of these designs, as they will involve an extra hop between 
the client and DataHub. Things will get even worse if the aspect updates are under heavy contention.

#### Performance under contention

This approach would suffer a lot under high contention. Let's say:
* the time sending a request from client to gms and receiving response is `x`
* the time taken to manipulate read to create a new write request at the client is `y` (assume constant for simplicity)
* a constant transaction/LWT overhead of `z` (LWT if using Cassandra)

Under a contention of n clients sending 1 request targeting the same aspect, we would see:
* 1 client would take `2x + y + z` time
* 1 client would take `4x + 2y + 2z` time
* ...
* nth concurrent client would take 2nx + ny + nz time

This is assuming a client is just making one update to the aspect. If an aspect is being used in some complex 
communication chain this could take a very long time.

## Alternatives

> What other designs have been considered? What is the impact of not doing this?

> This section could also include prior art, that is, how other frameworks in the same domain have solved this problem.

### Modelling

Alternatives involve cleverly modelling aspects so they are only ever upserted without a previous state in mind, but 
I haven't worked out how to achieve this for our requirements given the standard entity and aspect model available.

### Locking

If it were possible to lock an aspect for updating, then we could try this, although this would also lead to 
slowness and contention of a different kind and we'd have to handle time-outs for locks which are never released, 
for example.

### Patch Language

If the patch language were available, it might be possible to update collections in such a way that it's not 
necessary for clients to know about the previous state of an aspect.

MongoDB offers an update mode which allows clients to [add items to a set](https://mongodb.github.io/mongo-java-driver/4.7/apidocs/mongodb-driver-core/com/mongodb/client/model/Updates.html#addToSet(java.lang.String,TItem)),
for example. This would avoid the need to go back to the client to ask them to construct the final state of a 
collection.

## Rollout / Adoption Strategy

> If we implemented this proposal, how will existing users / developers adopt it? Is it a breaking change? Can we write
> automatic refactoring / migration tools? Can we provide a runtime adapter library for the original API it replaces? 

This rollout would be done as either a new API endpoint or an optional additional parameter to an existing API, so 
existing APIs will not be broken. Once available, clients may opt in to use the new features.

## Future Work

> Describe any future projects, at a very high level, that will build off this proposal. This does not need to be
> exhaustive, nor does it need to be anything you work on. It just helps reviewers see how this can be used in the
> future, so they can help ensure your design is flexible enough.

The capabilities built here would allow us to implement more parts of GMS than just UPSERT change types, as we could 
guarantee the current state before making any changes.

## Unresolved questions

> Optional, but suggested for first drafts. What parts of the design are still TBD?

I'm not as familiar with the GMS code as I could be, so it's unclear to me exactly where we'd end up making code 
changes. I know the solution needs to be agnostic to whichever backing store is being used.

TODO: Still need to do a deep technical proposal.