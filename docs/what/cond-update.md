# What is a conditional update?

A conditional update is a way to support the client side transactions during insert and update operations over an Aspect. Using the conditional update the
user has the possibility to execute his update request only if the Aspect is not changed since the user has read the Aspect data.

The conditional update parameter should be passed via "If-Unmodified-Since" header in the following format:
```
urn1+AspectName1=createdOn1;urn2+AspectName2=createdOn2;...;urnN+AspectNameN=createdOnN
```
Explanation of the provided format:
1. **Urn** example `urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v2,PROD)`
2. **AspectName** example `globalTags`
3. **createdOn** is the UNIX epoch in milliseconds when the Urn+aspectName combination is edited for the last time. For simplicity, we do not support the
   default `If-Unmodified-Since` [date time format](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Unmodified-Since). We only support the UNIX
   timestamp.

Read more about [conditional requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests#avoiding_the_lost_update_problem_with_optimistic_locking).

## Conditional Update Scenarios

There are 3 different use cases.

### Unconditional insert/update (default behaviour)

In this case we should not pass the `urn+aspectName` in `If-Unmodified-Since` header, or we could pass the `null` value. The insert/update request will be
executed without any condition checked before it. If two clients try to update the same aspect in quick succession, the latest received update will
overwrite any previous state.

### Insert the aspect only if it doesn't already exist

In this case we should pass `0` for the `urn+aspectName` in `If-Unmodified-Since` header. The meaning of 0 value is "Insert data only if urn+aspectName
don't exist in database".

### Update the aspect only if it has not been modified since our last read

In this case we should pass the `createdOn` value we have previously read for the `urn+aspectName` in `If-Unmodified-Since` header. The meaning of value
greater than 0 is "Update data only if urn+aspectName is not changed since aspect data is read".

## Responses

If using conditional requests, the client should expect and be able to react to the following status codes

### Success (200)

The update was successful.

### Precondition Failed (412)

The update failed as the precondition did not match. See details in the response or the GMS logs.