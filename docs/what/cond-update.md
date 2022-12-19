# What is a conditional update?

A conditional update is a way to support the client side 
transactions during insert and update operations over an Aspect. Using the
conditional update the user has the possibility to execute his update request only 
if the Aspect is not changed since the user has read the Aspect data.

The conditional update parameter should be passed via "If-Unmodified-Since" header
in the following format:
```
"urn1+AspectName1=createdOn1;urn2+AspectName2=createdOn2;...;urnN+AspectNameN=createdOnN"
```
Explanation of the provided format:
1. **Urn** example "urn:li:dataset:(urn:li:dataPlatform:mysql,datahub.metadata_aspect_v2,PROD)"
2. **Aspect Name** example "globalTags"
3. **createOn** is the timestamp when the Urn+aspectName 
combination is edited for the last time. For simplicity, 
we do not support the default "If-Unmodified-Since" date 
time format https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Unmodified-Since
but instead, we only support the UNIX timestamp.

More about condition requests could be found here https://developer.mozilla.org/en-US/docs/Web/HTTP/Conditional_requests#avoiding_the_lost_update_problem_with_optimistic_locking

There are 3 different use cases:
1. **We do not want to apply any condition while inserting/updating the aspect.**

In this case we should not pass the urn+aspectName in "If-Unmodified-Since" header,
or we could pass the null value. The insert/update request will be executed without
any condition checked before it.

2. **We want to insert the aspect only if the aspect doesn't exist.**

In this case we should pass 0 value for the urn+aspectName in 
"If-Unmodified-Since" header. The meaning of 0 value is
"Insert data only if urn+aspectName don't exist in database".

3. **We want to update the aspect only if the aspect is not modified since our last read.**

In this case we should pass createdOn value we have previously read for the 
urn+aspectName in "If-Unmodified-Since" header. The meaning of value greater than 0
is "Update data only if urn+aspectName is not changed since aspect data is read".
