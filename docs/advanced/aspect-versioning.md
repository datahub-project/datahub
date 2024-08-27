# Aspect Versioning
As each version of [metadata aspect](../what/aspect.md) is immutable, any update to an existing aspect results in the creation of a new version. Typically one would expect the version number increases sequentially with the largest version number being the latest version, i.e. `v1` (oldest), `v2` (second oldest), ..., `vN` (latest). However, this approach results in major challenges in both rest.li modeling & transaction isolation and therefore requires a rethinking.

## Rest.li Modeling
As it's common to create dedicated rest.li sub-resources for a specific aspect, e.g. `/datasets/{datasetKey}/ownership`, the concept of versions become an interesting modeling question. Should the sub-resource be a [Simple](https://linkedin.github.io/rest.li/modeling/modeling#simple) or a [Collection](https://linkedin.github.io/rest.li/modeling/modeling#collection) type?

If Simple, the [GET](https://linkedin.github.io/rest.li/user_guide/restli_server#get) method is expected to return the latest version, and the only way to retrieve non-latest versions is through a custom [ACTION](https://linkedin.github.io/rest.li/user_guide/restli_server#action) method, which is going against the [REST](https://en.wikipedia.org/wiki/Representational_state_transfer) principle. As a result, a Simple sub-resource doesn't seem to a be a good fit.

If Collection, the version number naturally becomes the key so it's easy to retrieve specific version number using the typical GET method. It's also easy to list all versions using the standard [GET_ALL](https://linkedin.github.io/rest.li/user_guide/restli_server#get_all) method or get a set of versions via [BATCH_GET](https://linkedin.github.io/rest.li/user_guide/restli_server#batch_get). However, Collection resources don't support a simple way to get the latest/largest key directly. To achieve that, one must do one of the following

  - a GET_ALL (assuming descending key order) with a page size of 1
  - a [FINDER](https://linkedin.github.io/rest.li/user_guide/restli_server#finder) with special parameters and a page size of 1
  - a custom ACTION method again
  
None of these options seems like a natural way to ask for the latest version of an aspect, which is one of the most common use cases.

## Transaction Isolation
[Transaction isolation](https://en.wikipedia.org/wiki/Isolation_(database_systems)) is a complex topic so make sure to familiarize yourself with the basics first. 

To support concurrent update of a metadata aspect, the following pseudo DB operations must be run in a single transaction,
```
1. Retrieve the current max version (Vmax)
2. Write the new value as (Vmax + 1)
```
Operation 1 above can easily suffer from [Phantom Reads](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads). This subsequently leads to Operation 2 computing the incorrect version and thus overwrites an existing version instead of creating a new one.

One way to solve this is by enforcing [Serializable](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable) isolation level in DB at the [cost of performance](https://logicalread.com/optimize-mysql-perf-part-2-mc13/#.XjxSRSlKh1N). In reality, very few DB even supports this level of isolation, especially for distributed document stores. It's more common to support [Repeatable Reads](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Repeatable_reads) or [Read Committed](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_committed) isolation levels—sadly neither would help in this case.

Another possible solution is to transactionally keep track of `Vmax` directly in a separate table to avoid the need to compute that through a `select` (thus prevent Phantom Reads). However, cross-table/document/entity transaction is not a feature supported by all distributed document stores, which precludes this as a generalized solution.

## Solution: Version 0
The solution to both challenges turns out to be surprisingly simple. Instead of using a "floating" version number to represent the latest version, one can use a "fixed/sentinel" version number instead. In this case we choose Version 0 as we want all non-latest versions to still keep increasing sequentially. In other words, it'd be `v0` (latest), `v1` (oldest), `v2` (second oldest), etc. Alternatively, you can also simply view all the non-zero versions as an audit trail.

Let's examine how Version 0 can solve the aforementioned challenges.

### Rest.li Modeling
With Version 0, getting the latest version becomes calling the GET method of a Collection aspect-specific sub-resource with a deterministic key, e.g. `/datasets/{datasetkey}/ownership/0`, which is a lot more natural than using GET_ALL or FINDER.

### Transaction Isolation
The pseudo DB operations change to the following transaction block with version 0,
```
1. Retrieve v0 of the aspect
2. Retrieve the current max version (Vmax)
3. Write the old value back as (Vmax + 1)
4. Write the new value back as v0
```
While Operation 2 still suffers from potential Phantom Reads and thus corrupting existing version in Operation 3, Repeatable Reads isolation level will ensure that the transaction fails due to [Lost Update](https://codingsight.com/the-lost-update-problem-in-concurrent-transactions/) detected in Operation 4. Note that this happens to also be the [default isolation level](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html) for InnoDB in MySQL.
