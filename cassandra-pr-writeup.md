# Cassandra PR

This PR implements a new EntityService (DatastaxEntityService) and its associated DAO, aspect classes etc. in order to support Datastax's [Cassandra](https://en.wikipedia.org/wiki/Apache_Cassandra) for DataHub's [document store](https://datahubproject.io/docs/architecture/metadata-serving#metadata-query-serving) component.

The implementation (Cassandra Datastax or Ebean SQL) can be set with the `DAO_SERVICE_LAYER` environment variable (e.g. in `/docker/datahub-gms/env/docker.cassandra.env`). The other corresponding DB configs that can be set are:

* `DATASTAX_DATASOURCE_USERNAME` (e.g. `cassandra`)
* `DATASTAX_DATASOURCE_PASSWORD` (e.g. `cassandra`)
* `DATASTAX_HOSTS` (e.g. `localhost`)
* `DATASTAX_PORT` (e.g. `9042`)
* `DATASTAX_DATACENTER` (e.g. `datacenter1`)
* `DATASTAX_KEYSPACE` (e.g. `datahubpoc`)
* `DATASTAX_USESSL` (e.g. `false`)

## Motivation

While evaluating DataHub for internal use, we identified that it was important for the query serving to be able to be horizontally scalable, as we've come up with scaling issues for one of our use cases relying on a traditional SQL store that DataHub would replace. Along with the requirement that the database should be highly available and with native support for multi DC replication, this led us to evaluate the use of Cassandra, a horizontally scalable database as part of our data catalog solution.

## Technical Notes

### Overview

* We use the Cassandra library from [testcontainers](https://www.testcontainers.org/modules/databases/cassandra/) to provide an out-of-process Cassandra instance for integration tests (`DatastaxEntityServiceTest.java`)
* Most of the architecture (and tests) are inspired from the `EbeanEntityService` and the other Ebean implementations
* Right now, the DatastaxAspect is a dumb class. There is support for DAO (https://docs.datastax.com/en/developer/java-driver/4.4/manual/mapper/daos/update/) class annotations which may be more idiomatic
* Some nuances to queries and operations in a distributed database (e.g. see Paging and Sorting), are discussed below

### Conditional Updates vs Transactions

Cassandra does not have ACID transactions; it does however have [conditional updates](https://docs.datastax.com/en/developer/java-driver/4.4/manual/mapper/daos/update/) which we are likely to make use of soon in order to support state transitions being serialisable. More discussion around this and the current ebean implementation of ingesting and updating entities will be added later.

### SCSI Support

Since Cassandra does not support `JOIN`s, it would be extra work to implement SCSI without adding more columns into the `metadata_aspect` table, although it's our understanding that if this feature is about to be dropped, hopefully this wouldn't be a long-term issue.

### Paging and Sorting

*IN PROGRESS*

*This section will be expanded upon/rewritten once the frontend is built correctly*

This mostly relates to the reimplementing of the `listLatestAspects` method. A new column in the DB has been added for the entity type (e.g. `corpuser`), populated upon insertion. The alternative to this would be to use a [SASI](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/useSASIIndex.html), although there are some sources suggesting that it won't be any more performant (https://www.scnsoft.com/blog/cassandra-performance).

Furthermore, the sorting order (ascending by URN) feature has been dropped as it would have to be done in memory (since URN is used as the partition key), and the test condition has been relaxed in `testListLatestAspects`.

Cassandra's paging has been implemented using the [OffsetPager](https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/paging/); there does not seem to be a performant way of getting the total number of matches back (e.g. https://www.datastax.com/blog/running-count-expensive-cassandra). Our use case isn't heavily focused on the frontend so there hasn't been much effort expended in optimising this.