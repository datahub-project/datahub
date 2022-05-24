# What is Generalized Metadata Service (GMS)?

Metadata for [entities](entity.md) [onboarded](../modeling/metadata-model.md) to [GMA](gma.md) is served through microservices known as Generalized Metadata Service (GMS). GMS typically provides a [Rest.li](http://rest.li) API and must access the metadata using [GMA DAOs](../architecture/metadata-serving.md). 

While a GMS is completely free to define its public APIs, we do provide a list of [resource base classes](https://github.com/datahub-project/datahub-gma/tree/master/restli-resources/src/main/java/com/linkedin/metadata/restli) to leverage for common patterns.

GMA is designed to support a distributed fleet of GMS, each serving a subset of the [GMA graph](graph.md). However, for simplicity we include a single centralized GMS ([datahub-gms](../../gms)) that serves all entities.
