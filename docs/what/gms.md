# What is Generalized Metadata Service (GMS)?

Metadata for [entities](entity.md) [onboarded](../modeling/metadata-model.md) to [GMA](gma.md) is served through microservices known as Generalized Metadata Service (GMS). GMS typically provides a [Rest.li](http://rest.li) API and must access the metadata using [GMA DAOs](../architecture/metadata-serving.md). 

GMA is designed to support a distributed fleet of GMS, each serving a subset of the [GMA graph](graph.md). However, for simplicity we include a single centralized GMS that serves all entities.
