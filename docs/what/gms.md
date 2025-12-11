<!--
  ~ © Crown Copyright 2025. This work has been developed by the National Digital Twin Programme and is legally attributed to the Department for Business and Trade (UK) as the governing entity.
  ~
  ~ Licensed under the Open Government Licence v3.0.
-->

# What is Generalized Metadata Service (GMS)?

Metadata for [entities](entity.md) [onboarded](../modeling/metadata-model.md) to [GMA](gma.md) is served through microservices known as Generalized Metadata Service (GMS). GMS typically provides a [Rest.li](http://rest.li) API and must access the metadata using [GMA DAOs](../architecture/metadata-serving.md).

GMA is designed to support a distributed fleet of GMS, each serving a subset of the [GMA graph](graph.md). However, for simplicity we include a single centralized GMS that serves all entities.
