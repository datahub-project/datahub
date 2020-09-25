# Metadata Ingestion

This directory contains example apps for ingesting data into DataHub.

You are more than welcome to use these examples directly, or use them as a reference for you own jobs.

See the READMEs of each example for more information on each.

### Common themes

All these examples ingest by firing MetadataChangeEvent Kafka events. They do not ingest directly into DataHub, though
this is possible. Instead, the mce-consumer-job should be running, listening for these events, and perform the ingestion
for us.

### A note on languages

We initially wrote these examples in Python (they still exist in `contrib`). The idea was that these were very small
example scripts, that should've been easy to use. However, upon reflection, not all developers are familiar with Python,
and the lack of types can hinder development. So the decision was made to port the examples to Java.

You're more than welcome to extrapolate these examples into whatever languages you like. At LinkedIn, we primarily use
Java.

### Ingestion at LinkedIn

It is worth noting that we do not use any of these examples directly (in Java, Python, or anything else) at LinkedIn. We
have several different pipelines for ingesting data; it all depends on the source.

- Some pipelines are based off other Kafka events, where we'll transform some existing Kafka event to a metadata event.
  - For example, we get Kafka events hive changes. We make MCEs out of those hive events to ingest hive data.
- For others, we've directly instrumented existing pipelines / apps / jobs to also emit metadata events.
- For others still, we've created a series offline jobs to ingest data.
  - For example, we have an Azkaban job to process our HDFS datasets.

For some sources of data one of these example scripts may work fine. For others, it may make more sense to have some
custom logic, like the above list. Namely, all these examples today are one-off (they run, fire events, and then stop),
you may wish to build continuous ingestion pipelines instead.
