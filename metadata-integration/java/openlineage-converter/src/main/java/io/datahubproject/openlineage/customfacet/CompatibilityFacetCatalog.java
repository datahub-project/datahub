package io.datahubproject.openlineage.customfacet;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class CompatibilityFacetCatalog {
  public enum AttachmentPoint {
    RUN,
    JOB,
    DATASET,
    INPUT_DATASET,
    OUTPUT_DATASET
  }

  public enum SupportStatus {
    ACTIVE,
    DEPRECATED
  }

  private static final URI RUN_FACET_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunFacet");
  private static final URI BASE_FACET_SCHEMA =
      URI.create("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/BaseFacet");

  private static final ProducerUriPattern SPARK_PRODUCER =
      new ProducerUriPattern(
          "https",
          "github.com",
          Pattern.compile(
              "^/OpenLineage/OpenLineage/(?:tree|blob)/[^/]+/integration/spark(?:/.*)?$"));
  private static final ProducerUriPattern OPENLINEAGE_AIRFLOW_PRODUCER =
      new ProducerUriPattern(
          "https",
          "github.com",
          Pattern.compile(
              "^/OpenLineage/OpenLineage/(?:tree|blob)/[^/]+/integration/airflow(?:/.*)?$"));
  private static final ProducerUriPattern LEGACY_OPENLINEAGE_AIRFLOW_PRODUCER =
      new ProducerUriPattern(
          "https",
          "github.com",
          Pattern.compile("^/OpenLineage/OpenLineage/integration/airflow/?$"));
  private static final ProducerUriPattern APACHE_AIRFLOW_PRODUCER =
      new ProducerUriPattern(
          "https",
          "github.com",
          Pattern.compile("^/apache/airflow/tree/providers-openlineage/[^/]+/?$"));

  private static final Set<URI> AIRFLOW_SCHEMAS = Set.of(RUN_FACET_SCHEMA, BASE_FACET_SCHEMA);
  private static final Set<URI> SPARK_SCHEMAS = Set.of(RUN_FACET_SCHEMA);
  private static final Set<ProducerUriPattern> AIRFLOW_PRODUCERS =
      Set.of(
          OPENLINEAGE_AIRFLOW_PRODUCER,
          LEGACY_OPENLINEAGE_AIRFLOW_PRODUCER,
          APACHE_AIRFLOW_PRODUCER);
  private static final Set<ProducerUriPattern> SPARK_PRODUCERS = Set.of(SPARK_PRODUCER);

  private static final List<CompatibilityFacetContract> CONTRACTS =
      List.of(
          contract("airflow", SupportStatus.ACTIVE, AIRFLOW_SCHEMAS, AIRFLOW_PRODUCERS),
          contract("spark_jobDetails", SupportStatus.ACTIVE, SPARK_SCHEMAS, SPARK_PRODUCERS),
          contract("spark_properties", SupportStatus.ACTIVE, SPARK_SCHEMAS, SPARK_PRODUCERS),
          contract("spark.logicalPlan", SupportStatus.ACTIVE, SPARK_SCHEMAS, SPARK_PRODUCERS),
          contract("spark_version", SupportStatus.DEPRECATED, SPARK_SCHEMAS, SPARK_PRODUCERS),
          contract(
              "unknownSourceAttribute",
              SupportStatus.DEPRECATED,
              AIRFLOW_SCHEMAS,
              AIRFLOW_PRODUCERS));

  private CompatibilityFacetCatalog() {}

  public static List<CompatibilityFacetContract> contracts() {
    return CONTRACTS;
  }

  private static CompatibilityFacetContract contract(
      String key, SupportStatus status, Set<URI> schemaUrls, Set<ProducerUriPattern> producers) {
    return new CompatibilityFacetContract(AttachmentPoint.RUN, key, status, schemaUrls, producers);
  }
}
