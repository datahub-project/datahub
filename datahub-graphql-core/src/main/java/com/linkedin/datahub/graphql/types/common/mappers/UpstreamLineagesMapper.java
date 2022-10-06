package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.SchemaFieldRef;
import com.linkedin.dataset.FineGrainedLineage;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class UpstreamLineagesMapper {

  public static final UpstreamLineagesMapper INSTANCE = new UpstreamLineagesMapper();

  public static List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> map(@Nonnull final com.linkedin.dataset.UpstreamLineage upstreamLineage) {
    return INSTANCE.apply(upstreamLineage);
  }

  public List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> apply(@Nonnull final com.linkedin.dataset.UpstreamLineage upstreamLineage) {
    final List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> result = new ArrayList<>();
    if (!upstreamLineage.hasFineGrainedLineages()) {
      return result;
    }

    for (FineGrainedLineage fineGrainedLineage : upstreamLineage.getFineGrainedLineages()) {
      com.linkedin.datahub.graphql.generated.FineGrainedLineage resultEntry = new com.linkedin.datahub.graphql.generated.FineGrainedLineage();
      resultEntry.setUpstreams(fineGrainedLineage.getUpstreams().stream()
          .filter(entry -> entry.getEntityType().equals("schemaField"))
          .map(entry -> mapDatasetSchemaField(entry)).collect(
          Collectors.toList()));
      resultEntry.setDownstreams(fineGrainedLineage.getDownstreams().stream()
          .filter(entry -> entry.getEntityType().equals("schemaField"))
          .map(entry ->  mapDatasetSchemaField(entry)).collect(
          Collectors.toList()));
      result.add(resultEntry);
    }
    return result;
  }

  private static SchemaFieldRef mapDatasetSchemaField(final Urn schemaFieldUrn) {
    return new SchemaFieldRef(schemaFieldUrn.getEntityKey().get(0), schemaFieldUrn.getEntityKey().get(1));
  }
}
