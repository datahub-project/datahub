package com.linkedin.datahub.graphql.types.common.mappers;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.FineGrainedLineage;
import com.linkedin.datahub.graphql.generated.SchemaFieldRef;
import com.linkedin.dataset.FineGrainedLineageArray;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class FineGrainedLineagesMapper {

  public static final FineGrainedLineagesMapper INSTANCE = new FineGrainedLineagesMapper();

  public static List<FineGrainedLineage> map(
      @Nonnull final FineGrainedLineageArray fineGrainedLineages) {
    return INSTANCE.apply(fineGrainedLineages);
  }

  public List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> apply(
      @Nonnull final FineGrainedLineageArray fineGrainedLineages) {
    final List<com.linkedin.datahub.graphql.generated.FineGrainedLineage> result =
        new ArrayList<>();
    if (fineGrainedLineages.size() == 0) {
      return result;
    }

    for (com.linkedin.dataset.FineGrainedLineage fineGrainedLineage : fineGrainedLineages) {
      com.linkedin.datahub.graphql.generated.FineGrainedLineage resultEntry =
          new com.linkedin.datahub.graphql.generated.FineGrainedLineage();
      if (fineGrainedLineage.hasUpstreams()) {
        resultEntry.setUpstreams(
            fineGrainedLineage.getUpstreams().stream()
                .filter(entry -> entry.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME))
                .map(FineGrainedLineagesMapper::mapDatasetSchemaField)
                .collect(Collectors.toList()));
      }
      if (fineGrainedLineage.hasDownstreams()) {
        resultEntry.setDownstreams(
            fineGrainedLineage.getDownstreams().stream()
                .filter(entry -> entry.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME))
                .map(FineGrainedLineagesMapper::mapDatasetSchemaField)
                .collect(Collectors.toList()));
      }
      result.add(resultEntry);
    }
    return result;
  }

  private static SchemaFieldRef mapDatasetSchemaField(final Urn schemaFieldUrn) {
    return new SchemaFieldRef(
        schemaFieldUrn.getEntityKey().get(0), schemaFieldUrn.getEntityKey().get(1));
  }
}
