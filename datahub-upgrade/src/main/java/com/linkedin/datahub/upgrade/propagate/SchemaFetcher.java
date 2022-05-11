package com.linkedin.datahub.upgrade.propagate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
class SchemaFetcher {
  private final EntityService _entityService;

  private static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(Constants.SCHEMA_METADATA_ASPECT_NAME, Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME);

  Map<Urn, SchemaWithDetails> fetchSchema(Set<Urn> urns) {
    try {
      return _entityService.getEntitiesV2(Constants.DATASET_ENTITY_NAME, urns, ASPECTS_TO_FETCH)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, entry -> transformResponse(entry.getValue())));
    } catch (URISyntaxException e) {
      log.error("Error while fetching schema for a batch of urns", e);
      return Collections.emptyMap();
    }
  }

  private SchemaWithDetails transformResponse(EntityResponse entityResponse) {
    if (!entityResponse.getAspects().containsKey(Constants.SCHEMA_METADATA_ASPECT_NAME)) {
      return SchemaWithDetails.EMPTY;
    }
    SchemaMetadata schemaMetadata =
        new SchemaMetadata(entityResponse.getAspects().get(Constants.SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    Map<String, SchemaDetails> schemaWithDetails =
        schemaMetadata.getFields().stream().collect(Collectors.toMap(SchemaField::getFieldPath, this::fetchDetails));

    if (!entityResponse.getAspects().containsKey(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)) {
      return new SchemaWithDetails(schemaWithDetails);
    }
    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata(
        entityResponse.getAspects().get(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME).getValue().data());
    for (EditableSchemaFieldInfo editableSchemaFieldInfo : editableSchemaMetadata.getEditableSchemaFieldInfo()) {
      String fieldPath = editableSchemaFieldInfo.getFieldPath();
      if (schemaWithDetails.containsKey(fieldPath)) {
        schemaWithDetails.put(fieldPath, mergeDetails(schemaWithDetails.get(fieldPath), editableSchemaFieldInfo));
      }
    }
    return new SchemaWithDetails(schemaWithDetails);
  }

  private SchemaDetails fetchDetails(SchemaField schemaField) {
    GlossaryTerms glossaryTerms = schemaField.getGlossaryTerms();
    List<Urn> termList;
    if (glossaryTerms != null) {
      termList = glossaryTerms.getTerms().stream().map(GlossaryTermAssociation::getUrn).collect(Collectors.toList());
    } else {
      termList = Collections.emptyList();
    }
    return new SchemaDetails(termList);
  }

  private SchemaDetails mergeDetails(SchemaDetails original, EditableSchemaFieldInfo editableSchemaFieldInfo) {
    GlossaryTerms glossaryTerms = editableSchemaFieldInfo.getGlossaryTerms();
    List<Urn> termList = original.getGlossaryTerms();
    if (glossaryTerms != null) {
      termList = Streams.concat(original.getGlossaryTerms().stream(),
          glossaryTerms.getTerms().stream().map(GlossaryTermAssociation::getUrn)).collect(Collectors.toList());
    }
    return new SchemaDetails(termList);
  }

  @Value
  static class SchemaWithDetails {
    Map<String, SchemaDetails> fieldPathToDetails;

    private static final SchemaWithDetails EMPTY = new SchemaWithDetails(Collections.emptyMap());
  }

  @Value
  static class SchemaDetails {
    List<Urn> glossaryTerms;
  }
}
