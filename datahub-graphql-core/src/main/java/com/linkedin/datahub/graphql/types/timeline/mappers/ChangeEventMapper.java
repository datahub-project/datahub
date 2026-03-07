package com.linkedin.datahub.graphql.types.timeline.mappers;

import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

// Class for converting ChangeTransactions received from the Timeline API to SchemaFieldBlame
// structs for every schema
// at every semantic version.
@Slf4j
public class ChangeEventMapper {

  // Backend ChangeCategory -> GraphQL ChangeCategoryType mapping.
  // Needed because backend uses OWNER while GraphQL uses OWNERSHIP.
  private static final Map<ChangeCategory, ChangeCategoryType> CATEGORY_MAP =
      Map.of(
          ChangeCategory.DOCUMENTATION, ChangeCategoryType.DOCUMENTATION,
          ChangeCategory.GLOSSARY_TERM, ChangeCategoryType.GLOSSARY_TERM,
          ChangeCategory.OWNER, ChangeCategoryType.OWNERSHIP,
          ChangeCategory.TECHNICAL_SCHEMA, ChangeCategoryType.TECHNICAL_SCHEMA,
          ChangeCategory.TAG, ChangeCategoryType.TAG,
          ChangeCategory.PARENT, ChangeCategoryType.PARENT,
          ChangeCategory.RELATED_ENTITIES, ChangeCategoryType.RELATED_ENTITIES,
          ChangeCategory.DOMAIN, ChangeCategoryType.DOMAIN,
          ChangeCategory.STRUCTURED_PROPERTY, ChangeCategoryType.STRUCTURED_PROPERTY,
          ChangeCategory.APPLICATION, ChangeCategoryType.APPLICATION);

  @Nullable
  static ChangeCategoryType mapCategory(@Nullable ChangeCategory backendCategory) {
    if (backendCategory == null) {
      return null;
    }
    ChangeCategoryType mapped = CATEGORY_MAP.get(backendCategory);
    if (mapped == null) {
      log.warn(
          "No GraphQL ChangeCategoryType mapping for backend category: {}. "
              + "Add it to ChangeEventMapper.CATEGORY_MAP.",
          backendCategory);
    }
    return mapped;
  }

  public static com.linkedin.datahub.graphql.generated.ChangeEvent map(
      @Nonnull final ChangeEvent incomingChangeEvent) {
    final com.linkedin.datahub.graphql.generated.ChangeEvent result =
        new com.linkedin.datahub.graphql.generated.ChangeEvent();

    if (incomingChangeEvent.getAuditStamp() != null) {
      result.setAuditStamp(AuditStampMapper.map(null, incomingChangeEvent.getAuditStamp()));
    }
    if (incomingChangeEvent.getEntityUrn() == null) {
      log.warn(
          "ChangeEvent has null entityUrn for category={}, operation={}. "
              + "This may indicate a data quality issue in the change event generator.",
          incomingChangeEvent.getCategory(),
          incomingChangeEvent.getOperation());
    }
    result.setUrn(
        incomingChangeEvent.getEntityUrn() != null ? incomingChangeEvent.getEntityUrn() : "empty");
    result.setCategory(mapCategory(incomingChangeEvent.getCategory()));
    result.setDescription(incomingChangeEvent.getDescription());
    result.setModifier(incomingChangeEvent.getModifier());
    if (incomingChangeEvent.getOperation() != null) {
      result.setOperation(
          ChangeOperationType.valueOf(incomingChangeEvent.getOperation().toString()));
    }
    if (incomingChangeEvent.getParameters() != null) {
      result.setParameters(
          incomingChangeEvent.getParameters().entrySet().stream()
              .map(
                  entry -> {
                    final com.linkedin.datahub.graphql.generated.TimelineParameterEntry
                        changeParameter =
                            new com.linkedin.datahub.graphql.generated.TimelineParameterEntry();
                    changeParameter.setKey(entry.getKey());
                    changeParameter.setValue(entry.getValue().toString());
                    return changeParameter;
                  })
              .collect(Collectors.toList()));
    }

    return result;
  }

  private ChangeEventMapper() {}
}
