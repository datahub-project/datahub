package com.linkedin.datahub.graphql.types.timeline.mappers;

import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

// Class for converting ChangeTransactions received from the Timeline API to SchemaFieldBlame
// structs for every schema
// at every semantic version.
@Slf4j
public class ChangeEventMapper {

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
    // ChangeCategory and ChangeOperation are server-side Java enums, not stored values, so a
    // rollback can't produce one this version lacks: kept strict, written this way for the lint.
    result.setCategory(
        Enum.valueOf(ChangeCategoryType.class, incomingChangeEvent.getCategory().toString()));
    result.setDescription(incomingChangeEvent.getDescription());
    result.setModifier(incomingChangeEvent.getModifier());
    if (incomingChangeEvent.getOperation() != null) {
      result.setOperation(
          Enum.valueOf(ChangeOperationType.class, incomingChangeEvent.getOperation().toString()));
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
