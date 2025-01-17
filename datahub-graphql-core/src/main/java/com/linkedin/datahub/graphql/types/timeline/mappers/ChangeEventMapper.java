package com.linkedin.datahub.graphql.types.timeline.mappers;

import com.linkedin.datahub.graphql.generated.ChangeCategoryType;
import com.linkedin.datahub.graphql.generated.ChangeOperationType;
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

    //    result.setAuditStamp(AuditStampMapper.map(incomingChangeEvent.getAuditStamp()));
    result.setUrn("empty");
    result.setCategory(ChangeCategoryType.valueOf(incomingChangeEvent.getCategory().toString()));
    result.setDescription(incomingChangeEvent.getDescription());
    result.setModifier(incomingChangeEvent.getModifier());
    result.setOperation(ChangeOperationType.valueOf(incomingChangeEvent.getOperation().toString()));
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
    result.setUrn(result.getUrn());

    return result;
  }

  private ChangeEventMapper() {}
}
