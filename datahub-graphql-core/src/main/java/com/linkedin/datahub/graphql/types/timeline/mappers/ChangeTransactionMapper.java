package com.linkedin.datahub.graphql.types.timeline.mappers;

import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

// Class for converting ChangeTransactions received from the Timeline API to SchemaFieldBlame
// structs for every schema
// at every semantic version.
@Slf4j
public class ChangeTransactionMapper {

  public static com.linkedin.datahub.graphql.generated.ChangeTransaction map(
      @Nonnull final ChangeTransaction incomingChangeTransaction) {
    final com.linkedin.datahub.graphql.generated.ChangeTransaction result =
        new com.linkedin.datahub.graphql.generated.ChangeTransaction();

    result.setLastSemanticVersion(
        incomingChangeTransaction.getSemVer() == null
            ? "none"
            : incomingChangeTransaction.getSemVer());
    result.setTimestampMillis(incomingChangeTransaction.getTimestamp());
    result.setVersionStamp("none");
    result.setChangeType(ChangeOperationType.MODIFY);

    result.setChanges(
        incomingChangeTransaction.getChangeEvents().stream()
            .map(ChangeEventMapper::map)
            .collect(Collectors.toList()));

    return result;
  }

  private ChangeTransactionMapper() {}
}
