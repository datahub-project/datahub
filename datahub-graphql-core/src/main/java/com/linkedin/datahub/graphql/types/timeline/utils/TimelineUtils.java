package com.linkedin.datahub.graphql.types.timeline.utils;

import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.datahub.graphql.generated.SchemaFieldChange;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.util.Pair;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.SemanticVersion;


@Slf4j
public class TimelineUtils {

  public static Optional<Pair<SemanticVersion, ChangeTransaction>> semanticVersionChangeTransactionPair(
      ChangeTransaction changeTransaction) {
    Optional<SemanticVersion> semanticVersion = createSemanticVersion(changeTransaction.getSemVer());
    return semanticVersion.map(version -> Pair.of(version, changeTransaction));
  }

  public static Optional<SemanticVersion> createSemanticVersion(String semanticVersionString) {
    String truncatedSemanticVersion = truncateSemanticVersion(semanticVersionString);
    try {
      SemanticVersion semanticVersion = SemanticVersion.parse(truncatedSemanticVersion);
      return Optional.of(semanticVersion);
    } catch (SemanticVersion.SemanticVersionParseException e) {
      return Optional.empty();
    }
  }

  // The SemanticVersion is currently returned from the ChangeTransactions in the format "x.y.z-computed". This function
  // removes the suffix "computed".
  public static String truncateSemanticVersion(String semanticVersion) {
    String suffix = "-computed";
    return semanticVersion.endsWith(suffix) ? semanticVersion.substring(0, semanticVersion.lastIndexOf(suffix))
        : semanticVersion;
  }

  public static SchemaFieldChange getLastSchemaFieldChange(ChangeEvent changeEvent, long timestamp,
      String semanticVersion, String versionStamp) {
    SchemaFieldChange schemaFieldChange = new SchemaFieldChange();
    schemaFieldChange.setTimestampMillis(timestamp);
    schemaFieldChange.setLastSemanticVersion(truncateSemanticVersion(semanticVersion));
    schemaFieldChange.setChangeType(
        ChangeOperationType.valueOf(ChangeOperationType.class, changeEvent.getOperation().toString()));
    schemaFieldChange.setVersionStamp(versionStamp);

    String translatedChangeOperationType;
    switch (changeEvent.getOperation()) {
      case ADD:
        translatedChangeOperationType = "Added";
        break;
      case MODIFY:
        translatedChangeOperationType = "Modified";
        break;
      case REMOVE:
        translatedChangeOperationType = "Removed";
        break;
      default:
        translatedChangeOperationType = "Unknown change made";
        log.warn(translatedChangeOperationType);
        break;
    }

    String suffix = "-computed";
    String translatedSemanticVersion =
        semanticVersion.endsWith(suffix) ? semanticVersion.substring(0, semanticVersion.lastIndexOf(suffix))
            : semanticVersion;

    String lastSchemaFieldChange = String.format("%s in v%s", translatedChangeOperationType, translatedSemanticVersion);
    schemaFieldChange.setLastSchemaFieldChange(lastSchemaFieldChange);

    return schemaFieldChange;
  }

  private TimelineUtils() {
  }
}
