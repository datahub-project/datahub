package com.linkedin.datahub.graphql.types.timeline.mappers;

import static com.linkedin.datahub.graphql.types.timeline.utils.TimelineUtils.*;

import com.linkedin.datahub.graphql.generated.GetSchemaVersionListResult;
import com.linkedin.datahub.graphql.generated.SemanticVersionStruct;
import com.linkedin.datahub.graphql.types.timeline.utils.TimelineUtils;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

// Class for converting ChangeTransactions received from the Timeline API to list of schema
// versions.
@Slf4j
public class SchemaVersionListMapper {

  public static GetSchemaVersionListResult map(List<ChangeTransaction> changeTransactions) {
    if (changeTransactions.isEmpty()) {
      log.debug("Change transactions are empty");
      return null;
    }

    GetSchemaVersionListResult result = new GetSchemaVersionListResult();

    String latestSemanticVersionString =
        truncateSemanticVersion(changeTransactions.get(changeTransactions.size() - 1).getSemVer());
    long latestSemanticVersionTimestamp =
        changeTransactions.get(changeTransactions.size() - 1).getTimestamp();
    String latestVersionStamp =
        changeTransactions.get(changeTransactions.size() - 1).getVersionStamp();
    result.setLatestVersion(
        new SemanticVersionStruct(
            latestSemanticVersionString, latestSemanticVersionTimestamp, latestVersionStamp));

    List<ChangeTransaction> reversedChangeTransactions =
        changeTransactions.stream()
            .map(TimelineUtils::semanticVersionChangeTransactionPair)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .sorted(Collections.reverseOrder(Comparator.comparing(Pair::getFirst)))
            .map(Pair::getSecond)
            .collect(Collectors.toList());

    List<SemanticVersionStruct> semanticVersionStructList =
        reversedChangeTransactions.stream()
            .map(
                changeTransaction ->
                    new SemanticVersionStruct(
                        truncateSemanticVersion(changeTransaction.getSemVer()),
                        changeTransaction.getTimestamp(),
                        changeTransaction.getVersionStamp()))
            .collect(Collectors.toList());

    result.setSemanticVersionList(semanticVersionStructList);
    return result;
  }

  private SchemaVersionListMapper() {}
}
