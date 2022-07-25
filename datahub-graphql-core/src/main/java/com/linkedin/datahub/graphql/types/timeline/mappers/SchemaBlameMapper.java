package com.linkedin.datahub.graphql.types.timeline.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ChangeOperationType;
import com.linkedin.datahub.graphql.generated.GetSchemaBlameResult;
import com.linkedin.datahub.graphql.generated.SchemaFieldBlame;
import com.linkedin.datahub.graphql.generated.SchemaFieldChange;
import com.linkedin.datahub.graphql.generated.SemanticVersionStruct;
import com.linkedin.datahub.graphql.types.timeline.utils.TimelineUtils;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.SemanticVersion;

import static com.linkedin.datahub.graphql.types.timeline.utils.TimelineUtils.*;


// Class for converting ChangeTransactions received from the Timeline API to SchemaFieldBlame structs for every schema
// at every semantic version.
@Slf4j
public class SchemaBlameMapper {

  public static GetSchemaBlameResult map(List<ChangeTransaction> changeTransactions, @Nullable String versionCutoff) {
    if (changeTransactions.isEmpty()) {
      log.debug("Change transactions are empty");
      return null;
    }

    Map<String, SchemaFieldBlame> schemaBlameMap = new HashMap<>();
    GetSchemaBlameResult result = new GetSchemaBlameResult();

    String latestSemanticVersionString =
        truncateSemanticVersion(changeTransactions.get(changeTransactions.size() - 1).getSemVer());

    String semanticVersionFilterString = versionCutoff == null ? latestSemanticVersionString : versionCutoff;
    Optional<SemanticVersion> semanticVersionFilterOptional = createSemanticVersion(semanticVersionFilterString);
    if (!semanticVersionFilterOptional.isPresent()) {
      return result;
    }

    SemanticVersion semanticVersionFilter = semanticVersionFilterOptional.get();

    List<ChangeTransaction> reversedChangeTransactions = changeTransactions.stream()
        .map(TimelineUtils::semanticVersionChangeTransactionPair)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(semanticVersionChangeTransactionPair ->
            semanticVersionChangeTransactionPair.getFirst().compareTo(semanticVersionFilter) <= 0)
        .sorted(Collections.reverseOrder(Comparator.comparing(Pair::getFirst)))
        .map(Pair::getSecond)
        .collect(Collectors.toList());

    String selectedSemanticVersion = truncateSemanticVersion(reversedChangeTransactions.get(0).getSemVer());
    long selectedSemanticVersionTimestamp = reversedChangeTransactions.get(0).getTimestamp();
    String selectedVersionStamp = reversedChangeTransactions.get(0).getVersionStamp();
    result.setVersion(
        new SemanticVersionStruct(selectedSemanticVersion, selectedSemanticVersionTimestamp, selectedVersionStamp));

    for (ChangeTransaction changeTransaction : reversedChangeTransactions) {
      for (ChangeEvent changeEvent : changeTransaction.getChangeEvents()) {
        if (changeEvent.getCategory() != ChangeCategory.TECHNICAL_SCHEMA) {
          continue;
        }

        String schemaUrn = changeEvent.getModifier();
        if (schemaUrn == null || schemaBlameMap.containsKey(schemaUrn)) {
          continue;
        }

        SchemaFieldBlame schemaFieldBlame = new SchemaFieldBlame();

        SchemaFieldKey schemaFieldKey;
        try {
          schemaFieldKey = (SchemaFieldKey) EntityKeyUtils.convertUrnToEntityKey(Urn.createFromString(schemaUrn),
              new SchemaFieldKey().schema());
        } catch (Exception e) {
          log.debug(String.format("Could not generate schema urn for %s", schemaUrn));
          continue;
        }

        String fieldPath = schemaFieldKey.getFieldPath();
        schemaFieldBlame.setFieldPath(fieldPath);

        SchemaFieldChange schemaFieldChange =
            getLastSchemaFieldChange(changeEvent, changeTransaction.getTimestamp(), changeTransaction.getSemVer(),
                changeTransaction.getVersionStamp());
        schemaFieldBlame.setSchemaFieldChange(schemaFieldChange);

        schemaBlameMap.put(schemaUrn, schemaFieldBlame);
      }
    }

    result.setSchemaFieldBlameList(schemaBlameMap.values()
        .stream()
        .filter(schemaFieldBlame -> !schemaFieldBlame.getSchemaFieldChange()
            .getChangeType()
            .equals(ChangeOperationType.REMOVE))
        .collect(Collectors.toList()));
    return result;
  }

  private SchemaBlameMapper() {
  }
}