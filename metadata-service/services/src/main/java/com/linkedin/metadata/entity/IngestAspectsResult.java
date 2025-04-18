package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class IngestAspectsResult {
  public static final IngestAspectsResult EMPTY = IngestAspectsResult.builder().build();

  List<UpdateAspectResult> updateAspectResults;
  List<Pair<ChangeMCP, Set<AspectValidationException>>> failedUpdateAspectResults;

  public boolean shouldCommit() {
    return updateAspectResults.stream().anyMatch(Objects::nonNull);
  }

  public static IngestAspectsResult combine(IngestAspectsResult first, IngestAspectsResult second) {
    if (first == null) {
      return second != null ? second : IngestAspectsResult.builder().build();
    }
    if (second == null) {
      return first;
    }

    List<UpdateAspectResult> combinedResults =
        Stream.concat(
                first.getUpdateAspectResults().stream(), second.getUpdateAspectResults().stream())
            .collect(Collectors.toList());

    List<Pair<ChangeMCP, Set<AspectValidationException>>> combinedFailedResults =
        Stream.concat(
                first.getFailedUpdateAspectResults().stream(),
                second.getFailedUpdateAspectResults().stream())
            .collect(Collectors.toList());

    return IngestAspectsResult.builder()
        .updateAspectResults(combinedResults)
        .failedUpdateAspectResults(combinedFailedResults)
        .build();
  }

  public static class IngestAspectsResultBuilder {
    public IngestAspectsResult build() {
      if (this.failedUpdateAspectResults == null) {
        this.failedUpdateAspectResults = Collections.emptyList();
      }
      if (this.updateAspectResults == null) {
        this.updateAspectResults = Collections.emptyList();
      }

      return new IngestAspectsResult(
          this.updateAspectResults.stream().filter(Objects::nonNull).collect(Collectors.toList()),
          this.failedUpdateAspectResults);
    }
  }
}
