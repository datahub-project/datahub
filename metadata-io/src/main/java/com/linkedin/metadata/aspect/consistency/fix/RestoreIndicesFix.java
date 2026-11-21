package com.linkedin.metadata.aspect.consistency.fix;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Generic batched fix that restates latest authoritative aspects from primary storage via {@link
 * EntityService#restoreIndices(OperationContext, Set, Set, Integer, boolean)}.
 *
 * <p>Reads optional comma-separated aspect names from {@code issue.metadata.get("aspects")}; when
 * omitted or blank, restates all aspects of the entity. Never mutates primary storage.
 */
@Slf4j
@Component("restoreIndicesFix")
public class RestoreIndicesFix implements BatchConsistencyFix {

  public static final String ASPECTS_METADATA_KEY = "aspects";

  private final EntityService<?> entityService;

  public RestoreIndicesFix(@Nonnull EntityService<?> entityService) {
    this.entityService = Objects.requireNonNull(entityService, "entityService");
  }

  @Override
  @Nonnull
  public ConsistencyFixType getType() {
    return ConsistencyFixType.RESTORE_INDICES;
  }

  @Override
  @Nonnull
  public ConsistencyFixDetail apply(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyIssue issue, boolean dryRun) {
    return applyBatch(opContext, List.of(issue), dryRun).get(0);
  }

  @Override
  @Nonnull
  public List<ConsistencyFixDetail> applyBatch(
      @Nonnull OperationContext opContext, @Nonnull List<ConsistencyIssue> issues, boolean dryRun) {
    if (issues.isEmpty()) {
      return List.of();
    }
    if (dryRun) {
      List<ConsistencyFixDetail> dryDetails = new ArrayList<>(issues.size());
      for (ConsistencyIssue issue : issues) {
        Set<String> aspects = parseAspectNames(issue);
        log.info(
            "[DRY-RUN] RESTORE_INDICES urn={} aspects={}",
            issue.getEntityUrn(),
            aspects == null ? "*" : aspects);
        dryDetails.add(
            ConsistencyFixDetail.builder()
                .urn(issue.getEntityUrn())
                .action(ConsistencyFixType.RESTORE_INDICES)
                .success(true)
                .details(formatDetails(true, aspects, 0))
                .build());
      }
      return dryDetails;
    }

    Set<Urn> batchUrns = new LinkedHashSet<>();
    for (ConsistencyIssue issue : issues) {
      batchUrns.add(issue.getEntityUrn());
    }

    Set<Urn> existingUrns;
    try {
      existingUrns = entityService.exists(opContext, batchUrns, null, true, false);
    } catch (Exception e) {
      log.error("Failed key-aspect existence check before RESTORE_INDICES: {}", e.getMessage(), e);
      List<ConsistencyFixDetail> failed = new ArrayList<>(issues.size());
      for (ConsistencyIssue issue : issues) {
        failed.add(failureDetail(issue.getEntityUrn(), e.getMessage()));
      }
      return failed;
    }

    Map<Urn, Outcome> outcomesByUrn = new LinkedHashMap<>();
    Map<Urn, Optional<Set<String>>> effectiveAspectsByUrn = new LinkedHashMap<>();
    for (ConsistencyIssue issue : issues) {
      Urn urn = issue.getEntityUrn();
      if (!existingUrns.contains(urn)) {
        outcomesByUrn.put(
            urn, new Outcome(false, "Entity key aspect not found in primary storage", null));
        continue;
      }
      Set<String> issueAspects = parseAspectNames(issue);
      effectiveAspectsByUrn.merge(
          urn, Optional.ofNullable(issueAspects), RestoreIndicesFix::mergeTargetAspects);
    }

    Map<Optional<Set<String>>, Set<Urn>> urnsByAspectGroup = new LinkedHashMap<>();
    effectiveAspectsByUrn.forEach(
        (urn, aspectKey) ->
            urnsByAspectGroup.computeIfAbsent(aspectKey, k -> new LinkedHashSet<>()).add(urn));

    for (Map.Entry<Optional<Set<String>>, Set<Urn>> entry : urnsByAspectGroup.entrySet()) {
      Set<String> aspectNames = entry.getKey().orElse(null);
      Set<Urn> groupUrns = entry.getValue();
      restoreGroup(opContext, groupUrns, aspectNames, outcomesByUrn);
    }

    List<ConsistencyFixDetail> details = new ArrayList<>(issues.size());
    for (ConsistencyIssue issue : issues) {
      Urn urn = issue.getEntityUrn();
      Outcome outcome = outcomesByUrn.get(urn);
      if (outcome == null) {
        details.add(failureDetail(urn, "No restore outcome recorded"));
      } else if (outcome.success()) {
        details.add(
            ConsistencyFixDetail.builder()
                .urn(urn)
                .action(ConsistencyFixType.RESTORE_INDICES)
                .success(true)
                .details(outcome.details())
                .build());
      } else {
        details.add(failureDetail(urn, outcome.errorMessage()));
      }
    }
    return details;
  }

  private void restoreGroup(
      @Nonnull OperationContext opContext,
      @Nonnull Set<Urn> groupUrns,
      @Nullable Set<String> aspectNames,
      @Nonnull Map<Urn, Outcome> outcomesByUrn) {
    try {
      List<RestoreIndicesResult> results =
          entityService.restoreIndices(opContext, groupUrns, aspectNames, groupUrns.size(), false);
      int rowsMigrated = totalRowsMigrated(results);
      log.info(
          "[APPLIED] RESTORE_INDICES urnCount={} aspects={} rowsMigrated={}",
          groupUrns.size(),
          aspectNames == null ? "*" : aspectNames,
          rowsMigrated);
      String detail = formatDetails(false, aspectNames, rowsMigrated);
      for (Urn urn : groupUrns) {
        outcomesByUrn.put(urn, new Outcome(true, null, detail));
      }
    } catch (Exception batchEx) {
      if (groupUrns.size() == 1) {
        Urn urn = groupUrns.iterator().next();
        log.error("Failed RESTORE_INDICES for {}: {}", urn, batchEx.getMessage(), batchEx);
        outcomesByUrn.put(urn, new Outcome(false, batchEx.getMessage(), null));
        return;
      }
      log.warn(
          "Batched RESTORE_INDICES failed for {} URNs (aspects={}); isolating per URN: {}",
          groupUrns.size(),
          aspectNames == null ? "*" : aspectNames,
          batchEx.getMessage());
      for (Urn urn : groupUrns) {
        try {
          List<RestoreIndicesResult> singleResult =
              entityService.restoreIndices(opContext, Set.of(urn), aspectNames, 1, false);
          outcomesByUrn.put(
              urn,
              new Outcome(
                  true, null, formatDetails(false, aspectNames, totalRowsMigrated(singleResult))));
        } catch (Exception singleEx) {
          log.error("Failed RESTORE_INDICES for {}: {}", urn, singleEx.getMessage(), singleEx);
          outcomesByUrn.put(urn, new Outcome(false, singleEx.getMessage(), null));
        }
      }
    }
  }

  @Nonnull
  private static Optional<Set<String>> mergeTargetAspects(
      @Nonnull Optional<Set<String>> first, @Nonnull Optional<Set<String>> second) {
    if (first.isEmpty() || second.isEmpty()) {
      return Optional.empty();
    }
    Set<String> merged = new LinkedHashSet<>(first.get());
    merged.addAll(second.get());
    return Optional.of(Collections.unmodifiableSet(merged));
  }

  @Nullable
  static Set<String> parseAspectNames(@Nonnull ConsistencyIssue issue) {
    Map<String, String> metadata = issue.getMetadata();
    if (metadata == null) {
      return null;
    }
    String raw = metadata.get(ASPECTS_METADATA_KEY);
    if (raw == null || raw.isBlank()) {
      return null;
    }
    Set<String> names = new LinkedHashSet<>();
    for (String part : raw.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        names.add(trimmed);
      }
    }
    return names.isEmpty() ? null : Collections.unmodifiableSet(names);
  }

  private static int totalRowsMigrated(@Nullable List<RestoreIndicesResult> results) {
    if (results == null) {
      return 0;
    }
    int total = 0;
    for (RestoreIndicesResult result : results) {
      if (result != null) {
        total += result.rowsMigrated;
      }
    }
    return total;
  }

  @Nonnull
  private static String formatDetails(
      boolean dryRun, @Nullable Set<String> aspectNames, int rowsMigrated) {
    String scope = aspectNames == null ? "all aspects" : "aspects=" + String.join(",", aspectNames);
    if (dryRun) {
      return "[DRY-RUN] Would restate " + scope;
    }
    return "Restated " + scope + " (rowsMigrated=" + rowsMigrated + ")";
  }

  @Nonnull
  private static ConsistencyFixDetail failureDetail(@Nonnull Urn urn, @Nullable String message) {
    return ConsistencyFixDetail.builder()
        .urn(urn)
        .action(ConsistencyFixType.RESTORE_INDICES)
        .success(false)
        .errorMessage(message != null ? message : "Unknown error during RESTORE_INDICES")
        .build();
  }

  private record Outcome(
      boolean success, @Nullable String errorMessage, @Nullable String details) {}
}
