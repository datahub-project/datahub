package com.linkedin.metadata.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Parsed and validated priority band configuration for weighted fair queuing. Bands must cover the
 * full priority range [0, 9] without overlap.
 */
@Immutable
public record PriorityBandConfig(@Nonnull List<PriorityBand> bands) {

  public PriorityBandConfig {
    bands = List.copyOf(bands);
    validate(bands);
  }

  /** Parse a JSON array of band objects: {@code [{"range":[min,max],"weight":N}, ...]}. */
  @Nonnull
  public static PriorityBandConfig parse(@Nonnull ObjectMapper objectMapper, @Nonnull String json) {
    try {
      List<Map<String, Object>> raw = objectMapper.readValue(json, new TypeReference<>() {});
      List<PriorityBand> parsed =
          raw.stream()
              .map(
                  m -> {
                    @SuppressWarnings("unchecked")
                    List<Integer> range = (List<Integer>) m.get("range");
                    if (range == null || range.size() != 2) {
                      throw new IllegalArgumentException(
                          "Each band must have a 'range' array of exactly 2 integers");
                    }
                    Object w = m.get("weight");
                    if (w == null) {
                      throw new IllegalArgumentException("Each band must have a 'weight' field");
                    }
                    int weight = ((Number) w).intValue();
                    return new PriorityBand(range.get(0), range.get(1), weight);
                  })
              .toList();
      return new PriorityBandConfig(parsed);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid priority bands JSON: " + e.getMessage(), e);
    }
  }

  /**
   * Compute per-band batch limits for a given total batch size, distributing proportionally by
   * weight. Remainders are distributed round-robin starting from the highest-weight band. Each band
   * is guaranteed at least 1 slot if totalBatchSize >= number of bands.
   */
  @Nonnull
  public int[] batchLimits(int totalBatchSize) {
    int n = bands.size();
    if (totalBatchSize <= 0) {
      return new int[n];
    }
    int totalWeight = bands.stream().mapToInt(PriorityBand::weight).sum();
    int[] limits = new int[n];
    int allocated = 0;
    for (int i = 0; i < n; i++) {
      limits[i] = (int) ((long) totalBatchSize * bands.get(i).weight() / totalWeight);
      allocated += limits[i];
    }
    int remainder = totalBatchSize - allocated;
    for (int i = 0; i < remainder; i++) {
      limits[i % n]++;
    }
    return limits;
  }

  private static void validate(List<PriorityBand> bands) {
    if (bands.isEmpty()) {
      throw new IllegalArgumentException("At least one priority band is required");
    }
    boolean[] covered = new boolean[QueueTopicMetadata.MAX_PRIORITY + 1];
    for (PriorityBand band : bands) {
      for (int p = band.minPriority(); p <= band.maxPriority(); p++) {
        if (covered[p]) {
          throw new IllegalArgumentException(
              "Priority " + p + " is covered by multiple bands (overlap)");
        }
        covered[p] = true;
      }
    }
    for (int p = QueueTopicMetadata.MIN_PRIORITY; p <= QueueTopicMetadata.MAX_PRIORITY; p++) {
      if (!covered[p]) {
        throw new IllegalArgumentException(
            "Priority " + p + " is not covered by any band (gap in [0, 9])");
      }
    }
  }
}
