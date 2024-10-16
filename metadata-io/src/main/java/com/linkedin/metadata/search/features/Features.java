package com.linkedin.metadata.search.features;

import com.google.common.collect.Streams;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Value
public class Features {
  Map<Name, Double> numericFeatures;

  public enum Name {
    SEARCH_BACKEND_SCORE, // Score returned by search backend
    NUM_ENTITIES_PER_TYPE, // Number of entities per entity type
    RANK_WITHIN_TYPE,
    ONLY_MATCH_CUSTOM_PROPERTIES; // Rank within the entity type
  }

  public Double getNumericFeature(Name featureName, double defaultValue) {
    return numericFeatures.getOrDefault(featureName, defaultValue);
  }

  @Nonnull
  public static Features from(Map<String, Double> numericFeatures) {
    Features result = new Features(new HashMap<>());
    if (numericFeatures == null) {
      return result;
    }

    for (Map.Entry<String, Double> entry : numericFeatures.entrySet()) {
      Name name;
      try {
        name = Name.valueOf(entry.getKey());
      } catch (IllegalArgumentException e) {
        log.error("Unknown feature name: {}", entry.getKey());
        throw e;
      }
      result.numericFeatures.put(name, entry.getValue());
    }
    return result;
  }

  @Nonnull
  public static Features merge(@Nonnull Features features1, @Nonnull Features features2) {
    Features result = new Features(new HashMap<>());
    result.numericFeatures.putAll(features1.numericFeatures);
    result.numericFeatures.putAll(features2.numericFeatures);
    return result;
  }

  @Nonnull
  public static List<Features> merge(
      @Nonnull List<Features> featureList1, @Nonnull List<Features> featureList2) {
    if (featureList1.size() != featureList2.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Expected both lists to have the same number of elements. %s != %s",
              featureList1.size(), featureList2.size()));
    }
    return Streams.zip(featureList1.stream(), featureList2.stream(), Features::merge)
        .collect(Collectors.toList());
  }
}
