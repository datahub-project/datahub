package com.linkedin.datahub.graphql.resolvers.search;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.generated.ExtraProperty;
import com.linkedin.datahub.graphql.generated.RescoreExplanation;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.SignalExplanation;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver for the rescoreExplanation field on SearchResult. Parses the _rescoreExplain JSON from
 * extraProperties and returns a typed RescoreExplanation object.
 */
@Slf4j
public class RescoreExplanationResolver implements DataFetcher<RescoreExplanation> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String RESCORE_EXPLAIN_KEY = "_rescoreExplain";

  @Override
  @Nullable
  public RescoreExplanation get(DataFetchingEnvironment environment) {
    SearchResult searchResult = environment.getSource();

    if (searchResult == null || searchResult.getExtraProperties() == null) {
      return null;
    }

    // Find the _rescoreExplain property
    String rescoreExplainJson = null;
    for (ExtraProperty prop : searchResult.getExtraProperties()) {
      if (RESCORE_EXPLAIN_KEY.equals(prop.getName())) {
        rescoreExplainJson = prop.getValue();
        break;
      }
    }

    if (rescoreExplainJson == null) {
      return null;
    }

    try {
      return parseRescoreExplanation(rescoreExplainJson);
    } catch (Exception e) {
      log.warn("Failed to parse rescore explanation JSON: {}", e.getMessage());
      return null;
    }
  }

  private RescoreExplanation parseRescoreExplanation(String json) throws Exception {
    Map<String, Object> data = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});

    RescoreExplanation explanation = new RescoreExplanation();
    explanation.setStage1Score(getDoubleValue(data, "bm25Score"));
    explanation.setStage2Score(getDoubleValue(data, "finalScore"));
    explanation.setFormula((String) data.get("formula"));

    // Parse signals
    List<SignalExplanation> signals = new ArrayList<>();
    @SuppressWarnings("unchecked")
    Map<String, Object> signalsMap = (Map<String, Object>) data.get("signals");

    if (signalsMap != null) {
      for (Map.Entry<String, Object> entry : signalsMap.entrySet()) {
        @SuppressWarnings("unchecked")
        Map<String, Object> signalData = (Map<String, Object>) entry.getValue();

        // Skip null or malformed signal data instead of failing entire explanation
        if (signalData == null) {
          log.debug("Skipping signal '{}' with null data", entry.getKey());
          continue;
        }

        signals.add(parseSignalExplanation(entry.getKey(), signalData));
      }
    }

    explanation.setSignals(signals);
    return explanation;
  }

  private SignalExplanation parseSignalExplanation(
      String name, @Nullable Map<String, Object> data) {
    SignalExplanation signal = new SignalExplanation();
    signal.setName(name);

    // Defensive: handle null data gracefully
    if (data == null) {
      return signal;
    }

    signal.setRawValue(data.get("rawValue") != null ? String.valueOf(data.get("rawValue")) : null);
    signal.setNumericValue(getDoubleValue(data, "numericValue"));
    signal.setNormalizedValue(getDoubleValue(data, "normalizedValue"));
    signal.setBoost(getDoubleValue(data, "boost"));
    signal.setContribution(getDoubleValue(data, "contribution"));
    return signal;
  }

  private double getDoubleValue(Map<String, Object> data, String key) {
    Object value = data.get(key);
    if (value == null) {
      return 0.0;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    return Double.parseDouble(String.valueOf(value));
  }
}
