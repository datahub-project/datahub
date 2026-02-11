package com.linkedin.metadata.search.rescore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.search.SearchHit;

/**
 * Extracts and normalizes signal values from Elasticsearch search hits.
 *
 * <p>This class handles: - Extracting raw values from document fields - Converting to numeric
 * values - Applying normalization based on signal configuration - Computing contribution
 * (pow(normalized, boost))
 */
@Slf4j
public class SignalExtractor {

  /**
   * Extract all signal values from a search hit.
   *
   * @param hit the Elasticsearch search hit
   * @param esScore the _score from Elasticsearch (BM25 + boosts)
   * @param signals list of signal definitions
   * @return map of signal name to SignalValue (ordered)
   */
  public Map<String, SignalValue> extract(
      SearchHit hit, double esScore, List<SignalDefinition> signals) {
    Map<String, Object> source = hit.getSourceAsMap();
    // Guard against null source map (can happen if _source is not stored)
    if (source == null) {
      source = new java.util.HashMap<>();
    }

    // Extract index name for entity type detection
    String indexName = hit.getIndex();

    if (log.isDebugEnabled() && !source.isEmpty()) {
      log.debug("Available fields in _source ({}): {}", source.size(), source.keySet());
    }

    Map<String, SignalValue> result = new LinkedHashMap<>();

    for (SignalDefinition signal : signals) {
      SignalValue value = extractSignal(signal, source, esScore, indexName);
      result.put(signal.getName(), value);
    }

    return result;
  }

  private SignalValue extractSignal(
      SignalDefinition signal, Map<String, Object> source, double esScore, String indexName) {
    // Extract raw value
    Object rawValue = extractRawValue(signal, source, esScore, indexName);
    double numericValue = toNumericValue(rawValue, signal.getType());

    // Apply normalization
    double normalizedValue = normalize(numericValue, signal);

    // Fairness: Apply assumed moderate usage for logical entities with inapplicable signals
    // Logical entities (glossary terms/nodes, domains, data products) don't accumulate
    // usage metrics like queryCount/viewCount, but that doesn't mean they're low value.
    // Give them 1.15 (equivalent to moderate usage ~200-300 count) instead of 1.0 which
    // is actually below the sigmoid minimum (1.047) and acts as a penalty.
    if (isLogicalEntity(indexName) && isUsageSignal(signal.getName())) {
      normalizedValue = 1.15; // Generous: assumes moderate-to-good usage/value
    }

    // Compute contribution: pow(normalizedValue, boost)
    // Guard against NaN (can occur with negative base and fractional exponent)
    double contribution = Math.pow(normalizedValue, signal.getBoost());
    if (Double.isNaN(contribution) || Double.isInfinite(contribution)) {
      log.warn(
          "Contribution calculation for signal '{}' produced {}: normalizedValue={}, boost={}. Using 1.0.",
          signal.getName(),
          contribution,
          normalizedValue,
          signal.getBoost());
      contribution = 1.0; // Neutral value for multiplication
    }

    return SignalValue.builder()
        .name(signal.getName())
        .rawValue(rawValue)
        .numericValue(numericValue)
        .normalizedValue(normalizedValue)
        .normalizationType(signal.getNormalization().getType())
        .boost(signal.getBoost())
        .contribution(contribution)
        .build();
  }

  private Object extractRawValue(
      SignalDefinition signal, Map<String, Object> source, double esScore, String indexName) {
    String fieldPath = signal.getFieldPath();

    // Special case: _score is the ES score
    if ("_score".equals(fieldPath)) {
      return esScore;
    }

    // Special case: _index is the index name (for entity type detection)
    if ("_index".equals(fieldPath) && signal.getType() == SignalType.INDEX_NAME) {
      return indexName;
    }

    // Special case: hasDescription should check both 'description' and 'definition' fields
    // Glossary terms use 'definition' instead of 'description'
    // Tags are self-descriptive (their name/ID is the description)
    if ("hasDescription".equals(fieldPath)) {
      Object description = source.get("description");
      if (description != null && !description.toString().isEmpty()) {
        return true;
      }
      Object definition = source.get("definition");
      if (definition != null && !definition.toString().isEmpty()) {
        return true;
      }
      // Tags use their name as description - they're self-descriptive entities
      if (indexName != null && indexName.contains("tagindex")) {
        return true;
      }
      // Fall back to checking the hasDescription field directly if present
      // This handles cases where the field is already pre-computed in the document
      Object hasDescField = source.get("hasDescription");
      if (hasDescField instanceof Boolean) {
        return (Boolean) hasDescField;
      }
      return false;
    }

    // Special case: Glossary terms and nodes ARE glossary terms semantically
    // So they should automatically have hasGlossaryTerms=true
    if ("hasGlossaryTerms".equals(fieldPath)) {
      if (indexName != null
          && (indexName.contains("glossarytermindex") || indexName.contains("glossarynodeindex"))) {
        return true;
      }
    }

    // Handle nested paths (e.g., "statsSummary.queryCount")
    String[] parts = fieldPath.split("\\.");
    Object current = source;
    for (String part : parts) {
      if (current instanceof Map) {
        current = ((Map<?, ?>) current).get(part);
      } else {
        log.debug("Field {} - path part '{}' not a Map, returning null", fieldPath, part);
        return null;
      }
      if (current == null) {
        log.debug("Field {} - path part '{}' returned null", fieldPath, part);
        return null;
      }
    }
    log.debug(
        "Field {} - extracted value: {} (type: {})",
        fieldPath,
        current,
        current != null ? current.getClass().getSimpleName() : "null");
    return current;
  }

  private double toNumericValue(Object rawValue, SignalType type) {
    if (rawValue == null) {
      return 0.0;
    }

    switch (type) {
      case SCORE:
      case NUMERIC:
        if (rawValue instanceof Number) {
          return ((Number) rawValue).doubleValue();
        }
        try {
          return Double.parseDouble(rawValue.toString());
        } catch (NumberFormatException e) {
          return 0.0;
        }

      case BOOLEAN:
        if (rawValue instanceof Boolean) {
          return ((Boolean) rawValue) ? 1.0 : 0.0;
        }
        if (rawValue instanceof Number) {
          return ((Number) rawValue).doubleValue() > 0 ? 1.0 : 0.0;
        }
        return Boolean.parseBoolean(rawValue.toString()) ? 1.0 : 0.0;

      case TIMESTAMP:
        // For timestamps, we compute age in days from current time
        if (rawValue instanceof Number) {
          long timestampMs = ((Number) rawValue).longValue();
          long ageMs = System.currentTimeMillis() - timestampMs;
          return ageMs / (24.0 * 60 * 60 * 1000); // Convert to days
        }
        return 0.0;

      case INDEX_NAME:
        // For index names, detect entity type and return boost multiplier directly
        // These values will be used as-is (identity normalization)
        String indexName = rawValue.toString().toLowerCase();
        if (indexName.contains("glossarytermindex") || indexName.contains("glossarynodeindex")) {
          return 1.4; // 40% boost for glossary terms/nodes
        } else if (indexName.contains("domainindex") || indexName.contains("dataproductindex")) {
          return 1.3; // 30% boost for domains/data products
        } else if (indexName.contains("tagindex")) {
          return 1.4; // 40% boost for tags (organizational/definitional entities)
        }
        return 1.0; // No boost for other entity types

      default:
        return 0.0;
    }
  }

  private double normalize(double value, SignalDefinition signal) {
    NormalizationConfig config = signal.getNormalization();

    switch (config.getType()) {
      case SIGMOID:
        return sigmoidNormalize(
            value,
            config.getInputMin(),
            config.getInputMax(),
            config.getSteepness(),
            config.getOutputMin(),
            config.getOutputMax());

      case LOG_SIGMOID:
        return logSigmoidNormalize(
            value,
            config.getInputMin(),
            config.getInputMax(),
            config.getSteepness(),
            config.getOutputMin(),
            config.getOutputMax());

      case LINEAR_DECAY:
        return linearDecayNormalize(
            value, config.getScale(), config.getOutputMin(), config.getOutputMax());

      case BOOLEAN:
        return value > 0 ? config.getTrueValue() : config.getFalseValue();

      case NONE:
      default:
        return value;
    }
  }

  /**
   * Sigmoid normalization with configurable input range and steepness.
   *
   * @param value raw input value
   * @param inputMin values at or below this map to outputMin
   * @param inputMax values at or above this map to outputMax
   * @param steepness controls slope (6.0 = default, uses 90% of S-curve)
   * @param outputMin minimum output value
   * @param outputMax maximum output value
   * @return normalized value in [outputMin, outputMax]
   */
  private double sigmoidNormalize(
      double value,
      double inputMin,
      double inputMax,
      double steepness,
      double outputMin,
      double outputMax) {
    // Handle invalid range
    if (inputMax <= inputMin) {
      return outputMin;
    }

    // Clamp to input range
    double clamped = Math.max(inputMin, Math.min(value, inputMax));

    // Normalize to [0, 1]
    double normalized = (clamped - inputMin) / (inputMax - inputMin);

    // Apply sigmoid centered at 0.5, scaled by steepness
    double x = (normalized - 0.5) * steepness;
    double sig = 1.0 / (1.0 + Math.exp(-x));

    // Map to output range
    return outputMin + (outputMax - outputMin) * sig;
  }

  private static final double LOG2 = Math.log(2);

  /**
   * Log-sigmoid normalization for large value ranges.
   *
   * <p>Applies log2(value + 1) before sigmoid, useful when values span orders of magnitude (e.g.,
   * view counts from 0 to millions). Log base 2 preserves more granularity than log10 - a doubling
   * of values results in +1 in log space.
   */
  private double logSigmoidNormalize(
      double value,
      double inputMin,
      double inputMax,
      double steepness,
      double outputMin,
      double outputMax) {
    // Handle invalid range
    if (inputMax <= inputMin) {
      return outputMin;
    }

    // Apply log2 transformation (add 1 to handle zero)
    double logValue = Math.log(Math.max(0, value) + 1) / LOG2;
    double logMin = Math.log(Math.max(0, inputMin) + 1) / LOG2;
    double logMax = Math.log(Math.max(0, inputMax) + 1) / LOG2;

    // Clamp to log range
    double clamped = Math.max(logMin, Math.min(logValue, logMax));

    // Normalize to [0, 1] in log space
    double normalized = (logMax > logMin) ? (clamped - logMin) / (logMax - logMin) : 0.0;

    // Apply sigmoid centered at 0.5, scaled by steepness
    double x = (normalized - 0.5) * steepness;
    double sig = 1.0 / (1.0 + Math.exp(-x));

    // Map to output range
    return outputMin + (outputMax - outputMin) * sig;
  }

  private double linearDecayNormalize(double ageInDays, double scale, double min, double max) {
    if (scale <= 0) {
      return min; // Return minimum if scale is invalid
    }
    double decayed = max - (ageInDays / scale) * (max - min);
    return Math.max(min, Math.min(max, decayed));
  }

  /**
   * Check if the entity is a logical/organizational entity (as opposed to a data asset).
   *
   * <p>Logical entities include: - Glossary Terms (definitional) - Glossary Nodes (hierarchical
   * term groups) - Domains (organizational containers) - Data Products (organizational containers)
   * - Tags (metadata classification labels)
   *
   * <p>These entities serve reference/organizational purposes and don't accumulate usage metrics
   * like queryCount or usageCount.
   */
  private boolean isLogicalEntity(String indexName) {
    if (indexName == null) {
      return false;
    }
    String lowerIndex = indexName.toLowerCase();
    return lowerIndex.contains("glossarytermindex")
        || lowerIndex.contains("glossarynodeindex")
        || lowerIndex.contains("domainindex")
        || lowerIndex.contains("dataproductindex")
        || lowerIndex.contains("tagindex");
  }

  /**
   * Check if the signal is a usage/popularity metric that doesn't apply to logical entities.
   *
   * <p>Usage signals that are inapplicable to logical entities: - queryCount: Logical entities
   * aren't queried (no SQL operations) - usageCount: Not tracked for logical entities - viewCount:
   * Currently not tracked for logical entity page views - uniqueUserCount: Currently not tracked
   * for logical entities
   *
   * <p>These signals make sense for data assets (datasets, dashboards, charts) but not for
   * reference/organizational entities.
   *
   * <p>NOTE: In the future, when we add logical entity-specific usage signals (e.g.,
   * glossaryTermReferenceCount = "how many assets use this term"), the fairness principle applies
   * symmetrically: non-logical entities should get neutral defaults for those signals.
   */
  private boolean isUsageSignal(String signalName) {
    return "viewCount".equals(signalName)
        || "queryCount".equals(signalName)
        || "usageCount".equals(signalName)
        || "uniqueUserCount".equals(signalName);
  }
}
