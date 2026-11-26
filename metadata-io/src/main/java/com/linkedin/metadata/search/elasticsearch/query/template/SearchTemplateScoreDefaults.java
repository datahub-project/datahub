package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.search.template.EntityTypeBoost;
import com.linkedin.search.template.EntityTypeBoostArray;
import com.linkedin.search.template.FieldFactor;
import com.linkedin.search.template.FieldFactorArray;
import com.linkedin.search.template.FieldFactorModifier;
import com.linkedin.search.template.FieldValueScore;
import com.linkedin.search.template.FieldValueScoreArray;
import com.linkedin.search.template.TermWeight;
import com.linkedin.search.template.TermWeightArray;
import com.linkedin.structured.StructuredPropertyScore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Checks and resolves template score sections. A property's omitted extents come from its
 * definition. A weight of 1 is omitted because it does not change order.
 */
public final class SearchTemplateScoreDefaults {
  private static final Pattern FIELD = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z0-9_]+)*");

  private SearchTemplateScoreDefaults() {}

  /**
   * Finite range with {@code inputMax} greater than {@code inputMin} and a multiplier of at least
   * 1. A value set on the template replaces the definition.
   */
  public record Extent(double inputMin, double inputMax, double maxMultiplier) {}

  /** One entity type whose weight is not 1. */
  public record EntityWeight(@Nonnull String entityType, float weight) {}

  public static void checkFieldValueScores(@Nullable FieldValueScoreArray scores) {
    if (scores == null || scores.isEmpty()) {
      return;
    }
    Set<String> names = new LinkedHashSet<>();
    for (FieldValueScore score : scores) {
      String name =
          exclusiveName(
              "fieldValueScores",
              score.hasField(),
              score.hasField() ? score.getField() : null,
              score.hasPropertyUrn() ? score.getPropertyUrn() : null);
      if (!names.add(name)) {
        throw new SearchTemplateException(
            "Search template fieldValueScores names " + displayName(name) + " more than once");
      }
      Double min = score.hasInputMin() ? score.getInputMin() : null;
      Double max = score.hasInputMax() ? score.getInputMax() : null;
      Double multiplier = score.hasMaxMultiplier() ? score.getMaxMultiplier() : null;
      if (name.startsWith("field:") && (min == null || max == null || multiplier == null)) {
        throw new SearchTemplateException(
            "Search template fieldValueScores field requires inputMin, inputMax, and maxMultiplier");
      }
      if (min != null && !Double.isFinite(min)) {
        throw new SearchTemplateException(
            "Search template fieldValueScores inputMin must be finite");
      }
      if (max != null && !Double.isFinite(max)) {
        throw new SearchTemplateException(
            "Search template fieldValueScores inputMax must be finite");
      }
      if (min != null && max != null && max <= min) {
        throw new SearchTemplateException(
            "Search template fieldValueScores inputMax must be greater than inputMin");
      }
      if (multiplier != null && (!Double.isFinite(multiplier) || multiplier < 1.0)) {
        throw new SearchTemplateException(
            "Search template fieldValueScores maxMultiplier must be at least 1");
      }
    }
  }

  public static void checkFieldFactors(@Nullable FieldFactorArray factors) {
    if (factors == null || factors.isEmpty()) {
      return;
    }
    Set<String> names = new LinkedHashSet<>();
    for (FieldFactor factor : factors) {
      String name =
          exclusiveName(
              "fieldFactors",
              factor.hasField(),
              factor.hasField() ? factor.getField() : null,
              factor.hasPropertyUrn() ? factor.getPropertyUrn() : null);
      if (!names.add(name)) {
        throw new SearchTemplateException(
            "Search template fieldFactors names " + displayName(name) + " more than once");
      }
      Double value = factor.hasFactor() ? factor.getFactor() : null;
      if (value != null && (!Double.isFinite(value) || value <= 0.0)) {
        throw new SearchTemplateException("Search template fieldFactors factor must be positive");
      }
      Double missing = factor.hasMissing() ? factor.getMissing() : null;
      boolean reciprocal =
          factor.hasModifier() && factor.getModifier() == FieldFactorModifier.RECIPROCAL;
      if (missing != null
          && (!Double.isFinite(missing) || missing < 0.0 || (reciprocal && missing == 0.0))) {
        throw new SearchTemplateException(
            reciprocal
                ? "Search template fieldFactors reciprocal missing must be positive"
                : "Search template fieldFactors missing must be at least 0");
      }
    }
  }

  public static void checkTermWeights(@Nullable TermWeightArray weights) {
    if (weights == null || weights.isEmpty()) {
      return;
    }
    Set<String> names = new LinkedHashSet<>();
    for (TermWeight weight : weights) {
      Urn propertyUrn = weight.hasPropertyUrn() ? weight.getPropertyUrn() : null;
      String name =
          exclusiveName(
              "termWeights",
              weight.hasField(),
              weight.hasField() ? weight.getField() : null,
              propertyUrn);
      String subfield = text(weight.hasSubfield() ? weight.getSubfield() : null);
      if (subfield != null && propertyUrn == null) {
        throw new SearchTemplateException(
            "Search template termWeights subfield is only valid on a structured property");
      }
      if (subfield != null
          && !StructuredPropertyReferences.KEYWORD.equals(subfield)
          && !StructuredPropertyReferences.TEXT.equals(subfield)) {
        throw new SearchTemplateException(
            "Search template termWeights subfield is not valid: " + subfield);
      }
      String value = text(weight.getValue());
      if (value == null) {
        throw new SearchTemplateException(
            "Search template termWeights value must be one JSON value");
      }
      JsonNode scalar = jsonScalar(value);
      if (!names.add(name + "\0" + scalar.toString() + "\0" + subfield)) {
        throw new SearchTemplateException(
            "Search template termWeights names "
                + displayName(name)
                + " and "
                + scalar
                + " more than once");
      }
      Double factor = weight.getWeight();
      if (factor == null || !Double.isFinite(factor) || factor <= 0.0) {
        throw new SearchTemplateException("Search template termWeights weight must be positive");
      }
    }
  }

  public static void checkEntityBoosts(@Nullable EntityTypeBoostArray boosts) {
    if (boosts == null || boosts.isEmpty()) {
      return;
    }
    Set<String> types = new LinkedHashSet<>();
    for (EntityTypeBoost boost : boosts) {
      String type = text(boost.getEntityType(GetMode.NULL));
      Double weight = boost.getWeight(GetMode.NULL);
      if (type == null) {
        throw new SearchTemplateException(
            "Search template entityTypeBoosts requires an entity type");
      }
      if (weight == null || !Double.isFinite(weight) || weight <= 0.0) {
        throw new SearchTemplateException(
            "Search template entityTypeBoosts weight must be positive");
      }
      if (!types.add(type)) {
        throw new SearchTemplateException(
            "Search template entityTypeBoosts names " + type + " more than once");
      }
    }
  }

  /**
   * Combined extents. A value set on the template replaces {@code defined}. Empty when the two
   * together still lack a finite range with {@code inputMax} greater than {@code inputMin} and a
   * multiplier of at least 1.
   */
  @Nullable
  public static Extent extents(
      @Nonnull FieldValueScore score, @Nullable StructuredPropertyScore defined) {
    Double min = score.hasInputMin() ? score.getInputMin() : null;
    Double max = score.hasInputMax() ? score.getInputMax() : null;
    Double multiplier = score.hasMaxMultiplier() ? score.getMaxMultiplier() : null;
    if (defined != null) {
      if (min == null && defined.hasInputMin()) {
        min = defined.getInputMin();
      }
      if (max == null && defined.hasInputMax()) {
        max = defined.getInputMax();
      }
      if (multiplier == null && defined.hasMaxMultiplier()) {
        multiplier = defined.getMaxMultiplier();
      }
    }
    if (min == null
        || max == null
        || multiplier == null
        || !Double.isFinite(min)
        || !Double.isFinite(max)
        || !Double.isFinite(multiplier)
        || max <= min
        || multiplier < 1.0) {
      return null;
    }
    return new Extent(min, max, multiplier);
  }

  /**
   * Weights to multiply. Empty when the section is absent, the request names one entity type, or
   * every selected weight is 1.
   */
  @Nonnull
  public static List<EntityWeight> entityBoosts(
      @Nullable EntityTypeBoostArray boosts, int distinctEntityTypes) {
    if (boosts == null || boosts.isEmpty() || distinctEntityTypes <= 1) {
      return List.of();
    }
    List<EntityWeight> weights = new ArrayList<>();
    for (EntityTypeBoost boost : boosts) {
      String type = text(boost.getEntityType(GetMode.NULL));
      Double weight = boost.getWeight(GetMode.NULL);
      if (type == null || weight == null) {
        continue;
      }
      addWeight(weights, type, weight);
    }
    return List.copyOf(weights);
  }

  private static void addWeight(
      @Nonnull List<EntityWeight> weights, @Nonnull String entityType, double weight) {
    if (!Double.isFinite(weight) || weight <= 0.0) {
      return;
    }
    float factor = (float) weight;
    if (Float.compare(factor, 1.0f) == 0) {
      return;
    }
    weights.add(new EntityWeight(entityType, factor));
  }

  /**
   * Field or structured-property key. An entry names exactly one. A field key is prefixed so it
   * cannot collide with a URN.
   */
  @Nonnull
  private static String exclusiveName(
      @Nonnull String section,
      boolean hasField,
      @Nullable String rawField,
      @Nullable Urn propertyUrn) {
    String field = text(hasField ? rawField : null);
    if (hasField && field == null) {
      throw new SearchTemplateException(
          "Search template " + section + " field must be a field name");
    }
    if (field != null && propertyUrn != null) {
      throw new SearchTemplateException(
          "Search template " + section + " names a field or a structured property, not both");
    }
    if (field == null && propertyUrn == null) {
      throw new SearchTemplateException(
          "Search template " + section + " names a field or a structured property");
    }
    if (field != null) {
      if (!FIELD.matcher(field).matches()) {
        throw new SearchTemplateException(
            "Search template " + section + " field is not a field name: " + field);
      }
      return "field:" + field;
    }
    if (!STRUCTURED_PROPERTY_ENTITY_NAME.equals(propertyUrn.getEntityType())) {
      throw new SearchTemplateException(
          "Search template " + section + " property must be a structuredProperty URN");
    }
    return propertyUrn.toString();
  }

  @Nonnull
  private static String displayName(@Nonnull String key) {
    return key.startsWith("field:") ? key.substring("field:".length()) : key;
  }

  @Nonnull
  private static JsonNode jsonScalar(@Nonnull String value) {
    try {
      JsonNode node = SearchTemplateMappers.JSON.readTree(value);
      if (node == null || node.isNull() || !node.isValueNode()) {
        throw new SearchTemplateException(
            "Search template termWeights value must be one JSON value");
      }
      return node;
    } catch (IOException e) {
      throw new SearchTemplateException("Search template termWeights value must be one JSON value");
    }
  }

  @Nullable
  private static String text(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
