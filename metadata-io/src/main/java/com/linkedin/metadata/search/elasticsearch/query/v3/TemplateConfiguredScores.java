package com.linkedin.metadata.search.elasticsearch.query.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMappers;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateScoreDefaults;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateScoreDefaults.EntityWeight;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateScoreDefaults.Extent;
import com.linkedin.search.template.FieldFactor;
import com.linkedin.search.template.FieldValueScore;
import com.linkedin.search.template.TermWeight;
import com.linkedin.structured.PropertyCardinality;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;

/** Multiplies a rendered search v3 query by the template's score sections. */
@Slf4j
public final class TemplateConfiguredScores {
  private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

  private TemplateConfiguredScores() {}

  @Nonnull
  public static QueryBuilder apply(
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull QueryBuilder query) {
    int distinctTypes = (int) entitySpecs.stream().map(EntitySpec::getName).distinct().count();
    QueryBuilder boosted =
        entityBoosts(
            query,
            SearchTemplateScoreDefaults.entityBoosts(
                template.getEntityTypeBoosts(), distinctTypes));
    List<ResolvedFieldValueScore> resolved = new ArrayList<>();
    if (template.getFieldValueScores() != null) {
      for (FieldValueScore score : template.getFieldValueScores()) {
        resolveFieldValue(opContext, score).ifPresent(resolved::add);
      }
    }
    return multiplyFieldValues(boosted, resolved);
  }

  /**
   * Functions appended inside the template function score, plus mode overrides. Field factors come
   * before term weights. A missing property definition is skipped.
   */
  @Nonnull
  public static SearchTemplateJson.FunctionScoreTail functionScoreTail(
      @Nonnull OperationContext opContext, @Nonnull MergedSearchTemplate template) {
    List<JsonNode> functions = new ArrayList<>();
    if (template.getFieldFactors() != null) {
      for (FieldFactor factor : template.getFieldFactors()) {
        String field = literal(factor.hasField() ? factor.getField() : null);
        if (field == null && factor.hasPropertyUrn() && factor.getPropertyUrn() != null) {
          field = numberField(opContext, factor.getPropertyUrn(), "Field factor").orElse(null);
        }
        if (field == null) {
          continue;
        }
        functions.add(
            SearchTemplateJson.fieldFactorFunction(
                field,
                factor.hasFactor() ? factor.getFactor() : null,
                factor.hasMissing() ? factor.getMissing() : null,
                modifier(factor)));
      }
    }
    if (template.getTermWeights() != null) {
      for (TermWeight weight : template.getTermWeights()) {
        termFunction(opContext, weight).ifPresent(functions::add);
      }
    }
    return new SearchTemplateJson.FunctionScoreTail(
        functions, modeName(template.getScoreMode()), modeName(template.getBoostMode()));
  }

  /** One resolved numeric multiplier. Several compose by nesting the browse clamp. */
  public record ResolvedFieldValueScore(
      @Nonnull String field, double inputMin, double inputMax, double maxMultiplier) {}

  @Nonnull
  public static QueryBuilder multiplyFieldValues(
      @Nonnull QueryBuilder query, @Nonnull List<ResolvedFieldValueScore> scores) {
    QueryBuilder boosted = query;
    for (ResolvedFieldValueScore score : scores) {
      Optional<QueryBuilder> wrapped =
          ESSearchDAO.structuredPropertyScoreBrowseQuery(
              boosted, score.field(), score.inputMin(), score.inputMax(), score.maxMultiplier());
      if (wrapped.isPresent()) {
        boosted = wrapped.get();
      }
    }
    return boosted;
  }

  @Nonnull
  public static QueryBuilder entityBoosts(
      @Nonnull QueryBuilder query, @Nonnull List<EntityWeight> weights) {
    if (weights.isEmpty()) {
      return query;
    }
    FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions =
        weights.stream()
            .map(
                weight ->
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery(INDEX_VIRTUAL_FIELD, weight.entityType()),
                        ScoreFunctionBuilders.weightFactorFunction(weight.weight())))
            .toArray(FunctionScoreQueryBuilder.FilterFunctionBuilder[]::new);
    return QueryBuilders.functionScoreQuery(query, functions).boostMode(CombineFunction.MULTIPLY);
  }

  /**
   * Resolved multiplier for one template entry. A raw field uses the extents on the entry. A
   * property fills each omitted extent from its definition, and a value on the entry replaces the
   * definition. Empty when the property is not a single number or the range is unusable.
   */
  @Nonnull
  private static Optional<ResolvedFieldValueScore> resolveFieldValue(
      @Nonnull OperationContext opContext, @Nonnull FieldValueScore score) {
    String field = literal(score.hasField() ? score.getField() : null);
    if (field != null) {
      return resolved(field, SearchTemplateScoreDefaults.extents(score, null));
    }
    Urn propertyUrn = score.hasPropertyUrn() ? score.getPropertyUrn() : null;
    if (propertyUrn == null) {
      return Optional.empty();
    }
    Optional<StructuredPropertyDefinition> loaded =
        definition(opContext, propertyUrn, "Field value score");
    if (loaded.isEmpty() || !scorable(loaded.get(), propertyUrn, "Field value score")) {
      return Optional.empty();
    }
    StructuredPropertyDefinition property = loaded.get();
    return resolved(
        indexField(propertyUrn, property),
        SearchTemplateScoreDefaults.extents(
            score, property.hasScore() ? property.getScore() : null));
  }

  @Nonnull
  private static Optional<ResolvedFieldValueScore> resolved(
      @Nonnull String field, @Nullable Extent extent) {
    if (extent == null) {
      return Optional.empty();
    }
    return Optional.of(
        new ResolvedFieldValueScore(
            field, extent.inputMin(), extent.inputMax(), extent.maxMultiplier()));
  }

  @Nonnull
  private static Optional<String> numberField(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String label) {
    Optional<StructuredPropertyDefinition> loaded = definition(opContext, urn, label);
    if (loaded.isEmpty() || !scorable(loaded.get(), urn, label)) {
      return Optional.empty();
    }
    return Optional.of(indexField(urn, loaded.get()));
  }

  private static boolean scorable(
      @Nonnull StructuredPropertyDefinition property, @Nonnull Urn urn, @Nonnull String label) {
    if (StructuredPropertyUtils.getLogicalValueType(property) != LogicalValueType.NUMBER) {
      warn("{} ignores '{}': no number-typed definition found", label, urn);
      return false;
    }
    if (property.getCardinality() == PropertyCardinality.MULTIPLE) {
      warn("{} ignores '{}': the property allows multiple values", label, urn);
      return false;
    }
    return true;
  }

  @Nonnull
  private static Optional<JsonNode> termFunction(
      @Nonnull OperationContext opContext, @Nonnull TermWeight weight) {
    String field = literal(weight.hasField() ? weight.getField() : null);
    if (field == null) {
      Urn urn = weight.hasPropertyUrn() ? weight.getPropertyUrn() : null;
      if (urn == null) {
        return Optional.empty();
      }
      Optional<StructuredPropertyDefinition> loaded = definition(opContext, urn, "Term weight");
      if (loaded.isEmpty()) {
        return Optional.empty();
      }
      String subfield = literal(weight.hasSubfield() ? weight.getSubfield() : null);
      LogicalValueType type = StructuredPropertyUtils.getLogicalValueType(loaded.get());
      if (subfield != null && !StructuredPropertyUtils.usesKeywordSubfield(type)) {
        warn("Term weight ignores '{}': the property has no {} subfield", urn, subfield);
        return Optional.empty();
      }
      field = indexField(urn, loaded.get());
      if (subfield != null) {
        field = field + "." + subfield;
      }
    }
    JsonNode value = scalar(weight.getValue());
    if (value == null) {
      return Optional.empty();
    }
    return Optional.of(SearchTemplateJson.termWeightFunction(field, value, weight.getWeight()));
  }

  @Nonnull
  private static Optional<StructuredPropertyDefinition> definition(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nonnull String label) {
    try {
      if (opContext.getRetrieverContext() == null
          || opContext.getRetrieverContext().getCachingAspectRetriever() == null) {
        return Optional.empty();
      }
      Aspect aspect =
          opContext
              .getRetrieverContext()
              .getCachingAspectRetriever()
              .getLatestAspectObject(opContext, urn, STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
      if (aspect == null || aspect.data() == null) {
        warn("{} ignores '{}': no definition found", label, urn);
        return Optional.empty();
      }
      return Optional.of(new StructuredPropertyDefinition(aspect.data()));
    } catch (RuntimeException e) {
      warn("{} ignores '{}': definition lookup failed: {}", label, urn, e.getMessage());
      return Optional.empty();
    }
  }

  @Nonnull
  private static String indexField(
      @Nonnull Urn urn, @Nonnull StructuredPropertyDefinition definition) {
    return STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
        + StructuredPropertyUtils.toElasticsearchFieldName(urn, definition);
  }

  @Nullable
  private static String modifier(@Nonnull FieldFactor factor) {
    if (!factor.hasModifier() || factor.getModifier() == null) {
      return null;
    }
    return factor.getModifier().name().toLowerCase(Locale.ROOT);
  }

  @Nullable
  private static String modeName(@Nullable Enum<?> mode) {
    return mode == null ? null : mode.name().toLowerCase(Locale.ROOT);
  }

  @Nullable
  private static String literal(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  @Nullable
  private static JsonNode scalar(@Nullable String value) {
    String text = literal(value);
    if (text == null) {
      return null;
    }
    try {
      JsonNode node = SearchTemplateMappers.JSON.readTree(text);
      return node != null && node.isValueNode() && !node.isNull() ? node : null;
    } catch (IOException e) {
      return null;
    }
  }

  private static void warn(String message, Object... args) {
    if (WARNED.add(message + " " + Arrays.toString(args))) {
      log.warn(message, args);
    }
  }
}
