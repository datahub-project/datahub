package com.linkedin.metadata.structuredproperties.hooks;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class PropertyDefinitionDeleteSideEffect extends MCPSideEffect {
  public static final Integer SEARCH_SCROLL_SIZE = 1000;
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      Collection<MCLItem> mclItems, @Nonnull RetrieverContext retrieverContext) {
    return mclItems.stream().flatMap(item -> generatePatchRemove(item, retrieverContext));
  }

  private static Stream<MCPItem> generatePatchRemove(
      MCLItem mclItem, @Nonnull RetrieverContext retrieverContext) {

    if (STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME.equals(mclItem.getAspectName())) {
      return generatePatchMCPs(
          mclItem.getUrn(),
          mclItem.getPreviousAspect(StructuredPropertyDefinition.class),
          mclItem.getAuditStamp(),
          retrieverContext);
    } else if (STRUCTURED_PROPERTY_KEY_ASPECT_NAME.equals(mclItem.getAspectName())) {
      Aspect definitionAspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(mclItem.getUrn(), STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
      return generatePatchMCPs(
          mclItem.getUrn(),
          definitionAspect == null
              ? null
              : new StructuredPropertyDefinition(definitionAspect.data()),
          mclItem.getAuditStamp(),
          retrieverContext);
    }
    log.warn(
        "Expected either {} or {} aspects but got {}",
        STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME,
        STRUCTURED_PROPERTY_KEY_ASPECT_NAME,
        mclItem.getAspectName());
    return Stream.empty();
  }

  private static Stream<MCPItem> generatePatchMCPs(
      Urn propertyUrn,
      @Nullable StructuredPropertyDefinition definition,
      @Nullable AuditStamp auditStamp,
      @Nonnull RetrieverContext retrieverContext) {
    EntityWithPropertyIterator iterator =
        EntityWithPropertyIterator.builder()
            .propertyUrn(propertyUrn)
            .definition(definition)
            .searchRetriever(retrieverContext.getSearchRetriever())
            .count(SEARCH_SCROLL_SIZE)
            .build();
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
        .flatMap(
            scrollResult ->
                scrollResult.getEntities().stream()
                    .map(
                        entity -> {
                          GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
                          patchOp.setOp(PatchOperationType.REMOVE.getValue());
                          patchOp.setPath(String.format("/properties/%s", propertyUrn.toString()));

                          EntitySpec entitySpec =
                              retrieverContext
                                  .getAspectRetriever()
                                  .getEntityRegistry()
                                  .getEntitySpec(entity.getEntity().getEntityType());
                          return PatchItemImpl.builder()
                              .urn(entity.getEntity())
                              .entitySpec(entitySpec)
                              .aspectName(STRUCTURED_PROPERTIES_ASPECT_NAME)
                              .aspectSpec(
                                  entitySpec.getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME))
                              .patch(
                                  GenericJsonPatch.builder()
                                      .arrayPrimaryKeys(
                                          Map.of("properties", List.of("propertyUrn")))
                                      .patch(List.of(patchOp))
                                      .build()
                                      .getJsonPatch())
                              .auditStamp(auditStamp)
                              .build(retrieverContext.getAspectRetriever().getEntityRegistry());
                        }));
  }

  /**
   * Fetches pages of entity urns which have a value for the given structured property definition
   */
  @Builder
  public static class EntityWithPropertyIterator implements Iterator<ScrollResult> {
    @Nonnull private final Urn propertyUrn;
    @Nullable private final StructuredPropertyDefinition definition;
    @Nonnull private final SearchRetriever searchRetriever;
    private int count;
    @Builder.Default private String scrollId = null;
    @Builder.Default private boolean started = false;

    private List<String> getEntities() {
      if (definition != null && definition.getEntityTypes() != null) {
        return definition.getEntityTypes().stream()
            .map(StructuredPropertyUtils::getValueTypeId)
            .collect(Collectors.toList());
      } else {
        return Collections.emptyList();
      }
    }

    private Filter getFilter() {
      Filter propertyFilter = new Filter();
      final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
      final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
      final CriterionArray andCriterion = new CriterionArray();

      // Cannot rely on automatic field name since the definition is deleted
      final Criterion propertyExistsCriterion =
          buildExistsCriterion(
              STRUCTURED_PROPERTY_MAPPING_FIELD_PREFIX
                  + StructuredPropertyUtils.toElasticsearchFieldName(propertyUrn, definition));

      andCriterion.add(propertyExistsCriterion);
      conjunction.setAnd(andCriterion);
      disjunction.add(conjunction);
      propertyFilter.setOr(disjunction);

      return propertyFilter;
    }

    @Override
    public boolean hasNext() {
      return !started || scrollId != null;
    }

    @Override
    public ScrollResult next() {
      started = true;
      ScrollResult result = searchRetriever.scroll(getEntities(), getFilter(), scrollId, count);
      scrollId = result.getScrollId();
      return result;
    }
  }
}
