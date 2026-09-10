package com.datahub.authorization.fieldresolverprovider;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides field resolver for glossary terms/nodes given entitySpec.
 *
 * <p>Returns: - For glossaryTerm entities: the term URN + all ancestor node URNs - For glossaryNode
 * entities: the node URN + all ancestor node URNs - For other entities (datasets, dashboards,
 * etc.): all applied glossary term URNs + all ancestor node URNs of those terms
 */
@Slf4j
@RequiredArgsConstructor
public class GlossaryFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.GLOSSARY);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, spec -> getGlossary(opContext, spec));
  }

  @Nonnull
  private Set<Urn> expandGlossaryAncestors(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> roots) {
    if (roots.isEmpty()) {
      return Collections.emptySet();
    }
    final long startTime = System.nanoTime();
    Set<Urn> expanded =
        BoundHierarchyAccess.expandAncestors(
            opContext, HierarchyBindings.glossarySpec(opContext), roots);
    final long durationNanos = System.nanoTime() - startTime;
    log.debug(
        "expandGlossaryAncestors: {} input roots -> {} total entities ({}ms)",
        roots.size(),
        expanded.size(),
        durationNanos / 1_000_000);
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils -> {
              metricUtils.recordTimer("glossary.policy.resolve_terms.duration", durationNanos);
              metricUtils.recordDistribution(
                  "glossary.policy.term_expansion_ratio",
                  expanded.size() / Math.max(1, roots.size()));
            });
    return expanded;
  }

  private FieldResolver.FieldValue getGlossary(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    final long startTime = System.nanoTime();
    String resolvedEntityType = "unknown";
    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      final Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());
      final String entityType = entityUrn.getEntityType();
      resolvedEntityType = entityType;

      // Case 1: Entity is a glossary term
      if (GLOSSARY_TERM_ENTITY_NAME.equals(entityType)) {
        Set<Urn> termsWithParents =
            expandGlossaryAncestors(opContext, Collections.singleton(entityUrn));
        recordFieldResolutionMetrics(opContext, startTime);
        log.debug(
            "Glossary field resolution for glossaryTerm {} -> {} entities ({}ms)",
            entityUrn,
            termsWithParents.size(),
            (System.nanoTime() - startTime) / 1_000_000);
        return FieldResolver.FieldValue.builder()
            .values(termsWithParents.stream().map(Object::toString).collect(Collectors.toSet()))
            .build();
      }

      // Case 2: Entity is a glossary node
      if (GLOSSARY_NODE_ENTITY_NAME.equals(entityType)) {
        Set<Urn> nodesWithParents =
            expandGlossaryAncestors(opContext, Collections.singleton(entityUrn));
        recordFieldResolutionMetrics(opContext, startTime);
        log.debug(
            "Glossary field resolution for glossaryNode {} -> {} entities ({}ms)",
            entityUrn,
            nodesWithParents.size(),
            (System.nanoTime() - startTime) / 1_000_000);
        return FieldResolver.FieldValue.builder()
            .values(nodesWithParents.stream().map(Object::toString).collect(Collectors.toSet()))
            .build();
      }

      // Case 3: Entity has glossary terms (dataset, dashboard, etc.)
      EntityResponse response =
          _entityClient.getV2(
              opContext, entityType, entityUrn, Collections.singleton(GLOSSARY_TERMS_ASPECT_NAME));

      if (response == null || !response.getAspects().containsKey(GLOSSARY_TERMS_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }

      Set<Urn> appliedTerms =
          new GlossaryTerms(response.getAspects().get(GLOSSARY_TERMS_ASPECT_NAME).getValue().data())
              .getTerms().stream()
                  .map(association -> association.getUrn())
                  .collect(Collectors.toSet());

      Set<Urn> allGlossaryEntities = expandGlossaryAncestors(opContext, appliedTerms);

      recordFieldResolutionMetrics(opContext, startTime);
      log.debug(
          "Glossary field resolution for {} {} with {} applied terms -> {} entities ({}ms)",
          entityType,
          entityUrn,
          appliedTerms.size(),
          allGlossaryEntities.size(),
          (System.nanoTime() - startTime) / 1_000_000);

      return FieldResolver.FieldValue.builder()
          .values(allGlossaryEntities.stream().map(Object::toString).collect(Collectors.toSet()))
          .build();

    } catch (Exception e) {
      final long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      log.error(
          "Error while retrieving glossary for entitySpec {} (entityType: {}, {}ms)",
          entitySpec,
          resolvedEntityType,
          durationMs,
          e);
      return FieldResolver.emptyFieldValue();
    }
  }

  private static void recordFieldResolutionMetrics(
      @Nonnull OperationContext opContext, long startTimeNanos) {
    final long durationNanos = System.nanoTime() - startTimeNanos;
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils -> {
              metricUtils.recordTimer("glossary.policy.field_resolution.duration", durationNanos);
              metricUtils.incrementMicrometer("glossary.policy.field_checks", 1);
            });
  }
}
