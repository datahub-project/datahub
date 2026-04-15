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
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

  /**
   * Given a set of glossary term URNs, recursively resolve all parent nodes. Returns the terms plus
   * all their ancestor nodes.
   */
  private Set<Urn> resolveTermsWithParentNodes(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> termUrns) {

    final long startTime = System.nanoTime();
    Set<Urn> result = new HashSet<>(termUrns);

    try {
      // Batch fetch all term entities at once
      final long batchFetchStart = System.nanoTime();
      Map<Urn, EntityResponse> termResponses =
          _entityClient.batchGetV2(
              opContext,
              GLOSSARY_TERM_ENTITY_NAME,
              termUrns,
              Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME));
      final long batchFetchDurationMs = (System.nanoTime() - batchFetchStart) / 1_000_000;
      log.debug("Batch fetched {} glossary terms in {}ms", termUrns.size(), batchFetchDurationMs);

      // Extract all parent node URNs
      Set<Urn> parentNodeUrns = new HashSet<>();
      termResponses.forEach(
          (termUrn, response) -> {
            if (response.getAspects().containsKey(GLOSSARY_TERM_INFO_ASPECT_NAME)) {
              GlossaryTermInfo termInfo =
                  new GlossaryTermInfo(
                      response.getAspects().get(GLOSSARY_TERM_INFO_ASPECT_NAME).getValue().data());
              if (termInfo.hasParentNode()) {
                parentNodeUrns.add(termInfo.getParentNode());
              }
            }
          });

      // Recursively resolve all parent nodes
      if (!parentNodeUrns.isEmpty()) {
        result.addAll(resolveNodesWithParentNodes(opContext, parentNodeUrns));
      }

      final long totalDurationNanos = System.nanoTime() - startTime;
      final long totalDurationMs = totalDurationNanos / 1_000_000;
      log.debug(
          "resolveTermsWithParentNodes: {} input terms -> {} total entities ({}ms)",
          termUrns.size(),
          result.size(),
          totalDurationMs);

      // Record metrics
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils -> {
                metricUtils.recordTimer(
                    "glossary.policy.resolve_terms.duration", totalDurationNanos);
                metricUtils.recordDistribution(
                    "glossary.policy.term_expansion_ratio",
                    result.size() / Math.max(1, termUrns.size()));
              });

    } catch (Exception e) {
      log.error("Error resolving terms with parent nodes for {} terms", termUrns.size(), e);
    }

    return result;
  }

  /**
   * Given a set of glossary node URNs, recursively resolve all parent nodes. Returns the nodes plus
   * all their ancestors.
   *
   * <p>This mirrors DomainFieldResolverProvider.resolveDomainsWithParents()
   */
  private Set<Urn> resolveNodesWithParentNodes(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> nodeUrns) {

    final long startTime = System.nanoTime();
    int levelsTraversed = 0;
    Set<Urn> allNodes = new HashSet<>(nodeUrns);
    Set<Urn> currentLevel = new HashSet<>(nodeUrns);

    // Walk up the hierarchy level by level
    while (!currentLevel.isEmpty()) {
      levelsTraversed++;
      try {
        // Batch fetch all nodes at this level
        final long batchFetchStart = System.nanoTime();
        Map<Urn, EntityResponse> nodeResponses =
            _entityClient.batchGetV2(
                opContext,
                GLOSSARY_NODE_ENTITY_NAME,
                currentLevel,
                Collections.singleton(GLOSSARY_NODE_INFO_ASPECT_NAME));
        final long batchFetchDurationMs = (System.nanoTime() - batchFetchStart) / 1_000_000;
        log.debug(
            "Batch fetched {} glossary nodes at level {} in {}ms",
            currentLevel.size(),
            levelsTraversed,
            batchFetchDurationMs);

        Set<Urn> nextLevel = new HashSet<>();
        nodeResponses.forEach(
            (nodeUrn, response) -> {
              if (response.getAspects().containsKey(GLOSSARY_NODE_INFO_ASPECT_NAME)) {
                GlossaryNodeInfo nodeInfo =
                    new GlossaryNodeInfo(
                        response
                            .getAspects()
                            .get(GLOSSARY_NODE_INFO_ASPECT_NAME)
                            .getValue()
                            .data());
                if (nodeInfo.hasParentNode()) {
                  Urn parentNode = nodeInfo.getParentNode();
                  if (!allNodes.contains(parentNode)) {
                    nextLevel.add(parentNode);
                  }
                }
              }
            });

        allNodes.addAll(nextLevel);
        currentLevel = nextLevel;

      } catch (Exception e) {
        log.error("Error resolving node hierarchy for {} nodes", currentLevel.size(), e);
        break;
      }
    }

    final long totalDurationNanos = System.nanoTime() - startTime;
    final long totalDurationMs = totalDurationNanos / 1_000_000;
    final int finalLevelsTraversed = levelsTraversed;
    log.debug(
        "resolveNodesWithParentNodes: {} input nodes -> {} total nodes ({} levels, {}ms)",
        nodeUrns.size(),
        allNodes.size(),
        finalLevelsTraversed,
        totalDurationMs);

    // Record metrics
    opContext
        .getMetricUtils()
        .ifPresent(
            metricUtils -> {
              metricUtils.recordTimer("glossary.policy.resolve_nodes.duration", totalDurationNanos);
              metricUtils.recordDistribution(
                  "glossary.policy.node_hierarchy_depth", finalLevelsTraversed);
            });

    return allNodes;
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
            resolveTermsWithParentNodes(opContext, Collections.singleton(entityUrn));
        final long durationNanos = System.nanoTime() - startTime;
        final long durationMs = durationNanos / 1_000_000;
        log.debug(
            "Glossary field resolution for glossaryTerm {} -> {} entities ({}ms)",
            entityUrn,
            termsWithParents.size(),
            durationMs);

        // Record metrics
        opContext
            .getMetricUtils()
            .ifPresent(
                metricUtils -> {
                  metricUtils.recordTimer(
                      "glossary.policy.field_resolution.duration", durationNanos);
                  metricUtils.incrementMicrometer("glossary.policy.field_checks", 1);
                });

        return FieldResolver.FieldValue.builder()
            .values(termsWithParents.stream().map(Object::toString).collect(Collectors.toSet()))
            .build();
      }

      // Case 2: Entity is a glossary node
      if (GLOSSARY_NODE_ENTITY_NAME.equals(entityType)) {
        Set<Urn> nodesWithParents =
            resolveNodesWithParentNodes(opContext, Collections.singleton(entityUrn));
        final long durationNanos = System.nanoTime() - startTime;
        final long durationMs = durationNanos / 1_000_000;
        log.debug(
            "Glossary field resolution for glossaryNode {} -> {} entities ({}ms)",
            entityUrn,
            nodesWithParents.size(),
            durationMs);

        // Record metrics
        opContext
            .getMetricUtils()
            .ifPresent(
                metricUtils -> {
                  metricUtils.recordTimer(
                      "glossary.policy.field_resolution.duration", durationNanos);
                  metricUtils.incrementMicrometer("glossary.policy.field_checks", 1);
                });

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

      // Get all applied terms
      Set<Urn> appliedTerms =
          new GlossaryTerms(response.getAspects().get(GLOSSARY_TERMS_ASPECT_NAME).getValue().data())
              .getTerms().stream()
                  .map(association -> association.getUrn())
                  .collect(Collectors.toSet());

      // Resolve all terms with their parent nodes
      Set<Urn> allGlossaryEntities = resolveTermsWithParentNodes(opContext, appliedTerms);

      final long durationNanos = System.nanoTime() - startTime;
      final long durationMs = durationNanos / 1_000_000;
      log.debug(
          "Glossary field resolution for {} {} with {} applied terms -> {} entities ({}ms)",
          entityType,
          entityUrn,
          appliedTerms.size(),
          allGlossaryEntities.size(),
          durationMs);

      // Record metrics
      opContext
          .getMetricUtils()
          .ifPresent(
              metricUtils -> {
                metricUtils.recordTimer("glossary.policy.field_resolution.duration", durationNanos);
                metricUtils.incrementMicrometer("glossary.policy.field_checks", 1);
              });

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
}
