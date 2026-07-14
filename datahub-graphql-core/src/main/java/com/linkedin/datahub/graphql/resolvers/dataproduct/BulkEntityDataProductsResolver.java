package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BulkEntityDataProductsInput;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityDataProducts;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.search.utils.QueryUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used to fetch the Data Products that directly contain each of a set of entities, in a
 * single request. Used by the data product lineage visualization, which must know the data product
 * membership of every node in the graph, without fetching relationships for each entity as part of
 * the standard lineage queries.
 *
 * <p>Membership is resolved in bulk rather than per entity: siblings are read in one batch, then a
 * single scroll over the {@code DataProductContains} graph relationship resolves the data products
 * for every entity and sibling at once. An entity is reported as belonging to (and being an output
 * port of) any data product that contains it <i>or one of its siblings</i>.
 */
@Slf4j
public class BulkEntityDataProductsResolver
    implements DataFetcher<CompletableFuture<List<EntityDataProducts>>> {

  private static final String DATA_PRODUCT_CONTAINS_RELATIONSHIP = "DataProductContains";
  private static final int MAX_URNS = 100;
  // Safety net for the sibling-expanded set that drives the relationship scroll's urn filter. Set
  // well above MAX_URNS since each input entity may contribute several siblings.
  private static final int MAX_URNS_WITH_SIBLINGS = 1000;
  // Page size for scrolling the DataProductContains relationship. An entity is typically in at most
  // one data product, so a single page comfortably covers the input batch and their siblings.
  private static final int RELATIONSHIP_SCROLL_COUNT = 1000;

  private final EntityClient _entityClient;

  public BulkEntityDataProductsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<List<EntityDataProducts>> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final OperationContext opContext = context.getOperationContext();
    final BulkEntityDataProductsInput input =
        bindArgument(environment.getArgument("input"), BulkEntityDataProductsInput.class);

    final List<String> inputUrns = input.getUrns();
    if (inputUrns.size() > MAX_URNS) {
      throw new IllegalArgumentException(
          String.format("Cannot fetch data products for more than %s entities at once", MAX_URNS));
    }
    // Parse up front so a malformed urn fails fast, synchronously, as a client error rather than
    // escaping from the async supplier below.
    final List<Urn> parsedUrns =
        inputUrns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // 1. Read each input entity's siblings in one batched aspect read.
          final Map<String, Set<String>> siblingsByEntity = getSiblings(opContext, parsedUrns);

          // 2. Every entity plus its siblings, since a sibling's data product membership counts
          // too. Bounded well above MAX_URNS as a safety net on the set that drives the scroll.
          final Set<String> allUrns = new HashSet<>(inputUrns);
          siblingsByEntity.values().forEach(allUrns::addAll);
          if (allUrns.size() > MAX_URNS_WITH_SIBLINGS) {
            throw new IllegalStateException(
                String.format(
                    "Resolved %s entities including siblings, exceeding the limit of %s",
                    allUrns.size(), MAX_URNS_WITH_SIBLINGS));
          }

          // 3. Resolve data product membership for all of them in a single relationship scroll.
          final Map<String, Set<String>> dataProductsByEntity =
              getDataProductsByEntity(opContext, allUrns);

          // 4. Load each referenced data product's output-port assets in one batch.
          final Set<String> allDataProductUrns =
              dataProductsByEntity.values().stream()
                  .flatMap(Set::stream)
                  .collect(Collectors.toSet());
          final Map<String, Set<String>> outputPortsByDataProduct =
              getOutputPortsByDataProduct(opContext, allDataProductUrns);

          // 5. Assemble results, folding in siblings' memberships and output-port status.
          return inputUrns.stream()
              .map(
                  urn ->
                      buildEntityDataProducts(
                          urn,
                          siblingsByEntity.getOrDefault(urn, Collections.emptySet()),
                          dataProductsByEntity,
                          outputPortsByDataProduct))
              .collect(Collectors.toList());
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /** Reads the siblings of each urn via batched aspect reads, grouped by entity type. */
  private Map<String, Set<String>> getSiblings(
      @Nonnull final OperationContext opContext, @Nonnull final List<Urn> urns) {
    final Map<String, Set<Urn>> urnsByEntityType = new HashMap<>();
    urns.forEach(
        urn ->
            urnsByEntityType.computeIfAbsent(urn.getEntityType(), k -> new HashSet<>()).add(urn));

    final Map<String, Set<String>> result = new HashMap<>();
    urnsByEntityType.forEach(
        (entityType, typedUrns) -> {
          try {
            final Map<Urn, EntityResponse> responses =
                _entityClient.batchGetV2(
                    opContext,
                    entityType,
                    typedUrns,
                    Collections.singleton(Constants.SIBLINGS_ASPECT_NAME));
            responses.forEach(
                (urn, response) -> {
                  final Set<String> siblings = extractSiblings(response);
                  if (!siblings.isEmpty()) {
                    result.put(urn.toString(), siblings);
                  }
                });
          } catch (Exception e) {
            // Best-effort: without siblings we simply report direct membership only.
            log.error("Failed to resolve siblings for entities of type {}", entityType, e);
          }
        });
    return result;
  }

  private Set<String> extractSiblings(final EntityResponse response) {
    if (response == null || !response.getAspects().containsKey(Constants.SIBLINGS_ASPECT_NAME)) {
      return Collections.emptySet();
    }
    final DataMap data =
        response.getAspects().get(Constants.SIBLINGS_ASPECT_NAME).getValue().data();
    final Siblings siblings = new Siblings(data);
    if (!siblings.hasSiblings()) {
      return Collections.emptySet();
    }
    return siblings.getSiblings().stream().map(Urn::toString).collect(Collectors.toSet());
  }

  /**
   * Maps each entity urn to the data products that directly contain it, resolved in a single scroll
   * over the {@code DataProductContains} relationship (incoming to the entity).
   */
  private Map<String, Set<String>> getDataProductsByEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Set<String> entityUrns) {
    final Map<String, Set<String>> result = new HashMap<>();
    if (entityUrns.isEmpty()) {
      return result;
    }

    final GraphRetriever graphRetriever = opContext.getRetrieverContext().getGraphRetriever();
    // A data product typically contains many of the input entities, so cache the view check to
    // avoid re-authorizing the same data product for every edge and scroll page.
    final Map<String, Boolean> viewableByDataProduct = new HashMap<>();
    String scrollId = null;
    do {
      final RelatedEntitiesScrollResult scroll =
          graphRetriever.scrollRelatedEntities(
              null,
              QueryUtils.newFilter(QueryUtils.newCriterion("urn", new ArrayList<>(entityUrns))),
              null,
              EMPTY_FILTER,
              Set.of(DATA_PRODUCT_CONTAINS_RELATIONSHIP),
              QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              Collections.emptyList(),
              scrollId,
              RELATIONSHIP_SCROLL_COUNT,
              null,
              null);

      for (RelatedEntities related : scroll.getEntities()) {
        // Incoming DataProductContains: source is the data product, destination is the entity.
        final String dataProductUrn = related.getSourceUrn();
        final boolean viewable =
            viewableByDataProduct.computeIfAbsent(
                dataProductUrn, urn -> canView(opContext, UrnUtils.getUrn(urn)));
        if (viewable) {
          result
              .computeIfAbsent(related.getDestinationUrn(), k -> new HashSet<>())
              .add(dataProductUrn);
        }
      }

      scrollId = scroll.getEntities().isEmpty() ? null : scroll.getScrollId();
    } while (scrollId != null);

    return result;
  }

  /** Maps each data product urn to the set of asset urns it marks as output ports. */
  private Map<String, Set<String>> getOutputPortsByDataProduct(
      @Nonnull final OperationContext opContext, @Nonnull final Set<String> dataProductUrns) {
    if (dataProductUrns.isEmpty()) {
      return Collections.emptyMap();
    }
    try {
      final Map<Urn, EntityResponse> responses =
          _entityClient.batchGetV2(
              opContext,
              Constants.DATA_PRODUCT_ENTITY_NAME,
              dataProductUrns.stream().map(UrnUtils::getUrn).collect(Collectors.toSet()),
              Collections.singleton(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME));

      final Map<String, Set<String>> result = new HashMap<>();
      responses.forEach(
          (dataProductUrn, response) -> {
            if (response == null
                || !response
                    .getAspects()
                    .containsKey(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)) {
              return;
            }
            final DataMap data =
                response
                    .getAspects()
                    .get(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
                    .getValue()
                    .data();
            final DataProductProperties properties = new DataProductProperties(data);
            if (!properties.hasAssets()) {
              return;
            }
            result.put(
                dataProductUrn.toString(),
                properties.getAssets().stream()
                    .filter(DataProductAssociation::isOutputPort)
                    .map(association -> association.getDestinationUrn().toString())
                    .collect(Collectors.toSet()));
          });
      return result;
    } catch (Exception e) {
      // Output-port badges are best-effort; membership is still returned if this fails.
      log.error("Failed to resolve output ports for data products {}", dataProductUrns, e);
      return Collections.emptyMap();
    }
  }

  // Package-private for unit testing of the sibling-folding and output-port assembly logic.
  EntityDataProducts buildEntityDataProducts(
      @Nonnull final String urn,
      @Nonnull final Set<String> siblings,
      @Nonnull final Map<String, Set<String>> dataProductsByEntity,
      @Nonnull final Map<String, Set<String>> outputPortsByDataProduct) {
    // The entity and its siblings are interchangeable for data product membership.
    final Set<String> entityGroup = new HashSet<>(siblings);
    entityGroup.add(urn);

    final Set<String> dataProductUrns = new LinkedHashSet<>();
    entityGroup.forEach(
        member ->
            dataProductUrns.addAll(
                dataProductsByEntity.getOrDefault(member, Collections.emptySet())));

    final EntityDataProducts result = new EntityDataProducts();
    result.setUrn(urn);
    result.setDataProducts(
        dataProductUrns.stream()
            .map(
                dataProductUrn -> {
                  final DataProduct dataProduct = new DataProduct();
                  dataProduct.setUrn(dataProductUrn);
                  dataProduct.setType(EntityType.DATA_PRODUCT);
                  return dataProduct;
                })
            .collect(Collectors.toList()));

    // A data product is an output port for this entity if it marks the entity, or any of its
    // siblings, as an output port.
    result.setOutputPortInDataProducts(
        dataProductUrns.stream()
            .filter(
                dataProductUrn -> {
                  final Set<String> outputPorts =
                      outputPortsByDataProduct.getOrDefault(dataProductUrn, Collections.emptySet());
                  return entityGroup.stream().anyMatch(outputPorts::contains);
                })
            .collect(Collectors.toList()));
    return result;
  }
}
