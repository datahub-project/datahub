package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Covers {@link DeleteEntityService}'s handling of delete-time guard rejections: when the entity
 * service rejects an individual aspect deletion with {@link IllegalArgumentException} (e.g. the
 * structured-property soft-delete-first guard), reference cleanup must log, leave that aspect in
 * place, and continue with the remaining references instead of aborting the cascade.
 */
public class DeleteEntityServiceGuardHandlingTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private final EntityService<?> _entityService = mock(EntityService.class);
  private final GraphService _graphService = mock(GraphService.class);
  private final EntitySearchService _searchService = mock(EntitySearchService.class);
  private final DeleteEntityService _deleteEntityService =
      new DeleteEntityService(_entityService, _graphService, _searchService, null, null);

  private static EntityResponse datasetWithContainerAspect(Urn datasetUrn, Urn containerUrn) {
    final Container containerAspect = new Container();
    containerAspect.setContainer(containerUrn);
    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(Constants.CONTAINER_ASPECT_NAME)
            .setValue(new Aspect(containerAspect.data()));
    final EntityResponse response = new EntityResponse();
    response.setUrn(datasetUrn);
    response.setEntityName(datasetUrn.getEntityType());
    response.setAspects(
        new EnvelopedAspectMap(Map.of(Constants.CONTAINER_ASPECT_NAME, envelopedAspect)));
    return response;
  }

  @Test
  public void testGuardRejectionDoesNotAbortReferenceCleanup() throws Exception {
    final Urn container = UrnUtils.getUrn("urn:li:container:d1006cf3-3ff9-48e3-85cd-26eb23775ab2");
    final Urn dataset1 = UrnUtils.toDatasetUrn("snowflake", "guard_test_one", "DEV");
    final Urn dataset2 = UrnUtils.toDatasetUrn("snowflake", "guard_test_two", "DEV");

    // File-reference phase scrolls the search index; return no files.
    final ScrollResult emptyFileScrollResult = new ScrollResult();
    emptyFileScrollResult.setEntities(new SearchEntityArray());
    emptyFileScrollResult.setNumEntities(0);
    when(_searchService.structuredScroll(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(Filter.class),
            isNull(),
            nullable(String.class),
            anyString(),
            anyInt()))
        .thenReturn(emptyFileScrollResult);

    // Two entities reference the deleted container.
    when(_graphService.scrollRelatedEntities(
            any(OperationContext.class),
            nullable(Set.class),
            eq(newFilter("urn", container.toString())),
            nullable(Set.class),
            eq(EMPTY_FILTER),
            eq(ImmutableSet.of()),
            eq(newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING)),
            eq(Edge.EDGE_SORT_CRITERION),
            nullable(String.class),
            eq("5m"),
            eq(1000),
            nullable(Long.class),
            nullable(Long.class)))
        .thenReturn(
            RelatedEntitiesScrollResult.builder()
                .numResults(2)
                .pageSize(2)
                .scrollId(null)
                .entities(
                    ImmutableList.of(
                        new RelatedEntities(
                            "IsPartOf",
                            dataset1.toString(),
                            container.toString(),
                            RelationshipDirection.INCOMING,
                            null),
                        new RelatedEntities(
                            "IsPartOf",
                            dataset2.toString(),
                            container.toString(),
                            RelationshipDirection.INCOMING,
                            null)))
                .build());

    when(_entityService.getEntityV2(
            any(OperationContext.class), eq(dataset1.getEntityType()), eq(dataset1), anySet()))
        .thenReturn(datasetWithContainerAspect(dataset1, container));
    when(_entityService.getEntityV2(
            any(OperationContext.class), eq(dataset2.getEntityType()), eq(dataset2), anySet()))
        .thenReturn(datasetWithContainerAspect(dataset2, container));

    // dataset1's aspect deletion is rejected by a delete-time guard (e.g. the structured-property
    // soft-delete-first precondition maps to IllegalArgumentException)...
    when(_entityService.deleteAspect(
            any(OperationContext.class),
            eq(dataset1.toString()),
            anyString(),
            anyMap(),
            anyBoolean()))
        .thenThrow(
            new IllegalArgumentException(
                "Hard delete rejected for structured property qualifiedName 'io.acryl.example'."));
    // ...while dataset2's deletion succeeds: a deleted-aspect result has newValue == null, so the
    // helper's post-delete error branch is not taken and the success path completes.
    final Container deletedAspect = new Container();
    deletedAspect.setContainer(container);
    when(_entityService.deleteAspect(
            any(OperationContext.class),
            eq(dataset2.toString()),
            anyString(),
            anyMap(),
            anyBoolean()))
        .thenReturn(
            Optional.of(
                new RollbackResult(
                    dataset2,
                    Constants.DATASET_ENTITY_NAME,
                    Constants.CONTAINER_ASPECT_NAME,
                    deletedAspect,
                    null,
                    null,
                    null,
                    ChangeType.DELETE,
                    false,
                    1)));

    // The rejection must not escape deleteReferencesTo.
    final DeleteReferencesResponse response =
        _deleteEntityService.deleteReferencesTo(opContext, container, false);

    assertNotNull(response);
    assertEquals(2, (int) response.getTotal());
    assertFalse(response.getRelatedAspects().isEmpty());

    // The cascade continued past the rejection to a SUCCESSFUL delete: dataset1's rejection was
    // contained, and dataset2's aspect deletion was still attempted and completed.
    verify(_entityService)
        .deleteAspect(
            any(OperationContext.class),
            eq(dataset1.toString()),
            eq(Constants.CONTAINER_ASPECT_NAME),
            anyMap(),
            eq(true));
    verify(_entityService)
        .deleteAspect(
            any(OperationContext.class),
            eq(dataset2.toString()),
            eq(Constants.CONTAINER_ASPECT_NAME),
            anyMap(),
            eq(true));
  }
}
