package com.linkedin.metadata.graph;

import static com.linkedin.metadata.search.utils.QueryUtils.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public abstract class GraphServiceTestBaseNoVia extends GraphServiceTestBase {

  @DataProvider(name = "NoViaFindRelatedEntitiesDestinationTypeTests")
  public Object[][] getNoViaFindRelatedEntitiesDestinationTypeTests() {
    return new Object[][] {
      new Object[] {
        null,
        Set.of(downstreamOf),
        outgoingRelationships,
        // All DownstreamOf relationships, outgoing
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            // TODO: Via not supported in Neo4J and DGraph
            downstreamOfSchemaFieldTwo)
      },
      new Object[] {
        null,
        Set.of(downstreamOf),
        incomingRelationships,
        // All DownstreamOf relationships, incoming
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            // TODO: Via not supported in Neo4J and DGraph
            downstreamOfSchemaFieldOne)
      },
      new Object[] {
        null,
        Set.of(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            // TODO: Via not supported in Neo4J and DGraph
            downstreamOfSchemaFieldOne,
            downstreamOfSchemaFieldTwo)
      },
      new Object[] {"", Set.of(downstreamOf), outgoingRelationships, Collections.emptyList()},
      new Object[] {"", Set.of(downstreamOf), incomingRelationships, Collections.emptyList()},
      new Object[] {"", Set.of(downstreamOf), undirectedRelationships, Collections.emptyList()},
      new Object[] {
        datasetType,
        Set.of(downstreamOf),
        outgoingRelationships,
        Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        datasetType,
        Set.of(downstreamOf),
        incomingRelationships,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Set.of(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {datasetType, Set.of(hasOwner), outgoingRelationships, Arrays.asList()},
      new Object[] {
        datasetType,
        Set.of(hasOwner),
        incomingRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Set.of(hasOwner),
        undirectedRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      },
      new Object[] {
        userType,
        Set.of(hasOwner),
        outgoingRelationships,
        Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
      },
      new Object[] {userType, Set.of(hasOwner), incomingRelationships, Arrays.asList()},
      new Object[] {
        userType,
        Set.of(hasOwner),
        undirectedRelationships,
        Arrays.asList(hasOwnerUserOneRelatedEntity, hasOwnerUserTwoRelatedEntity)
      }
    };
  }

  @DataProvider(name = "NoViaFindRelatedEntitiesSourceTypeTests")
  public Object[][] getNoViaFindRelatedEntitiesSourceTypeTests() {
    return new Object[][] {
      // All DownstreamOf relationships, outgoing
      new Object[] {
        null,
        Set.of(downstreamOf),
        outgoingRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            // TODO: DGraph and Neo4J do not support via
            downstreamOfSchemaFieldTwo)
      },
      // All DownstreamOf relationships, incoming
      new Object[] {
        null,
        Set.of(downstreamOf),
        incomingRelationships,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            // TODO: DGraph and Neo4J do not support via
            downstreamOfSchemaFieldOne)
      },
      // All DownstreamOf relationships, both directions
      new Object[] {
        null,
        Set.of(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            // TODO: DGraph and Neo4J do not support via
            downstreamOfSchemaFieldTwo,
            downstreamOfSchemaFieldOne)
      },

      // "" used to be any type before v0.9.0, which is now encoded by null
      new Object[] {"", Set.of(downstreamOf), outgoingRelationships, Collections.emptyList()},
      new Object[] {"", Set.of(downstreamOf), incomingRelationships, Collections.emptyList()},
      new Object[] {"", Set.of(downstreamOf), undirectedRelationships, Collections.emptyList()},
      new Object[] {
        datasetType,
        Set.of(downstreamOf),
        outgoingRelationships,
        Arrays.asList(downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity)
      },
      new Object[] {
        datasetType,
        Set.of(downstreamOf),
        incomingRelationships,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {
        datasetType,
        Set.of(downstreamOf),
        undirectedRelationships,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity, downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity, downstreamOfDatasetFourRelatedEntity)
      },
      new Object[] {userType, Set.of(downstreamOf), outgoingRelationships, Arrays.asList()},
      new Object[] {userType, Set.of(downstreamOf), incomingRelationships, Arrays.asList()},
      new Object[] {userType, Set.of(downstreamOf), undirectedRelationships, Arrays.asList()},
      new Object[] {userType, Set.of(hasOwner), outgoingRelationships, Arrays.asList()},
      new Object[] {
        userType,
        Set.of(hasOwner),
        incomingRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      },
      new Object[] {
        userType,
        Set.of(hasOwner),
        undirectedRelationships,
        Arrays.asList(
            hasOwnerDatasetOneRelatedEntity, hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity, hasOwnerDatasetFourRelatedEntity)
      }
    };
  }

  @Test
  @Override
  public void testFindRelatedEntitiesRelationshipTypes() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult allOutgoingRelatedEntities =
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    // All DownstreamOf relationships, outgoing (destination)
    assertEqualsAnyOrder(
        allOutgoingRelatedEntities,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            hasOwnerUserOneRelatedEntity,
            hasOwnerUserTwoRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            // TODO: DGraph and Neo4J do not support via
            downstreamOfSchemaFieldTwo));

    RelatedEntitiesResult allIncomingRelatedEntities =
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of(downstreamOf, hasOwner, knowsUser),
            incomingRelationships,
            0,
            100);
    // All DownstreamOf relationships, incoming (source)
    assertEqualsAnyOrder(
        allIncomingRelatedEntities,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity,
            hasOwnerDatasetFourRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            // TODO: DGraph and Neo4J do not support via
            downstreamOfSchemaFieldOne));

    RelatedEntitiesResult allUnknownRelationshipTypeRelatedEntities =
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of("unknownRelationshipType", "unseenRelationshipType"),
            outgoingRelationships,
            0,
            100);
    assertEqualsAnyOrder(allUnknownRelationshipTypeRelatedEntities, Collections.emptyList());

    RelatedEntitiesResult someUnknownRelationshipTypeRelatedEntities =
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of("unknownRelationshipType", downstreamOf),
            outgoingRelationships,
            0,
            100);
    // All DownstreamOf relationships, outgoing (destination)
    assertEqualsAnyOrder(
        someUnknownRelationshipTypeRelatedEntities,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            // TODO: DGraph and Neo4J do not support via
            downstreamOfSchemaFieldTwo));
  }

  @Test
  @Override
  public void testPopulatedGraphService() throws Exception {
    GraphService service = getPopulatedGraphService();

    RelatedEntitiesResult relatedOutgoingEntitiesBeforeRemove =
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100);
    // All downstreamOf, hasOwner, or knowsUser relationships, outgoing
    assertEqualsAnyOrder(
        relatedOutgoingEntitiesBeforeRemove,
        Arrays.asList(
            downstreamOfDatasetOneRelatedEntity,
            downstreamOfDatasetTwoRelatedEntity,
            hasOwnerUserOneRelatedEntity,
            hasOwnerUserTwoRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            // TODO: DGraph and Neo4j do not support via
            downstreamOfSchemaFieldTwo));
    RelatedEntitiesResult relatedIncomingEntitiesBeforeRemove =
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of(downstreamOf, hasOwner, knowsUser),
            incomingRelationships,
            0,
            100);
    // All downstreamOf, hasOwner, or knowsUser relationships, incoming
    assertEqualsAnyOrder(
        relatedIncomingEntitiesBeforeRemove,
        Arrays.asList(
            downstreamOfDatasetTwoRelatedEntity,
            downstreamOfDatasetThreeRelatedEntity,
            downstreamOfDatasetFourRelatedEntity,
            hasOwnerDatasetOneRelatedEntity,
            hasOwnerDatasetTwoRelatedEntity,
            hasOwnerDatasetThreeRelatedEntity,
            hasOwnerDatasetFourRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            // TODO: DGraph and Neo4j do not support via
            downstreamOfSchemaFieldOne));
    // TODO: DGraph and Neo4j do not support via
    // No checking of split via edge
  }

  @Test
  @Override
  public void testRemoveNode() throws Exception {
    GraphService service = getPopulatedGraphService();

    service.removeNode(mock(OperationContext.class), dataset2Urn);
    syncAfterWrite();

    // assert the modified graph
    // All downstreamOf, hasOwner, knowsUser relationships minus datasetTwo's, outgoing
    assertEqualsAnyOrder(
        service.findRelatedEntities(
            mock(OperationContext.class),
            anyType,
            EMPTY_FILTER,
            anyType,
            EMPTY_FILTER,
            Set.of(downstreamOf, hasOwner, knowsUser),
            outgoingRelationships,
            0,
            100),
        Arrays.asList(
            hasOwnerUserOneRelatedEntity,
            hasOwnerUserTwoRelatedEntity,
            knowsUserOneRelatedEntity,
            knowsUserTwoRelatedEntity,
            // TODO: DGraph and Neo4j do not support via
            downstreamOfSchemaFieldTwo));
  }
}
