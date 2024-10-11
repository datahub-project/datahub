package com.linkedin.metadata.test.query;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SearchUtil.INDEX_VIRTUAL_FIELD;
import static com.linkedin.metadata.utils.SearchUtil.URN_FIELD;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Ownership;
import com.linkedin.common.Siblings;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.virtualFields.VirtualFieldsQueryEvaluator;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class QueryEngineTest {
  final EntityService<?> entityService = mock(EntityService.class);
  final QueryVersionedAspectEvaluator queryVersionedAspectEvaluator =
      new QueryVersionedAspectEvaluator(SnapshotEntityRegistry.getInstance(), entityService);
  final EntityUrnTypeEvaluator urnTypeEvaluator = new EntityUrnTypeEvaluator();
  final SystemAspectEvaluator systemAspectEvaluator = new SystemAspectEvaluator(entityService);
  final StructuredPropertyEvaluator structuredPropertyEvaluator =
      new StructuredPropertyEvaluator(entityService);
  final VirtualFieldsQueryEvaluator virtualFieldsQueryEvaluator = new VirtualFieldsQueryEvaluator();
  final QueryEngine _queryEngine =
      new QueryEngine(
          ImmutableList.of(
              urnTypeEvaluator,
              queryVersionedAspectEvaluator,
              systemAspectEvaluator,
              structuredPropertyEvaluator,
              virtualFieldsQueryEvaluator));

  static final DatasetUrn DATASET_URN =
      new DatasetUrn(new DataPlatformUrn("bigquery"), "test_dataset", FabricType.DEV);

  static final DatasetUrn SIBLING_URN =
      new DatasetUrn(new DataPlatformUrn("dbt"), "test_dataset", FabricType.DEV);
  static final GlossaryTermUrn GLOSSARY_TERM_WITH_PARENT = new GlossaryTermUrn("term_with_parent");
  static final GlossaryTermUrn GLOSSARY_TERM_WITHOUT_PARENT =
      new GlossaryTermUrn("term_without_parent");
  static final GlossaryNodeUrn PARENT_NODE = new GlossaryNodeUrn("parent_node");

  static final String TEST_OWNERSHIP_TYPE_1 = "urn:li:ownershipType:dataowner";
  static final String TEST_OWNERSHIP_TYPE_2 = "urn:li:ownershipType:datasteward";
  static final String TEST_OWNERSHIP_TYPE_3 = "urn:li:ownershipType:businessowner";
  static final String TEST_USER_1 = "urn:li:corpuser:testUser1";
  static final String TEST_USER_2 = "urn:li:corpuser:testUser2";
  static final String TEST_USER_3 = "urn:li:corpuser:testUser3";
  private OperationContext opContext;

  @BeforeTest
  public void reset() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    Mockito.reset(entityService);
  }

  @SneakyThrows
  @Test
  public void testEngine() {
    TestQuery testQuery = new TestQuery("datasetProperties.description");
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, emptyEntityResponse()));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createPropertiesWithDescription()));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of("test description")))));
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createPropertiesWithoutDescription()));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());

    testQuery = new TestQuery("glossaryTerms.terms.urn.glossaryTermInfo.parentNode");
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, emptyEntityResponse()));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createGlossaryTerms(Collections.emptyList())));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
                eq(ImmutableSet.of(GLOSSARY_TERM_WITH_PARENT)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                GLOSSARY_TERM_WITH_PARENT,
                createGlossaryTermInfo(GLOSSARY_TERM_WITH_PARENT, PARENT_NODE)));
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
                eq(ImmutableSet.of(GLOSSARY_TERM_WITHOUT_PARENT)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                GLOSSARY_TERM_WITHOUT_PARENT,
                createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)));
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
                eq(ImmutableSet.of(GLOSSARY_TERM_WITH_PARENT, GLOSSARY_TERM_WITHOUT_PARENT)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                GLOSSARY_TERM_WITH_PARENT,
                createGlossaryTermInfo(GLOSSARY_TERM_WITH_PARENT, PARENT_NODE),
                GLOSSARY_TERM_WITHOUT_PARENT,
                createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)));
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN, createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT))));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of(PARENT_NODE.toString())))));
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN, createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITHOUT_PARENT))));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN,
                createGlossaryTerms(
                    ImmutableList.of(GLOSSARY_TERM_WITH_PARENT, GLOSSARY_TERM_WITHOUT_PARENT))));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of(PARENT_NODE.toString())))));
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN,
                createGlossaryTerms(
                    ImmutableList.of(GLOSSARY_TERM_WITH_PARENT, GLOSSARY_TERM_WITH_PARENT))));
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery,
                new TestQueryResponse(
                    ImmutableList.of(PARENT_NODE.toString(), PARENT_NODE.toString())))));

    // Test URN + Entity Type queries

    // Case 1: Base Entity URN
    testQuery = new TestQuery("urn");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of(DATASET_URN.toString())))));
    // Case 2: Base Entity Type
    testQuery = new TestQuery("entityType");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of(DATASET_URN.getEntityType())))));

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                DATASET_URN, createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT))));

    // Case 3: Nested Entity URN
    testQuery = new TestQuery("glossaryTerms.terms.urn.urn");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery,
                new TestQueryResponse(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT.toString())))));
    // Case 4: Nested Entity Type
    testQuery = new TestQuery("glossaryTerms.terms.urn.entityType");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery,
                new TestQueryResponse(
                    ImmutableList.of(GLOSSARY_TERM_WITH_PARENT.getEntityType())))));
    // Case 5: Entity Type query
    testQuery = new TestQuery(INDEX_VIRTUAL_FIELD);
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(GLOSSARY_TERM_WITH_PARENT), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            GLOSSARY_TERM_WITH_PARENT,
            ImmutableMap.of(
                testQuery,
                new TestQueryResponse(
                    ImmutableList.of(
                        "GLOSSARY_TERM", GLOSSARY_TERM_WITH_PARENT.getEntityType())))));
    // Case 6: Urn query
    testQuery = new TestQuery(URN_FIELD);
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(GLOSSARY_TERM_WITH_PARENT), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            GLOSSARY_TERM_WITH_PARENT,
            ImmutableMap.of(
                testQuery,
                new TestQueryResponse(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT.toString())))));
  }

  @SneakyThrows
  @Test
  public void testSystemAspectQueries() {

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                any()))
        .thenReturn(ImmutableMap.of());

    TestQuery testQuery = new TestQuery("__firstSynchronized");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(DATASET_URN);
    final DatasetKey datasetKey = new DatasetKey().setName(DATASET_URN.toString());
    final DatasetProperties datasetProperties =
        new DatasetProperties().setDescription("test description");

    final EnvelopedAspect keyAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_KEY_ASPECT_NAME)
            .setValue(new Aspect(datasetKey.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-1").setLastObserved(2))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(2));

    final EnvelopedAspect propertiesAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect(datasetProperties.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-2").setLastObserved(10))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(10));

    final EnvelopedAspect glossaryTermInfoAspect =
        createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)
            .getAspects()
            .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
            .setSystemMetadata(
                new SystemMetadata().setRunId(Constants.DEFAULT_RUN_ID).setLastObserved(11))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(11));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATASET_PROPERTIES_ASPECT_NAME, propertiesAspect,
                Constants.DATASET_KEY_ASPECT_NAME, keyAspect,
                Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, glossaryTermInfoAspect)));

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                any()))
        .thenReturn(ImmutableMap.of(DATASET_URN, entityResponse));

    // Validate firstSynchronized query (createdOn time for key aspect)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN, ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("2")))));

    testQuery = new TestQuery("__lastSynchronized");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("10")))));

    testQuery = new TestQuery("__lastObserved");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("11")))));

    // Check system aspects when siblings exist

    final Siblings siblings = new Siblings().setSiblings(new UrnArray(List.of(SIBLING_URN)));
    final EnvelopedAspect siblingAspect =
        new EnvelopedAspect()
            .setName(SIBLINGS_ASPECT_NAME)
            .setValue(new Aspect(siblings.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(12))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(11));

    entityResponse.getAspects().put(SIBLINGS_ASPECT_NAME, siblingAspect);

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                any()))
        .thenReturn(ImmutableMap.of(DATASET_URN, entityResponse));

    // But sibling is empty, i.e: We have a sibling reference to something that doesn't exist.
    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(SIBLING_URN)),
                any()))
        .thenReturn(ImmutableMap.of());

    // Validate system query with null sibling are the same when no sibling information appears.
    testQuery = new TestQuery("__firstSynchronized");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN, ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("2")))));

    testQuery = new TestQuery("__lastSynchronized");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("12")))));

    testQuery = new TestQuery("__lastObserved");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("12")))));

    // Validate system query with sibling that have content and appeared *before* main entity

    final EntityResponse siblingEntityResponse = new EntityResponse();
    siblingEntityResponse.setUrn(SIBLING_URN);
    final DatasetKey siblingDatasetKey = new DatasetKey().setName(SIBLING_URN.toString());
    final DatasetProperties siblingDatasetProperties =
        new DatasetProperties().setDescription("test description");

    EnvelopedAspect siblingKeyAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_KEY_ASPECT_NAME)
            .setValue(new Aspect(siblingDatasetKey.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(1))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(1));

    EnvelopedAspect siblingPropertiesAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect(siblingDatasetProperties.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(1))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(1));

    EnvelopedAspect siblingGlossaryTermInfoAspect =
        createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)
            .getAspects()
            .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
            .setSystemMetadata(
                new SystemMetadata().setRunId(Constants.DEFAULT_RUN_ID).setLastObserved(1))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(1));

    EnvelopedAspect siblingSiblingAspect =
        new EnvelopedAspect()
            .setName(SIBLINGS_ASPECT_NAME)
            .setValue(
                new Aspect(new Siblings().setSiblings(new UrnArray(List.of(DATASET_URN))).data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-2").setLastObserved(1))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(1));

    siblingEntityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATASET_PROPERTIES_ASPECT_NAME,
                siblingPropertiesAspect,
                Constants.DATASET_KEY_ASPECT_NAME,
                siblingKeyAspect,
                Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                siblingGlossaryTermInfoAspect,
                SIBLINGS_ASPECT_NAME,
                siblingSiblingAspect)));

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(SIBLING_URN)),
                any()))
        .thenReturn(ImmutableMap.of(SIBLING_URN, siblingEntityResponse));

    testQuery = new TestQuery("__firstSynchronized");
    // first synced from sibling (1) should surface before main entity (2)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN, ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("1")))));

    testQuery = new TestQuery("__lastSynchronized");
    // last synched from main entity (10) should surface instead of sibling (2)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("12")))));

    testQuery = new TestQuery("__lastObserved");
    // last observed from main entity (10) should surface instead of sibling (2)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("12")))));

    // Validate system query with sibling that have content and appeared *after* main entity
    siblingKeyAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_KEY_ASPECT_NAME)
            .setValue(new Aspect(siblingDatasetKey.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(21))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(21));

    siblingPropertiesAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect(siblingDatasetProperties.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(21))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(21));

    siblingGlossaryTermInfoAspect =
        createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)
            .getAspects()
            .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(22))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(22));

    siblingSiblingAspect =
        new EnvelopedAspect()
            .setName(SIBLINGS_ASPECT_NAME)
            .setValue(
                new Aspect(new Siblings().setSiblings(new UrnArray(List.of(DATASET_URN))).data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-3").setLastObserved(21))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(21));

    siblingEntityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(
                Constants.DATASET_PROPERTIES_ASPECT_NAME,
                siblingPropertiesAspect,
                Constants.DATASET_KEY_ASPECT_NAME,
                siblingKeyAspect,
                Constants.GLOSSARY_TERM_INFO_ASPECT_NAME,
                siblingGlossaryTermInfoAspect,
                SIBLINGS_ASPECT_NAME,
                siblingSiblingAspect)));

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(SIBLING_URN)),
                any()))
        .thenReturn(ImmutableMap.of(SIBLING_URN, siblingEntityResponse));

    testQuery = new TestQuery("__firstSynchronized");
    // first synced from sibling (1) should surface before main entity (2)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN, ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("2")))));

    testQuery = new TestQuery("__lastSynchronized");
    // last synched from main entity (10) should surface instead of sibling (2)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("22")))));

    testQuery = new TestQuery("__lastObserved");
    // last synched from main entity (10) should surface instead of sibling (2)
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("22")))));
  }

  @SneakyThrows
  @Test
  public void testSystemAspectQueriesWithSiblings() {}

  @SneakyThrows
  @Test
  public void testCustomProperties() {
    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(DATASET_URN);
    final DatasetKey datasetKey = new DatasetKey();
    datasetKey.setPlatform(DATASET_URN.getPlatformEntity());
    datasetKey.setName(DATASET_URN.getDatasetNameEntity());

    final DatasetProperties datasetProperties =
        new DatasetProperties().setDescription("test description");
    final DataMap dataMap = new DataMap();
    dataMap.put("prop1", "value1");
    dataMap.put("owner_group", "['group1', 'group2']");
    datasetProperties.setCustomProperties(new StringMap(dataMap));

    final EnvelopedAspect propertiesAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect(datasetProperties.data()))
            .setSystemMetadata(new SystemMetadata().setRunId("ingestion-2").setLastObserved(10))
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(10));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(Constants.DATASET_PROPERTIES_ASPECT_NAME, propertiesAspect)));

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                any()))
        .thenReturn(ImmutableMap.of(DATASET_URN, entityResponse));

    TestQuery testQuery = new TestQuery("datasetProperties.customProperties");
    ValidationResult validationResult =
        _queryEngine.validateQuery(testQuery, List.of(DATASET_URN.getEntityType()));

    assertTrue(validationResult.isValid());

    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of("prop1", "owner_group")))));
  }

  @SneakyThrows
  @Test
  public void testStructuredPropertiesEvaluator() {

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                Mockito.eq(
                    com.google.common.collect.ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of());

    final EntityResponse entityResponse = new EntityResponse();

    final StructuredProperties structuredProperties =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(
                    ImmutableList.of(
                        new StructuredPropertyValueAssignment()
                            .setPropertyUrn(
                                Urn.createFromString("urn:li:structuredProperty:string_test"))
                            .setValues(
                                new PrimitivePropertyValueArray(
                                    ImmutableList.of(PrimitivePropertyValue.create("TEST 1")))),
                        new StructuredPropertyValueAssignment()
                            .setPropertyUrn(
                                Urn.createFromString("urn:li:structuredProperty:string_test"))
                            .setValues(
                                new PrimitivePropertyValueArray(
                                    ImmutableList.of(PrimitivePropertyValue.create("TEST 2")))),
                        new StructuredPropertyValueAssignment()
                            .setPropertyUrn(
                                Urn.createFromString("urn:li:structuredProperty:double_test_1"))
                            .setValues(
                                new PrimitivePropertyValueArray(
                                    ImmutableList.of(PrimitivePropertyValue.create(1.22)))),
                        new StructuredPropertyValueAssignment()
                            .setPropertyUrn(
                                Urn.createFromString("urn:li:structuredProperty:double_test_2"))
                            .setValues(
                                new PrimitivePropertyValueArray(
                                    ImmutableList.of(
                                        PrimitivePropertyValue.create((double) 1)))))));

    final EnvelopedAspect structuredPropertiesAspect =
        new EnvelopedAspect()
            .setName(STRUCTURED_PROPERTIES_ASPECT_NAME)
            .setValue(new Aspect(structuredProperties.data()))
            .setSystemMetadata(new SystemMetadata())
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(10));

    entityResponse.setAspects(
        new EnvelopedAspectMap(
            ImmutableMap.of(STRUCTURED_PROPERTIES_ASPECT_NAME, structuredPropertiesAspect)));

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                Mockito.eq(
                    com.google.common.collect.ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, entityResponse));

    // String property query
    TestQuery testQuery =
        new TestQuery("structuredProperties.urn:li:structuredProperty:string_test");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of("TEST 1", "TEST 2")))));

    // Double property query
    testQuery = new TestQuery("structuredProperties.urn:li:structuredProperty:double_test_1");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("1.22")))));

    testQuery = new TestQuery("structuredProperties.urn:li:structuredProperty:double_test_2");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("1.0")))));

    // No match
    testQuery = new TestQuery("structuredProperties.urn:li:structuredProperty:non_existant");
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery, TestQueryResponse.empty())));
  }

  @SneakyThrows
  @Test
  public void testMapArrayField() {

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                Mockito.eq(com.google.common.collect.ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of());

    final EntityResponse entityResponse = new EntityResponse();

    final UrnArrayMap ownershipTypes = new UrnArrayMap();
    final UrnArray dataOwnerUsers = new UrnArray();
    dataOwnerUsers.add(UrnUtils.getUrn(TEST_USER_1));
    final UrnArray dataStewardUsers = new UrnArray();
    dataStewardUsers.add(UrnUtils.getUrn(TEST_USER_2));
    dataStewardUsers.add(UrnUtils.getUrn(TEST_USER_3));
    final UrnArray businessOwnerUsers = new UrnArray();
    businessOwnerUsers.add(UrnUtils.getUrn(TEST_USER_1));
    businessOwnerUsers.add(UrnUtils.getUrn(TEST_USER_3));
    ownershipTypes.put(TEST_OWNERSHIP_TYPE_1, dataOwnerUsers);
    ownershipTypes.put(TEST_OWNERSHIP_TYPE_2, dataStewardUsers);
    ownershipTypes.put(TEST_OWNERSHIP_TYPE_3, businessOwnerUsers);

    final Ownership ownership = new Ownership().setOwnerTypes(ownershipTypes);

    final EnvelopedAspect ownershipAspect =
        new EnvelopedAspect()
            .setName(OWNERSHIP_ASPECT_NAME)
            .setValue(new Aspect(ownership.data()))
            .setSystemMetadata(new SystemMetadata())
            .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(10));

    entityResponse
        .setAspects(new EnvelopedAspectMap(ImmutableMap.of(OWNERSHIP_ASPECT_NAME, ownershipAspect)))
        .setUrn(DATASET_URN);

    Mockito.when(
            entityService.getEntitiesV2(
                any(OperationContext.class),
                eq(Constants.DATASET_ENTITY_NAME),
                eq(ImmutableSet.of(DATASET_URN)),
                Mockito.eq(com.google.common.collect.ImmutableSet.of(OWNERSHIP_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, entityResponse));

    // String property query
    TestQuery testQuery = new TestQuery("ownership.ownerTypes." + TEST_OWNERSHIP_TYPE_1);
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of(TEST_USER_1)))));

    // Double property query
    testQuery = new TestQuery("ownership.ownerTypes." + TEST_OWNERSHIP_TYPE_2);
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of(TEST_USER_2, TEST_USER_3)))));

    testQuery = new TestQuery("ownership.ownerTypes." + TEST_OWNERSHIP_TYPE_3);
    assertEquals(
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(
            DATASET_URN,
            ImmutableMap.of(
                testQuery, new TestQueryResponse(ImmutableList.of(TEST_USER_1, TEST_USER_3)))));

    // No match
    testQuery = new TestQuery("ownership.ownerTypes.urn:li:ownershipType:nonExistant");
    Map<Urn, Map<TestQuery, TestQueryResponse>> responseMap =
        _queryEngine.batchEvaluateQueries(
            opContext, ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery));
    assertEquals(
        responseMap,
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery, TestQueryResponse.empty())));
  }

  private EntityResponse emptyEntityResponse() {
    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(DATASET_URN)
        .setAspects(new EnvelopedAspectMap());
  }

  private EntityResponse createPropertiesWithDescription() {
    DatasetProperties datasetProperties =
        new DatasetProperties().setDescription("test description");
    return packageProperties(datasetProperties);
  }

  private EntityResponse createPropertiesWithoutDescription() {
    DatasetProperties datasetProperties = new DatasetProperties();
    return packageProperties(datasetProperties);
  }

  private EntityResponse packageProperties(DatasetProperties datasetProperties) {
    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setType(AspectType.VERSIONED)
            .setValue(new Aspect(datasetProperties.data()));
    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(DATASET_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(Constants.DATASET_PROPERTIES_ASPECT_NAME, envelopedAspect)));
  }

  private EntityResponse createGlossaryTerms(List<GlossaryTermUrn> glossaryTerms) {
    GlossaryTerms glossaryTermsAspect =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    glossaryTerms.stream()
                        .map(term -> new GlossaryTermAssociation().setUrn(term))
                        .collect(Collectors.toList())));
    return packageGlossaryTerms(glossaryTermsAspect);
  }

  private EntityResponse packageGlossaryTerms(GlossaryTerms glossaryTerms) {
    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(Constants.GLOSSARY_TERMS_ASPECT_NAME)
            .setType(AspectType.VERSIONED)
            .setValue(new Aspect(glossaryTerms.data()));
    return new EntityResponse()
        .setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(DATASET_URN)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(Constants.GLOSSARY_TERMS_ASPECT_NAME, envelopedAspect)));
  }

  private EntityResponse createGlossaryTermInfo(Urn term, GlossaryNodeUrn parentNode) {
    GlossaryTermInfo glossaryTermInfo =
        new GlossaryTermInfo().setDefinition("test definition").setParentNode(parentNode);
    return packageGlossaryTermInfo(term, glossaryTermInfo);
  }

  private EntityResponse createGlossaryTermInfo(Urn term) {
    GlossaryTermInfo glossaryTermInfo = new GlossaryTermInfo().setDefinition("test definition");
    return packageGlossaryTermInfo(term, glossaryTermInfo);
  }

  private EntityResponse packageGlossaryTermInfo(Urn term, GlossaryTermInfo glossaryTermInfo) {
    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
            .setType(AspectType.VERSIONED)
            .setValue(new Aspect(glossaryTermInfo.data()));
    return new EntityResponse()
        .setEntityName(Constants.GLOSSARY_TERM_ENTITY_NAME)
        .setUrn(term)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, envelopedAspect)));
  }
}
