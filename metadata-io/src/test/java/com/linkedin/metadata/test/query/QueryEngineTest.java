package com.linkedin.metadata.test.query;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.SystemMetadata;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.mockito.Mockito;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.any;
import static org.testng.Assert.*;


public class QueryEngineTest {
  final EntityService _entityService = Mockito.mock(EntityService.class);
  final QueryVersionedAspectEvaluator _queryVersionedAspectEvaluator =
      new QueryVersionedAspectEvaluator(SnapshotEntityRegistry.getInstance(), _entityService);
  final EntityUrnTypeEvaluator _urnTypeEvaluator = new EntityUrnTypeEvaluator();
  final SystemAspectEvaluator _systemAspectEvaluator = new SystemAspectEvaluator(_entityService);
  final QueryEngine _queryEngine = new QueryEngine(ImmutableList.of(_urnTypeEvaluator, _queryVersionedAspectEvaluator,
      _systemAspectEvaluator));

  static final DatasetUrn DATASET_URN = new DatasetUrn(new DataPlatformUrn("bigquery"), "test_dataset", FabricType.DEV);
  static final GlossaryTermUrn GLOSSARY_TERM_WITH_PARENT = new GlossaryTermUrn("term_with_parent");
  static final GlossaryTermUrn GLOSSARY_TERM_WITHOUT_PARENT = new GlossaryTermUrn("term_without_parent");
  static final GlossaryNodeUrn PARENT_NODE = new GlossaryNodeUrn("parent_node");

  @BeforeTest
  public void reset() {
    Mockito.reset(_entityService);
  }

  @SneakyThrows
  @Test
  public void testEngine() {
    TestQuery testQuery = new TestQuery("datasetProperties.description");
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, emptyEntityResponse()));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createPropertiesWithDescription()));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("test description")))));
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.DATASET_PROPERTIES_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createPropertiesWithoutDescription()));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());

    testQuery = new TestQuery("glossaryTerms.terms.urn.glossaryTermInfo.parentNode");
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, emptyEntityResponse()));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createGlossaryTerms(Collections.emptyList())));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());

    Mockito.when(_entityService.getEntitiesV2(eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        eq(ImmutableSet.of(GLOSSARY_TERM_WITH_PARENT)), eq(ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(GLOSSARY_TERM_WITH_PARENT, createGlossaryTermInfo(GLOSSARY_TERM_WITH_PARENT, PARENT_NODE)));
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        eq(ImmutableSet.of(GLOSSARY_TERM_WITHOUT_PARENT)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(GLOSSARY_TERM_WITHOUT_PARENT, createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)));
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.GLOSSARY_TERM_ENTITY_NAME),
        eq(ImmutableSet.of(GLOSSARY_TERM_WITH_PARENT, GLOSSARY_TERM_WITHOUT_PARENT)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(GLOSSARY_TERM_WITH_PARENT, createGlossaryTermInfo(GLOSSARY_TERM_WITH_PARENT, PARENT_NODE),
                GLOSSARY_TERM_WITHOUT_PARENT, createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)));
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT))));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of(PARENT_NODE.toString())))));
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN, createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITHOUT_PARENT))));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN,
            createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT, GLOSSARY_TERM_WITHOUT_PARENT))));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of(PARENT_NODE.toString())))));
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN,
            createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT, GLOSSARY_TERM_WITH_PARENT))));
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery,
            new TestQueryResponse(ImmutableList.of(PARENT_NODE.toString(), PARENT_NODE.toString())))));

    // Test URN + Entity Type queries

    // Case 1: Base Entity URN
    testQuery = new TestQuery("urn");
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery,
            new TestQueryResponse(ImmutableList.of(DATASET_URN.toString())))));
    // Case 2: Base Entity Type
    testQuery = new TestQuery("entityType");
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery,
            new TestQueryResponse(ImmutableList.of(DATASET_URN.getEntityType())))));

    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
        eq(ImmutableSet.of(Constants.GLOSSARY_TERMS_ASPECT_NAME))))
        .thenReturn(ImmutableMap.of(DATASET_URN,
            createGlossaryTerms(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT))));

    // Case 3: Nested Entity URN
    testQuery = new TestQuery("glossaryTerms.terms.urn.urn");
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery,
            new TestQueryResponse(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT.toString())))));
    // Case 4: Nested Entity Type
    testQuery = new TestQuery("glossaryTerms.terms.urn.entityType");
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN, ImmutableMap.of(testQuery,
            new TestQueryResponse(ImmutableList.of(GLOSSARY_TERM_WITH_PARENT.getEntityType())))));
  }

  @SneakyThrows
  @Test
  public void testSystemAspectQueries() {
    final EntityRegistry entityRegistry =
        new ConfigEntityRegistry(Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));

    Mockito.when(_entityService.getEntityRegistry()).thenReturn(entityRegistry);

    TestQuery testQuery = new TestQuery("__firstSynchronized");
    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
            any())).thenReturn(ImmutableMap.of());
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        Collections.emptyMap());

    final EntityResponse entityResponse = new EntityResponse();
    final DatasetKey datasetKey = new DatasetKey().setName(DATASET_URN.toString());
    final DatasetProperties datasetProperties = new DatasetProperties()
        .setDescription("test description");

    final EnvelopedAspect keyAspect = new EnvelopedAspect()
        .setName(Constants.DATASET_KEY_ASPECT_NAME)
        .setValue(new Aspect(datasetKey.data()))
        .setSystemMetadata(new SystemMetadata()
            .setRunId("ingestion-1")
            .setLastObserved(0))
        .setCreated(new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(0));

    final EnvelopedAspect propertiesAspect = new EnvelopedAspect()
        .setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
        .setValue(new Aspect(datasetProperties.data()))
        .setSystemMetadata(new SystemMetadata()
            .setRunId("ingestion-2")
            .setLastObserved(10))
        .setCreated(new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(10));

    final EnvelopedAspect glossaryTermInfoAspect = createGlossaryTermInfo(GLOSSARY_TERM_WITHOUT_PARENT)
        .getAspects()
        .get(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
        .setSystemMetadata(new SystemMetadata()
            .setRunId(Constants.DEFAULT_RUN_ID)
            .setLastObserved(11))
        .setCreated(new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(11));

    entityResponse.setAspects(new EnvelopedAspectMap(ImmutableMap.of(
        Constants.DATASET_PROPERTIES_ASPECT_NAME, propertiesAspect,
        Constants.DATASET_KEY_ASPECT_NAME, keyAspect,
        Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, glossaryTermInfoAspect
    )));

    Mockito.when(_entityService.getEntitiesV2(eq(Constants.DATASET_ENTITY_NAME), eq(ImmutableSet.of(DATASET_URN)),
            any())).thenReturn(ImmutableMap.of(DATASET_URN, entityResponse));

    // Validate firstSynchronized query (createdOn time for key aspect)
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("0")))));

    testQuery = new TestQuery("__lastSynchronized");
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("10")))));

    testQuery = new TestQuery("__created");
    assertEquals(_queryEngine.batchEvaluateQueries(ImmutableSet.of(DATASET_URN), ImmutableSet.of(testQuery)),
        ImmutableMap.of(DATASET_URN,
            ImmutableMap.of(testQuery, new TestQueryResponse(ImmutableList.of("11")))));


  }

  private EntityResponse emptyEntityResponse() {
    return new EntityResponse().setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(DATASET_URN)
        .setAspects(new EnvelopedAspectMap());
  }

  private EntityResponse createPropertiesWithDescription() {
    DatasetProperties datasetProperties = new DatasetProperties().setDescription("test description");
    return packageProperties(datasetProperties);
  }

  private EntityResponse createPropertiesWithoutDescription() {
    DatasetProperties datasetProperties = new DatasetProperties();
    return packageProperties(datasetProperties);
  }

  private EntityResponse packageProperties(DatasetProperties datasetProperties) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect().setName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
        .setType(AspectType.VERSIONED)
        .setValue(new Aspect(datasetProperties.data()));
    return new EntityResponse().setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(DATASET_URN)
        .setAspects(new EnvelopedAspectMap(ImmutableMap.of(Constants.DATASET_PROPERTIES_ASPECT_NAME, envelopedAspect)));
  }

  private EntityResponse createGlossaryTerms(List<GlossaryTermUrn> glossaryTerms) {
    GlossaryTerms glossaryTermsAspect = new GlossaryTerms().setTerms(new GlossaryTermAssociationArray(
        glossaryTerms.stream().map(term -> new GlossaryTermAssociation().setUrn(term)).collect(Collectors.toList())));
    return packageGlossaryTerms(glossaryTermsAspect);
  }

  private EntityResponse packageGlossaryTerms(GlossaryTerms glossaryTerms) {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect().setName(Constants.GLOSSARY_TERMS_ASPECT_NAME)
        .setType(AspectType.VERSIONED)
        .setValue(new Aspect(glossaryTerms.data()));
    return new EntityResponse().setEntityName(Constants.DATASET_ENTITY_NAME)
        .setUrn(DATASET_URN)
        .setAspects(new EnvelopedAspectMap(ImmutableMap.of(Constants.GLOSSARY_TERMS_ASPECT_NAME, envelopedAspect)));
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
    EnvelopedAspect envelopedAspect = new EnvelopedAspect().setName(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME)
        .setType(AspectType.VERSIONED)
        .setValue(new Aspect(glossaryTermInfo.data()));
    return new EntityResponse().setEntityName(Constants.GLOSSARY_TERM_ENTITY_NAME)
        .setUrn(term)
        .setAspects(new EnvelopedAspectMap(ImmutableMap.of(Constants.GLOSSARY_TERM_INFO_ASPECT_NAME, envelopedAspect)));
  }
}
