package com.linkedin.metadata.test.query;

import com.linkedin.common.FabricType;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
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

import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;


public class QueryEngineTest {
  EntityService _entityService = Mockito.mock(EntityService.class);
  QueryVersionedAspectEvaluator _queryVersionedAspectEvaluator =
      new QueryVersionedAspectEvaluator(SnapshotEntityRegistry.getInstance(), _entityService);
  QueryEngine _queryEngine = new QueryEngine(ImmutableList.of(_queryVersionedAspectEvaluator));

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
