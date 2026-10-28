package com.linkedin.metadata.aspect.consistency.fix;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.StructuredPropertyIndexMismatchCheck;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SyncStructuredPropertyIndexFieldsFixTest {

  private static final String FIELD = "structuredProperties.retentionPolicy";
  private static final String KEPT = "structuredProperties.kept";
  private static final String EXTRA = "structuredProperties.removed";
  private static final String URN_NAME = "structuredProperties.owner__urn_name";
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sample,PROD)");

  private final ObjectMapper objectMapper = new ObjectMapper();
  private StructuredPropertyMappingCache cache;
  private StructuredPropertyIndexFields indexFields;
  private SearchDocumentTransformer transformer;
  private ElasticSearchService elasticSearchService;
  private EntityService<?> entityService;
  private OperationContext opContext;
  private String v2Index;
  private String v3Index;
  private final AtomicReference<Map<String, JsonNode>> indexed = new AtomicReference<>();

  @BeforeMethod
  public void setup() throws Exception {
    cache = mock(StructuredPropertyMappingCache.class);
    indexFields = mock(StructuredPropertyIndexFields.class);
    transformer = mock(SearchDocumentTransformer.class);
    elasticSearchService = mock(ElasticSearchService.class);
    entityService = mock(EntityService.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    v2Index = convention.getEntityIndexName(opContext, "dataset");
    EntitySpec spec = opContext.getEntityRegistry().getEntitySpec("dataset");
    v3Index = convention.getEntityIndexNameV3(opContext, V3IndexKeys.resolve(spec));
    when(cache.isEnabled()).thenReturn(true);
    when(cache.fields(any(), any())).thenReturn(Optional.of(Set.of(FIELD, KEPT, EXTRA)));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response()));
    project(Map.of(FIELD, "yes", KEPT, "same"));
    indexed.set(
        Map.of(
            FIELD,
            TextNode.valueOf("old"),
            KEPT,
            TextNode.valueOf("same"),
            EXTRA,
            TextNode.valueOf("stale")));
    when(indexFields.byDocumentId(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Collection<String> ids = invocation.getArgument(2);
              String id = ids.iterator().next();
              return Map.of(id, new DocumentValues(id, URN.toString(), indexed.get()));
            });
  }

  @Test
  public void testDryRunReportsTheDiffAndWritesNothing() {
    ConsistencyFixDetail result = fix(v2Only()).apply(opContext, issue(), true);

    assertTrue(result.isSuccess());
    assertTrue(result.getDetails().contains("set 1"));
    assertTrue(result.getDetails().contains("null 1"));
    assertTrue(result.getDetails().contains(v2Index));
    verify(elasticSearchService, never()).upsertDocumentByIndexName(any(), any(), any(), any());
  }

  @Test
  public void testActiveFixWritesOnlyDifferingMappedFields() throws Exception {
    ConsistencyFixDetail result = fix(v2Only()).apply(opContext, issue(), false);

    assertTrue(result.isSuccess());
    ArgumentCaptor<String> document = ArgumentCaptor.forClass(String.class);
    verify(elasticSearchService)
        .upsertDocumentByIndexName(any(), eq(v2Index), document.capture(), any());
    JsonNode written = objectMapper.readTree(document.getValue());
    assertEquals(written.get(FIELD), TextNode.valueOf("yes"));
    assertTrue(written.get(EXTRA).isNull());
    assertFalse(written.has(KEPT));
    assertFalse(written.has("structuredProperties.decoy"));
    assertFalse(written.has("name"));
  }

  @Test
  public void testFixRechecksSqlAndTheIndexBeforeWriting() throws Exception {
    indexed.set(Map.of(FIELD, TextNode.valueOf("yes"), KEPT, TextNode.valueOf("same")));

    ConsistencyFixDetail result = fix(v2Only()).apply(opContext, issue(), false);

    assertTrue(result.isSuccess());
    assertEquals(result.getDetails(), "no differing mapped fields");
    verify(elasticSearchService, never()).upsertDocumentByIndexName(any(), any(), any(), any());
    verify(entityService).getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean());
    verify(indexFields).byDocumentId(any(), eq(v2Index), any());
  }

  @Test
  public void testAbsentDocumentIsNotUpserted() throws Exception {
    reset(indexFields);
    doReturn(Map.of()).when(indexFields).byDocumentId(any(), any(), any());

    ConsistencyFixDetail result = fix(v2Only()).apply(opContext, issue(), false);

    assertFalse(result.isSuccess());
    assertTrue(result.getErrorMessage().contains(v2Index));
    assertTrue(result.getErrorMessage().contains("full index write"));
    verify(elasticSearchService, never()).upsertDocumentByIndexName(any(), any(), any(), any());
  }

  @Test
  public void testFailedV3IdentityProjectionIsNotWritten() throws Exception {
    when(cache.fields(any(), any())).thenReturn(Optional.of(Set.of(FIELD, URN_NAME)));
    project(Map.of(FIELD, "yes"));
    indexed.set(Map.of(FIELD, TextNode.valueOf("yes"), URN_NAME, TextNode.valueOf("Owner")));

    try (MockedStatic<StructuredPropertyUrnIdentity> identity =
        mockStatic(StructuredPropertyUrnIdentity.class)) {
      identity
          .when(() -> StructuredPropertyUrnIdentity.project(any(), any(), any()))
          .thenThrow(new IllegalStateException("definitions unavailable"));

      ConsistencyFixDetail result = fix(v3Only()).apply(opContext, issue(), false);

      assertFalse(result.isSuccess());
      assertTrue(result.getErrorMessage().contains(v3Index));
      assertTrue(result.getErrorMessage().contains("could not be projected"));
      verify(elasticSearchService, never()).upsertDocumentByIndexName(any(), any(), any(), any());
    }
  }

  private SyncStructuredPropertyIndexFieldsFix fix(ElasticSearchConfiguration configuration) {
    StructuredPropertyIndexMismatchCheck check =
        new StructuredPropertyIndexMismatchCheck(
            cache,
            indexFields,
            transformer,
            configuration,
            entityService,
            new Sha256UrnEntityDocumentIdHasher(),
            objectMapper);
    return new SyncStructuredPropertyIndexFieldsFix(check, elasticSearchService);
  }

  private static ConsistencyIssue issue() {
    return ConsistencyIssue.builder()
        .entityUrn(URN)
        .entityType("dataset")
        .checkId(StructuredPropertyIndexMismatchCheck.CHECK_ID)
        .fixType(ConsistencyFixType.SYNC_STRUCTURED_PROPERTY_INDEX_FIELDS)
        .description("stale")
        .metadata(
            Map.of(
                "targets",
                "{\"decoy\":{\"set\":{\"structuredProperties.decoy\":\"x\"},\"null\":[]}}"))
        .build();
  }

  private void project(Map<String, String> values) throws Exception {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    values.forEach(document::put);
    when(transformer.transformAspect(any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(document));
  }

  private static EntityResponse response() {
    StructuredProperties properties = new StructuredProperties();
    properties.setProperties(new StructuredPropertyValueAssignmentArray());
    EnvelopedAspect enveloped = new EnvelopedAspect();
    enveloped.setName(STRUCTURED_PROPERTIES_ASPECT_NAME);
    enveloped.setValue(new Aspect(properties.data()));
    EntityResponse response = new EntityResponse();
    response.setUrn(URN);
    response.setEntityName("dataset");
    response.setAspects(
        new EnvelopedAspectMap(Map.of(STRUCTURED_PROPERTIES_ASPECT_NAME, enveloped)));
    return response;
  }

  private static ElasticSearchConfiguration v2Only() {
    return configuration(true, false);
  }

  private static ElasticSearchConfiguration v3Only() {
    return configuration(false, true);
  }

  private static ElasticSearchConfiguration configuration(boolean v2, boolean v3) {
    return ElasticSearchConfiguration.builder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(v2).build())
                .v3(EntityIndexVersionConfiguration.builder().enabled(v3).build())
                .build())
        .build();
  }
}
