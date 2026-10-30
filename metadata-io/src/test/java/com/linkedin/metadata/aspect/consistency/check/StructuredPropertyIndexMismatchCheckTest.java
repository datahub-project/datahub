package com.linkedin.metadata.aspect.consistency.check;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyFixRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.DocumentValues;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyIndexFields.Page;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredPropertyIndexMismatchCheckTest {

  private static final String FIELD = "structuredProperties.retentionPolicy";
  private static final String URN_NAME = "structuredProperties.owner__urn_name";
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sample,PROD)");
  private static final Urn OTHER =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,other,PROD)");

  private StructuredPropertyMappingCache cache;
  private StructuredPropertyIndexFields indexFields;
  private SearchDocumentTransformer transformer;
  private EntityService<?> entityService;
  private OperationContext opContext;
  private CheckContext checkContext;
  private String v2Index;
  private String v3Index;

  @BeforeMethod
  public void setup() {
    cache = mock(StructuredPropertyMappingCache.class);
    indexFields = mock(StructuredPropertyIndexFields.class);
    transformer = mock(SearchDocumentTransformer.class);
    entityService = mock(EntityService.class);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    checkContext =
        CheckContext.builder().operationContext(opContext).entityService(entityService).build();
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    v2Index = convention.getEntityIndexName(opContext, "dataset");
    EntitySpec spec = opContext.getEntityRegistry().getEntitySpec("dataset");
    v3Index = convention.getEntityIndexNameV3(opContext, V3IndexKeys.resolve(spec));
    when(cache.isEnabled()).thenReturn(true);
    when(cache.fields(any(), any())).thenReturn(Optional.of(Set.of(FIELD)));
  }

  @Test
  public void testOnDemandCheckIsAbsentFromADefaultRunAndRunsWhenRequested() {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ConsistencyCheckRegistry registry = new ConsistencyCheckRegistry(List.of(check));

    assertTrue(
        registry.getDefaultByEntityTypeAndIds("dataset", null).stream()
            .noneMatch(candidate -> check.getId().equals(candidate.getId())));
    assertEquals(
        registry.getDefaultByEntityTypeAndIds("dataset", List.of(check.getId())).get(0).getId(),
        StructuredPropertyIndexMismatchCheck.CHECK_ID);
    assertTrue(check.isOnDemandOnly());
  }

  @Test
  public void testDefaultServiceRunDoesNotReadTheIndex() {
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    ConsistencyService service =
        new ConsistencyService(
            entityService,
            systemMetadata,
            null,
            new ConsistencyCheckRegistry(List.of(check(v2Only()))),
            new ConsistencyFixRegistry(List.of()));

    CheckResult result =
        service.checkBatch(opContext, CheckBatchRequest.builder().entityType("dataset").build());

    assertEquals(result.getEntitiesScanned(), 0);
    verifyNoInteractions(systemMetadata);
    verifyNoInteractions(indexFields);
  }

  @Test
  public void testMissingDifferentAndExtraFieldsAndAMatch() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    project(FIELD, "expected");
    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenReturn(documents(URN, Map.of(FIELD, TextNode.valueOf("expected"))));

    assertTrue(check.check(checkContext, Map.of(URN, response(URN, true))).isEmpty());

    when(indexFields.byDocumentId(any(), eq(v2Index), any())).thenReturn(Map.of());
    ConsistencyIssue missing = check.check(checkContext, Map.of(URN, response(URN, true))).get(0);
    assertTrue(missing.getDescription().contains("1 missing"));
    assertEquals(missing.getFixType(), ConsistencyFixType.SYNC_STRUCTURED_PROPERTY_INDEX_FIELDS);

    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenReturn(documents(URN, Map.of(FIELD, TextNode.valueOf("other"))));
    assertTrue(
        check
            .check(checkContext, Map.of(URN, response(URN, true)))
            .get(0)
            .getDescription()
            .contains("1 different"));

    when(cache.fields(any(), any())).thenReturn(Optional.of(Set.of(FIELD, URN_NAME)));
    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenReturn(
            documents(
                URN,
                Map.of(FIELD, TextNode.valueOf("expected"), URN_NAME, TextNode.valueOf("stale"))));
    ConsistencyIssue extra = check.check(checkContext, Map.of(URN, response(URN, true))).get(0);
    assertTrue(extra.getDescription().contains("1 extra"));
    assertTrue(extra.getMetadata().get("targets").contains(URN_NAME));
  }

  @Test
  public void testV3StaleUrnNameSibling() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v3Only());
    project(FIELD, "expected");
    when(cache.fields(any(), eq(v3Index))).thenReturn(Optional.of(Set.of(FIELD, URN_NAME)));
    when(indexFields.byDocumentId(any(), eq(v3Index), any()))
        .thenReturn(
            documents(
                URN,
                Map.of(FIELD, TextNode.valueOf("expected"), URN_NAME, TextNode.valueOf("old")),
                true));

    ConsistencyIssue issue = check.check(checkContext, Map.of(URN, response(URN, true))).get(0);

    assertTrue(issue.getDescription().contains("1 extra"));
    assertTrue(issue.getMetadata().get("targets").contains(v3Index));
    assertTrue(issue.getMetadata().get("targets").contains(URN_NAME));
    verify(indexFields, never()).byDocumentId(any(), eq(v2Index), any());
  }

  @Test
  public void testAspectWithoutADocumentAndDocumentWithoutAnAspect() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    project(FIELD, "expected");
    when(indexFields.byDocumentId(any(), eq(v2Index), any())).thenReturn(Map.of());

    ConsistencyIssue missingDocument =
        check.check(checkContext, Map.of(URN, response(URN, true))).get(0);
    assertTrue(missingDocument.getDescription().contains("1 missing"));

    project(FIELD, "expected");
    when(transformer.transformAspect(any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(JsonNodeFactory.instance.objectNode()));
    assertTrue(check.check(checkContext, Map.of(URN, response(URN, true))).isEmpty());

    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("left-behind")))),
                null));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of());

    StructuredPropertyIndexMismatchCheck.IndexScan removed =
        check.scanIndexes(opContext, "dataset", 10, null);
    assertEquals(removed.issues().size(), 1);
    assertTrue(removed.issues().get(0).getDescription().contains("1 extra"));
    assertTrue(removed.issues().get(0).getMetadata().get("targets").contains("\"null\""));
  }

  @Test
  public void testFailedSqlReadDoesNotAdvanceTheIndexCursor() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("left-behind")))),
                "cursor-2"));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenThrow(new RuntimeException("sql down"));

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class, () -> check.scanIndexes(opContext, "dataset", 10, null));

    assertTrue(failure.getMessage().contains(v2Index));
    assertEquals(failure.getCause().getMessage(), "sql down");

    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of());
    StructuredPropertyIndexMismatchCheck.IndexScan retried =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertEquals(retried.issues().size(), 1);
    assertTrue(retried.issues().get(0).getDescription().contains("1 extra"));
  }

  @Test
  public void testFailedV3IdentityProjectionDoesNotClearUrnNames() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v3Only());
    project(FIELD, "expected");
    when(cache.fields(any(), eq(v3Index))).thenReturn(Optional.of(Set.of(FIELD, URN_NAME)));
    when(indexFields.byDocumentId(any(), eq(v3Index), any()))
        .thenReturn(
            documents(
                URN,
                Map.of(FIELD, TextNode.valueOf("expected"), URN_NAME, TextNode.valueOf("Owner")),
                true));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response(URN, true)));

    try (MockedStatic<StructuredPropertyUrnIdentity> identity =
        mockStatic(StructuredPropertyUrnIdentity.class)) {
      identity
          .when(() -> StructuredPropertyUrnIdentity.project(any(), any(), any()))
          .thenThrow(new IllegalStateException("definitions unavailable"));

      IllegalStateException failure =
          expectThrows(
              IllegalStateException.class,
              () -> check.check(checkContext, Map.of(URN, response(URN, true))));
      assertTrue(failure.getMessage().contains(URN.toString()));

      List<StructuredPropertyIndexMismatchCheck.TargetDiff> diffs = check.recompute(opContext, URN);
      assertEquals(diffs.size(), 1);
      assertTrue(diffs.get(0).error().contains("could not be projected"));
      assertNull(diffs.get(0).change());
    }
  }

  @Test
  public void testIndexPageSkipsAnAspectThatStillExistsAndUnmappedFields() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues("doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("x"))),
                    new DocumentValues(
                        "other",
                        OTHER.toString(),
                        Map.of("structuredProperties.unmapped", TextNode.valueOf("x")))),
                null));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(2);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              if (urns.contains(URN)) {
                responses.put(URN, response(URN, true));
              }
              return responses;
            });

    StructuredPropertyIndexMismatchCheck.IndexScan scan =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertTrue(scan.issues().isEmpty());
    verify(indexFields, never()).byDocumentId(any(), any(), any());
  }

  @Test
  public void testEmptyMappingIsNotScrolled() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    when(cache.fields(any(), any())).thenReturn(Optional.of(Set.of()));

    StructuredPropertyIndexMismatchCheck.IndexScan scan =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertEquals(scan.documents(), 0);
    assertTrue(scan.scrollId() == null);
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testOneReadServesABatchAndTheMappingCacheIsReused() throws Exception {
    AtomicInteger reads = new AtomicInteger();
    StructuredPropertyMappingCache realCache =
        new StructuredPropertyMappingCache(
            true,
            300,
            (context, index) -> {
              reads.incrementAndGet();
              Map<String, Object> retention = new HashMap<>();
              retention.put("type", "keyword");
              Map<String, Object> children = new HashMap<>();
              children.put("retentionPolicy", retention);
              Map<String, Object> structured = new HashMap<>();
              structured.put("properties", children);
              Map<String, Object> properties = new HashMap<>();
              properties.put("structuredProperties", structured);
              Map<String, Object> mapping = new HashMap<>();
              mapping.put("properties", properties);
              return mapping;
            });
    StructuredPropertyIndexMismatchCheck check =
        new StructuredPropertyIndexMismatchCheck(
            realCache,
            indexFields,
            transformer,
            v2Only(),
            entityService,
            new Sha256UrnEntityDocumentIdHasher(),
            new com.fasterxml.jackson.databind.ObjectMapper());
    project(FIELD, "expected");
    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenReturn(documents(URN, Map.of(FIELD, TextNode.valueOf("expected"))));

    check.check(checkContext, Map.of(URN, response(URN, true), OTHER, response(OTHER, true)));
    check.check(checkContext, Map.of(URN, response(URN, true)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<String>> ids = ArgumentCaptor.forClass(Collection.class);
    verify(indexFields, times(2)).byDocumentId(any(), eq(v2Index), ids.capture());
    assertEquals(ids.getAllValues().get(0).size(), 2);
    assertEquals(reads.get(), 1);
  }

  @Test
  public void testDisabledCacheSkipsTheCheck() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    when(cache.isEnabled()).thenReturn(false);

    assertTrue(check.check(checkContext, Map.of(URN, response(URN, true))).isEmpty());
    assertEquals(check.scanIndexes(opContext, "dataset", 10, null).documents(), 0);
    verify(indexFields, never()).byDocumentId(any(), any(), any());
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testTargetsOnlyTheKeywordReadIndex() {
    assertEquals(check(v2AndV3()).targets(opContext, "dataset").size(), 1);
    assertEquals(check(v2AndV3()).targets(opContext, "dataset").get(0).indexName(), v2Index);
    assertFalse(check(v2AndV3()).targets(opContext, "dataset").get(0).v3());

    assertEquals(check(v3KeywordRead()).targets(opContext, "dataset").size(), 1);
    assertEquals(check(v3KeywordRead()).targets(opContext, "dataset").get(0).indexName(), v3Index);
    assertTrue(check(v3KeywordRead()).targets(opContext, "dataset").get(0).v3());
  }

  @Test
  public void testAFailedMappingDoesNotAdvancePastThatIndex() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    project(FIELD, "expected");
    when(cache.fields(any(), eq(v2Index))).thenReturn(Optional.empty());
    when(cache.fields(any(), eq(v3Index))).thenReturn(Optional.of(Set.of(FIELD)));

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class,
            () -> check.check(checkContext, Map.of(URN, response(URN, true))));

    assertTrue(failure.getMessage().contains(v2Index));
    verify(indexFields, never()).byDocumentId(any(), any(), any());
  }

  @Test
  public void testEmptySqlPageContinuesIntoTheIndexAndALaterPageDoesNotRereadSql()
      throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse empty = mock(SearchResponse.class);
    when(empty.getHits())
        .thenReturn(
            new SearchHits(
                new org.opensearch.search.SearchHit[0],
                new org.apache.lucene.search.TotalHits(
                    0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                0f));
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(empty);
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("x")))),
                "cursor-2"));
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), eq("cursor-2"), isNull()))
        .thenReturn(new Page(List.of(), null));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of());
    ConsistencyService service =
        new ConsistencyService(
            entityService,
            systemMetadata,
            null,
            new ConsistencyCheckRegistry(List.of(check)),
            new ConsistencyFixRegistry(List.of()));

    CheckResult first =
        service.checkBatch(
            opContext,
            CheckBatchRequest.builder()
                .entityType("dataset")
                .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                .batchSize(10)
                .build());

    assertEquals(first.getIssues().size(), 1);
    assertTrue(
        first.getScrollId().startsWith(StructuredPropertyIndexMismatchCheck.INDEX_SCROLL_PREFIX));

    CheckResult second =
        service.checkBatch(
            opContext,
            CheckBatchRequest.builder()
                .entityType("dataset")
                .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                .batchSize(10)
                .scrollId(first.getScrollId())
                .build());

    assertTrue(second.getIssues().isEmpty());
    assertTrue(second.getScrollId() == null);
    verify(systemMetadata)
        .scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class));
  }

  @Test
  public void testFailedSystemMetadataScrollDoesNotStartTheIndexPass() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(null);
    ConsistencyService service = service(check, systemMetadata);

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class,
            () ->
                service.checkBatch(
                    opContext,
                    CheckBatchRequest.builder()
                        .entityType("dataset")
                        .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                        .batchSize(10)
                        .scrollId("sql-page")
                        .build()));

    assertTrue(failure.getMessage().contains("dataset"));
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testUnreadableSystemMetadataPageContinuesSqlInsteadOfTheIndex() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(null);
    when(hit.getIndex()).thenReturn("system_metadata");
    when(hit.getId()).thenReturn("1");
    when(hit.getSortValues()).thenReturn(new Object[] {"urn:li:dataset:unreadable", "aspect"});
    SearchResponse page = mock(SearchResponse.class);
    when(page.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {hit},
                new org.apache.lucene.search.TotalHits(
                    1, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                0f));
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(page);
    ConsistencyService service = service(check, systemMetadata);

    CheckResult result =
        service.checkBatch(
            opContext,
            CheckBatchRequest.builder()
                .entityType("dataset")
                .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                .batchSize(10)
                .build());

    assertTrue(result.getScrollId() != null);
    assertFalse(StructuredPropertyIndexMismatchCheck.isIndexContinuation(result.getScrollId()));
    assertEquals(result.getEntitiesScanned(), 1);
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testFailedMappingReadDoesNotFinishTheIndex() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    when(cache.fields(any(), eq(v2Index))).thenReturn(Optional.empty());

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class, () -> check.scanIndexes(opContext, "dataset", 10, null));

    assertTrue(failure.getMessage().contains(v2Index));
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));

    when(cache.fields(any(), eq(v2Index))).thenReturn(Optional.of(Set.of(FIELD)));
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("left-behind")))),
                null));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of());

    StructuredPropertyIndexMismatchCheck.IndexScan retried =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertEquals(retried.issues().size(), 1);
  }

  @Test
  public void testFailedIndexScrollDoesNotFinishTheIndex() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenThrow(new IOException("search down"));

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class, () -> check.scanIndexes(opContext, "dataset", 10, null));

    assertTrue(failure.getMessage().contains(v2Index));
    assertEquals(failure.getCause().getMessage(), "search down");

    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("left-behind")))),
                null));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of());

    StructuredPropertyIndexMismatchCheck.IndexScan retried =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertEquals(retried.issues().size(), 1);
    verify(indexFields, times(2)).scroll(any(), eq(v2Index), anyInt(), isNull(), isNull());
  }

  @Test
  public void testV3IndexScanFiltersByEntityType() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v3Only());
    when(indexFields.scroll(any(), eq(v3Index), anyInt(), isNull(), eq("dataset")))
        .thenReturn(new Page(List.of(), null));

    StructuredPropertyIndexMismatchCheck.IndexScan scan =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertEquals(scan.documents(), 0);
    verify(indexFields).scroll(any(), eq(v3Index), eq(10), isNull(), eq("dataset"));
  }

  @Test
  public void testUnreadableIndexCursorDoesNotFinishTheRun() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    ConsistencyService service = service(check, systemMetadata);

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class,
            () ->
                service.checkBatch(
                    opContext,
                    CheckBatchRequest.builder()
                        .entityType("dataset")
                        .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                        .batchSize(10)
                        .scrollId(StructuredPropertyIndexMismatchCheck.INDEX_SCROLL_PREFIX + "%%%")
                        .build()));

    assertTrue(failure.getMessage().contains("Unreadable"));
    verifyNoInteractions(systemMetadata);
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));

    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("left-behind")))),
                null));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of());

    StructuredPropertyIndexMismatchCheck.IndexScan retried =
        check.scanIndexes(opContext, "dataset", 10, null);

    assertEquals(retried.issues().size(), 1);
  }

  @Test
  public void testUrnFilterDoesNotLookUpUrnsMissingFromSystemMetadata() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    Urn absent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,absent,PROD)");
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse empty = mock(SearchResponse.class);
    when(empty.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[0],
                new org.apache.lucene.search.TotalHits(
                    0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                0f));
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(empty);
    ConsistencyService service = service(check, systemMetadata);
    CheckBatchRequest request =
        CheckBatchRequest.builder()
            .entityType("dataset")
            .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
            .urns(Set.of(URN, OTHER, absent))
            .batchSize(10)
            .build();

    CheckResult result = service.checkBatch(opContext, request);

    assertTrue(result.getIssues().isEmpty());
    assertNull(result.getScrollId());
    assertEquals(result.getEntitiesScanned(), 0);
    verify(indexFields, never()).byDocumentId(any(), any(), any());
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testUrnFilterDoesNotReportASystemMetadataUrnTwice() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    Urn absent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,absent,PROD)");
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse sqlPage = systemMetadataPage(URN, null);
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(sqlPage);
    when(entityService.exists(any(), any(), isNull(), anyBoolean(), anyBoolean()))
        .thenReturn(Set.of(URN));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response(URN, false)));
    Map<String, DocumentValues> found = new HashMap<>();
    found.putAll(documents(URN, Map.of(FIELD, TextNode.valueOf("left-behind"))));
    found.putAll(documents(absent, Map.of(FIELD, TextNode.valueOf("also-left"))));
    when(indexFields.byDocumentId(any(), eq(v2Index), any())).thenReturn(found);
    ConsistencyService service = service(check, systemMetadata);

    CheckResult result =
        service.checkBatch(
            opContext,
            CheckBatchRequest.builder()
                .entityType("dataset")
                .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                .urns(Set.of(URN, absent))
                .batchSize(10)
                .build());

    assertEquals(result.getIssues().size(), 1);
    assertEquals(result.getIssues().get(0).getEntityUrn(), URN);
    assertNull(result.getScrollId());
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testFailedPageLocalLookupDoesNotAdvanceTheSqlCursor() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse sqlPage = systemMetadataPage(URN, new Object[] {"cursor"});
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(sqlPage);
    when(entityService.exists(any(), any(), isNull(), anyBoolean(), anyBoolean()))
        .thenReturn(Set.of(URN));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response(URN, false)));
    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenThrow(new IOException("search down"));
    ConsistencyService service = service(check, systemMetadata);

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class,
            () ->
                service.checkBatch(
                    opContext,
                    CheckBatchRequest.builder()
                        .entityType("dataset")
                        .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                        .urns(Set.of(URN))
                        .batchSize(10)
                        .build()));

    assertTrue(failure.getMessage().contains(v2Index));
    verify(systemMetadata).scroll(any(), any(), anyBoolean(), isNull(), any(), any(), any());
  }

  @Test
  public void testUrnFilterReportsPageLocalLeftoversBeforePublishingTheNextCursor()
      throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    Urn absent = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,absent,PROD)");
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse sqlPage = systemMetadataPage(URN, new Object[] {"cursor"});
    SearchResponse empty = mock(SearchResponse.class);
    when(empty.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[0],
                new org.apache.lucene.search.TotalHits(
                    0, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                0f));
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(sqlPage)
        .thenReturn(empty);
    when(entityService.exists(any(), any(), isNull(), anyBoolean(), anyBoolean()))
        .thenReturn(Set.of(URN));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response(URN, false)));
    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenReturn(documents(URN, Map.of(FIELD, TextNode.valueOf("left-behind"))));
    ConsistencyService service = service(check, systemMetadata);
    CheckBatchRequest firstRequest =
        CheckBatchRequest.builder()
            .entityType("dataset")
            .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
            .urns(Set.of(URN, absent))
            .batchSize(10)
            .build();

    CheckResult first = service.checkBatch(opContext, firstRequest);

    assertEquals(first.getIssues().size(), 1);
    assertEquals(first.getIssues().get(0).getEntityUrn(), URN);
    assertFalse(StructuredPropertyIndexMismatchCheck.isIndexContinuation(first.getScrollId()));

    CheckResult second =
        service.checkBatch(
            opContext,
            CheckBatchRequest.builder()
                .entityType("dataset")
                .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                .urns(Set.of(URN, absent))
                .batchSize(10)
                .scrollId(first.getScrollId())
                .build());

    assertTrue(second.getIssues().isEmpty());
    assertNull(second.getScrollId());
    ArgumentCaptor<String> scrollIds = ArgumentCaptor.forClass(String.class);
    verify(systemMetadata, times(2))
        .scroll(
            any(),
            any(),
            anyBoolean(),
            scrollIds.capture(),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class));
    assertNull(scrollIds.getAllValues().get(0));
    assertEquals(scrollIds.getAllValues().get(1), first.getScrollId());
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testFullScanReportsAMissingAspectOnce() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse sqlPage = systemMetadataPage(URN, null);
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(sqlPage);
    when(entityService.exists(any(), any(), isNull(), anyBoolean(), anyBoolean()))
        .thenReturn(Set.of(URN));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response(URN, false)));
    when(indexFields.scroll(any(), eq(v2Index), anyInt(), isNull(), isNull()))
        .thenReturn(
            new Page(
                List.of(
                    new DocumentValues(
                        "doc", URN.toString(), Map.of(FIELD, TextNode.valueOf("left-behind")))),
                null));
    ConsistencyService service = service(check, systemMetadata);

    CheckResult result =
        service.checkBatch(
            opContext,
            CheckBatchRequest.builder()
                .entityType("dataset")
                .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                .batchSize(10)
                .build());

    assertEquals(result.getIssues().size(), 1);
    assertEquals(result.getIssues().get(0).getEntityUrn(), URN);
    assertNull(result.getScrollId());
    verify(indexFields, never()).byDocumentId(any(), any(), any());
  }

  @Test
  public void testFailedDocumentReadDoesNotAdvanceTheSqlPage() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse sqlPage = systemMetadataPage(URN, new Object[] {"cursor"});
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(sqlPage);
    when(entityService.exists(any(), any(), isNull(), anyBoolean(), anyBoolean()))
        .thenReturn(Set.of(URN));
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenReturn(Map.of(URN, response(URN, true)));
    when(indexFields.byDocumentId(any(), eq(v2Index), any()))
        .thenThrow(new IOException("search down"));
    ConsistencyService service = service(check, systemMetadata);

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class,
            () ->
                service.checkBatch(
                    opContext,
                    CheckBatchRequest.builder()
                        .entityType("dataset")
                        .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                        .batchSize(10)
                        .build()));

    assertTrue(failure.getMessage().contains(v2Index));
    assertEquals(failure.getCause().getMessage(), "search down");
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testFailedEntityLoadDoesNotFinishTheRun() throws Exception {
    StructuredPropertyIndexMismatchCheck check = check(v2Only());
    ESSystemMetadataDAO systemMetadata = mock(ESSystemMetadataDAO.class);
    SearchResponse sqlPage = systemMetadataPage(URN, new Object[] {"cursor"});
    when(systemMetadata.scroll(
            any(),
            any(),
            anyBoolean(),
            nullable(String.class),
            nullable(String.class),
            nullable(String.class),
            nullable(Integer.class)))
        .thenReturn(sqlPage);
    when(entityService.getEntitiesV2(any(), eq("dataset"), any(), any(), anyBoolean()))
        .thenThrow(new URISyntaxException("urn", "bad"));
    ConsistencyService service = service(check, systemMetadata);

    IllegalStateException failure =
        expectThrows(
            IllegalStateException.class,
            () ->
                service.checkBatch(
                    opContext,
                    CheckBatchRequest.builder()
                        .entityType("dataset")
                        .checkIds(List.of(StructuredPropertyIndexMismatchCheck.CHECK_ID))
                        .batchSize(10)
                        .build()));

    assertTrue(failure.getMessage().contains("dataset"));
    assertTrue(failure.getCause() instanceof URISyntaxException);
    verify(indexFields, never())
        .scroll(any(), any(), anyInt(), nullable(String.class), nullable(String.class));
  }

  @Test
  public void testWholeNumberTokensMatchTheDoubleProjection() {
    JsonNode expected =
        JsonNodeFactory.instance.arrayNode().add(JsonNodeFactory.instance.numberNode(1.0d));
    JsonNode wholeNumber =
        JsonNodeFactory.instance.arrayNode().add(JsonNodeFactory.instance.numberNode(1));
    JsonNode other =
        JsonNodeFactory.instance.arrayNode().add(JsonNodeFactory.instance.numberNode(2));

    StructuredPropertyIndexMismatchCheck.FieldChange match =
        StructuredPropertyIndexMismatchCheck.diff(
            Map.of(FIELD, expected), Map.of(FIELD, wholeNumber), Set.of(FIELD));
    StructuredPropertyIndexMismatchCheck.FieldChange mismatch =
        StructuredPropertyIndexMismatchCheck.diff(
            Map.of(FIELD, expected), Map.of(FIELD, other), Set.of(FIELD));

    assertTrue(match.isEmpty());
    assertEquals(mismatch.different(), 1);
  }

  private static SearchResponse systemMetadataPage(Urn urn, Object[] sortValues) {
    SearchHit hit = mock(SearchHit.class);
    when(hit.getSourceAsMap()).thenReturn(Map.of("urn", urn.toString()));
    when(hit.getIndex()).thenReturn("system_metadata");
    when(hit.getId()).thenReturn(urn.toString());
    when(hit.getSortValues()).thenReturn(sortValues);
    SearchResponse page = mock(SearchResponse.class);
    when(page.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {hit},
                new org.apache.lucene.search.TotalHits(
                    1, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                0f));
    return page;
  }

  private ConsistencyService service(
      StructuredPropertyIndexMismatchCheck check, ESSystemMetadataDAO systemMetadata) {
    return new ConsistencyService(
        entityService,
        systemMetadata,
        null,
        new ConsistencyCheckRegistry(List.of(check)),
        new ConsistencyFixRegistry(List.of()));
  }

  private StructuredPropertyIndexMismatchCheck check(ElasticSearchConfiguration configuration) {
    return new StructuredPropertyIndexMismatchCheck(
        cache,
        indexFields,
        transformer,
        configuration,
        entityService,
        new Sha256UrnEntityDocumentIdHasher(),
        new com.fasterxml.jackson.databind.ObjectMapper());
  }

  private void project(String field, String value) throws Exception {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set(field, TextNode.valueOf(value));
    when(transformer.transformAspect(any(), any(), any(), any(), any(), any()))
        .thenReturn(Optional.of(document));
  }

  private Map<String, DocumentValues> documents(Urn urn, Map<String, JsonNode> values) {
    return documents(urn, values, false);
  }

  private Map<String, DocumentValues> documents(Urn urn, Map<String, JsonNode> values, boolean v3) {
    String documentId =
        v3
            ? new Sha256UrnEntityDocumentIdHasher().documentId(opContext, urn)
            : opContext.getSearchContext().getIndexConvention().getEntityDocumentId(urn);
    return Map.of(documentId, new DocumentValues(documentId, urn.toString(), values));
  }

  private static EntityResponse response(Urn urn, boolean withAspect) {
    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName(urn.getEntityType());
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    if (withAspect) {
      StructuredProperties properties = new StructuredProperties();
      properties.setProperties(new StructuredPropertyValueAssignmentArray());
      EnvelopedAspect enveloped = new EnvelopedAspect();
      enveloped.setName(STRUCTURED_PROPERTIES_ASPECT_NAME);
      enveloped.setValue(new Aspect(properties.data()));
      aspects.put(STRUCTURED_PROPERTIES_ASPECT_NAME, enveloped);
    }
    response.setAspects(aspects);
    return response;
  }

  private static ElasticSearchConfiguration v2Only() {
    return configuration(true, false);
  }

  private static ElasticSearchConfiguration v3Only() {
    return configuration(false, true);
  }

  private static ElasticSearchConfiguration v2AndV3() {
    return configuration(true, true);
  }

  private static ElasticSearchConfiguration v3KeywordRead() {
    return ElasticSearchConfiguration.builder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(true)
                        .build())
                .build())
        .build();
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
