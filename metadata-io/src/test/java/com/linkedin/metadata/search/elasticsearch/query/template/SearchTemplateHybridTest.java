package com.linkedin.metadata.search.elasticsearch.query.template;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.List;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class SearchTemplateHybridTest {

  @AfterMethod
  public void clearFullTextFields() {
    StructuredPropertyFullTextFields.replace(List.of());
  }

  @Test
  public void testRerankReordersDocumentsInsideTheKeywordWindow() throws Exception {
    SearchResult keyword =
        result(
            entity("urn:li:document:doc-a", 10f),
            entity("urn:li:dataset:(urn:li:dataPlatform:hive,t,PROD)", 5f),
            entity("urn:li:document:doc-b", 1f));
    SearchResult knn =
        result(
            entity("urn:li:document:doc-a", 1f),
            entity("urn:li:document:doc-b", 1000f),
            entity("urn:li:document:doc-2", 50f));

    SearchResult reranked = SearchTemplateHybrid.rerank(keyword, knn, 1f, 1f, null);

    assertEquals(reranked.getNumEntities(), 3);
    assertEquals(reranked.getEntities().size(), 3);
    assertEquals(reranked.getEntities().get(0).getEntity().toString(), "urn:li:document:doc-b");
    assertEquals(reranked.getEntities().get(0).getScore(), 10.0);
    assertEquals(reranked.getEntities().get(1).getEntity().getEntityType(), "dataset");
    assertEquals(reranked.getEntities().get(1).getScore(), 5.0);
    assertEquals(reranked.getEntities().get(2).getEntity().toString(), "urn:li:document:doc-a");
    assertEquals(reranked.getEntities().get(2).getScore(), 1.0);
    assertTrue(SearchTemplateHybrid.includesDocument(List.of("dataset", "document")));
    assertFalse(SearchTemplateHybrid.includesDocument(List.of("dataset")));
  }

  @Test
  public void testRerankKeepsADocumentWithoutAVector() throws Exception {
    SearchResult keyword =
        result(entity("urn:li:document:doc-a", 10f), entity("urn:li:document:doc-b", 1f));
    SearchResult knn = result(entity("urn:li:document:doc-a", 4f));

    SearchResult reranked = SearchTemplateHybrid.rerank(keyword, knn, 1f, 1f, 5f);

    assertEquals(reranked.getEntities().get(0).getEntity().toString(), "urn:li:document:doc-a");
    assertEquals(reranked.getEntities().get(0).getScore(), 10.0);
    assertEquals(reranked.getEntities().get(1).getEntity().toString(), "urn:li:document:doc-b");
  }

  @Test
  public void testFullTextOptInRegistersOnlyOptedInProperties() throws Exception {
    StructuredPropertyDefinition opted = new StructuredPropertyDefinition();
    opted.setQualifiedName("io.acme.summary");
    opted.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    opted.setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    DataHubSearchConfig config = new DataHubSearchConfig();
    config.setFullTextSearch(true);
    opted.setSearchConfiguration(config);

    StructuredPropertyDefinition skipped = new StructuredPropertyDefinition();
    skipped.setQualifiedName("io.acme.hidden");
    skipped.setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));
    skipped.setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));

    Urn optedUrn = UrnUtils.getUrn("urn:li:structuredProperty:summary");
    Urn skippedUrn = UrnUtils.getUrn("urn:li:structuredProperty:hidden");
    StructuredPropertyFullTextFields.replace(
        List.of(Pair.of(optedUrn, opted), Pair.of(skippedUrn, skipped)));

    assertEquals(StructuredPropertyFullTextFields.fields().size(), 1);
    assertEquals(StructuredPropertyFullTextFields.fields().get(0).fieldName(), "customProperties");
    assertEquals(StructuredPropertyFullTextFields.fields().get(0).boost(), 0.8f);
    assertTrue(
        StructuredPropertyFullTextFields.sourceFields().stream()
            .anyMatch(name -> name.contains("summary")));
    assertTrue(
        StructuredPropertyFullTextFields.sourceFields().stream()
            .noneMatch(name -> name.contains("hidden")));
  }

  private static SearchResult result(SearchEntity... entities) {
    return new SearchResult()
        .setEntities(new SearchEntityArray(List.of(entities)))
        .setNumEntities(entities.length)
        .setFrom(0)
        .setPageSize(10)
        .setMetadata(new SearchResultMetadata().setAggregations(new AggregationMetadataArray()));
  }

  private static SearchEntity entity(String urn, float score) {
    return new SearchEntity().setEntity(UrnUtils.getUrn(urn)).setScore(score);
  }
}
