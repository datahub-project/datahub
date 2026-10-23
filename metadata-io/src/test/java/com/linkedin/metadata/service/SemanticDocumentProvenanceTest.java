package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticDocumentProvenance;
import com.linkedin.metadata.search.elasticsearch.index.entity.SemanticEmbeddingMappings;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import io.datahubproject.metadata.context.OperationContext;
import org.testng.annotations.Test;

public class SemanticDocumentProvenanceTest {

  private static final Urn DOCUMENT_URN = UrnUtils.getUrn("urn:li:document:bridge-1");

  @Test
  public void testV3DocumentInfoNestedTextStampsWithoutRootFields() {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObject(
            any(OperationContext.class), any(Urn.class), eq(Constants.SEMANTIC_TEXT_ASPECT_NAME)))
        .thenReturn(null);
    OperationContext ctx = contextWithRetriever(retriever);

    ObjectNode doc = documentWithNestedAspect(Constants.DOCUMENT_INFO_ASPECT_NAME);
    ((ObjectNode)
            doc.get(MappingConstants.ASPECTS_FIELD_NAME).get(Constants.DOCUMENT_INFO_ASPECT_NAME))
        .put("text", "the body");

    SemanticDocumentProvenance.stampResolvedTextSha256(
        ctx,
        DOCUMENT_URN,
        Constants.DOCUMENT_ENTITY_NAME,
        Constants.DOCUMENT_INFO_ASPECT_NAME,
        doc);

    assertEquals(
        doc.get(SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD).asText(),
        SemanticDocumentProvenance.sha256Hex("the body"));
  }

  @Test
  public void testV3SemanticTextNestedOverrideStamps() {
    AspectRetriever retriever = mock(AspectRetriever.class);
    OperationContext ctx = contextWithRetriever(retriever);

    ObjectNode doc = documentWithNestedAspect(Constants.SEMANTIC_TEXT_ASPECT_NAME);
    ((ObjectNode)
            doc.get(MappingConstants.ASPECTS_FIELD_NAME).get(Constants.SEMANTIC_TEXT_ASPECT_NAME))
        .put("semanticText", "curated override");

    SemanticDocumentProvenance.stampResolvedTextSha256(
        ctx,
        DOCUMENT_URN,
        Constants.DOCUMENT_ENTITY_NAME,
        Constants.SEMANTIC_TEXT_ASPECT_NAME,
        doc);

    assertEquals(
        doc.get(SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD).asText(),
        SemanticDocumentProvenance.sha256Hex("curated override"));
    verify(retriever, never())
        .getLatestAspectObject(any(OperationContext.class), any(Urn.class), anyString());
  }

  @Test
  public void testV3ContentsPathFallbackForDocumentBody() {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObject(
            any(OperationContext.class), any(Urn.class), eq(Constants.SEMANTIC_TEXT_ASPECT_NAME)))
        .thenReturn(null);
    OperationContext ctx = contextWithRetriever(retriever);

    ObjectNode doc = documentWithNestedAspect(Constants.DOCUMENT_INFO_ASPECT_NAME);
    ObjectNode contents = JsonNodeFactory.instance.objectNode();
    contents.put("text", "nested contents");
    ((ObjectNode)
            doc.get(MappingConstants.ASPECTS_FIELD_NAME).get(Constants.DOCUMENT_INFO_ASPECT_NAME))
        .set("contents", contents);

    SemanticDocumentProvenance.stampResolvedTextSha256(
        ctx, DOCUMENT_URN, Constants.DOCUMENT_ENTITY_NAME, "semanticContent", doc);

    assertEquals(
        doc.get(SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD).asText(),
        SemanticDocumentProvenance.sha256Hex("nested contents"));
  }

  @Test
  public void testUnrelatedAspectWithoutTextLeavesStampAbsent() {
    AspectRetriever retriever = mock(AspectRetriever.class);
    OperationContext ctx = contextWithRetriever(retriever);

    ObjectNode doc = JsonNodeFactory.instance.objectNode();
    doc.put("removed", false);

    SemanticDocumentProvenance.stampResolvedTextSha256(
        ctx, DOCUMENT_URN, Constants.DOCUMENT_ENTITY_NAME, Constants.STATUS_ASPECT_NAME, doc);

    assertNull(doc.get(SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD));
    verify(retriever, never())
        .getLatestAspectObject(any(OperationContext.class), any(Urn.class), anyString());
  }

  private static OperationContext contextWithRetriever(AspectRetriever retriever) {
    OperationContext ctx = mock(OperationContext.class);
    when(ctx.getAspectRetriever()).thenReturn(retriever);
    return ctx;
  }

  private static ObjectNode documentWithNestedAspect(String aspectName) {
    ObjectNode doc = JsonNodeFactory.instance.objectNode();
    ObjectNode aspects = JsonNodeFactory.instance.objectNode();
    aspects.set(aspectName, JsonNodeFactory.instance.objectNode());
    doc.set(MappingConstants.ASPECTS_FIELD_NAME, aspects);
    return doc;
  }
}
