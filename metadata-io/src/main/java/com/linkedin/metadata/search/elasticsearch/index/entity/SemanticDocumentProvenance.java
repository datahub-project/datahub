package com.linkedin.metadata.search.elasticsearch.index.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Stamps {@code resolvedTextSha256} on semantic and document-V3 search documents so coverage
 * reporting can compare against embed-time {@code embeddings.&lt;model&gt;.sourceTextSha256}.
 */
@Slf4j
public final class SemanticDocumentProvenance {

  private static final String BODY_TEXT_FIELD = "text";
  private static final String SEMANTIC_TEXT_FIELD = "semanticText";

  private SemanticDocumentProvenance() {}

  /**
   * See {@code UpdateIndicesV2Strategy.withResolvedTextSha256} for the full contract: document
   * entities only; semanticText override wins over document body; semanticContent projections fetch
   * both sides; retrieval failure writes JSON null.
   */
  public static void stampResolvedTextSha256(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nonnull ObjectNode document) {
    if (!Constants.DOCUMENT_ENTITY_NAME.equals(entityName)) {
      return;
    }
    boolean hasOverrideField = document.has(SEMANTIC_TEXT_FIELD);
    boolean hasBodyField = document.has(BODY_TEXT_FIELD);
    boolean isSemanticContentAspect =
        SearchDocumentTransformer.SEMANTIC_DATA_ASPECTS.contains(aspectName);
    if (!hasOverrideField && !hasBodyField && !isSemanticContentAspect) {
      return;
    }
    try {
      String override =
          hasOverrideField
              ? textValue(document.get(SEMANTIC_TEXT_FIELD))
              : fetchSemanticTextOverride(opContext, urn);
      final String resolved;
      if (override != null && !override.isEmpty()) {
        resolved = override;
      } else if (hasBodyField) {
        String body = textValue(document.get(BODY_TEXT_FIELD));
        resolved = body != null ? body : "";
      } else {
        String body = fetchDocumentBodyText(opContext, urn);
        resolved = body != null ? body : "";
      }
      document.put(SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD, sha256Hex(resolved));
    } catch (Exception e) {
      log.warn(
          "Failed to resolve embed text for {}; clearing {} (reads as unknown, never stale)",
          urn,
          SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD,
          e);
      document.set(
          SemanticEmbeddingMappings.RESOLVED_TEXT_SHA256_FIELD,
          JsonNodeFactory.instance.nullNode());
    }
  }

  @Nullable
  private static String fetchSemanticTextOverride(
      @Nonnull OperationContext opContext, @Nonnull Urn urn) {
    com.linkedin.entity.Aspect aspect =
        opContext
            .getAspectRetriever()
            .getLatestAspectObject(opContext, urn, Constants.SEMANTIC_TEXT_ASPECT_NAME);
    if (aspect == null) {
      return null;
    }
    Object text = aspect.data().get("text");
    return text != null ? text.toString() : null;
  }

  @Nullable
  private static String fetchDocumentBodyText(
      @Nonnull OperationContext opContext, @Nonnull Urn urn) {
    com.linkedin.entity.Aspect aspect =
        opContext
            .getAspectRetriever()
            .getLatestAspectObject(opContext, urn, Constants.DOCUMENT_INFO_ASPECT_NAME);
    if (aspect == null) {
      return null;
    }
    Object contents = aspect.data().get("contents");
    if (!(contents instanceof DataMap)) {
      return null;
    }
    Object text = ((DataMap) contents).get("text");
    return text != null ? text.toString() : null;
  }

  @Nullable
  private static String textValue(@Nullable JsonNode node) {
    return node != null && node.isTextual() ? node.asText() : null;
  }

  @Nonnull
  @VisibleForTesting
  public static String sha256Hex(@Nonnull String text) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(hash.length * 2);
      for (byte b : hash) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
