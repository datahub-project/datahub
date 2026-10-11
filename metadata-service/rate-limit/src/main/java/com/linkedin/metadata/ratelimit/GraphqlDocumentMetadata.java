package com.linkedin.metadata.ratelimit;

import graphql.language.OperationDefinition;
import io.datahubproject.metadata.context.graphql.GraphqlHttpConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.springframework.util.StringUtils;

/** Result of analyzing a GraphQL HTTP request body. Callers choose how to use operation details. */
@Getter
public final class GraphqlDocumentMetadata {

  /** Sentinel when no HTTP or document operation name is available. */
  public static final String ANONYMOUS_OPERATION_NAME =
      GraphqlHttpConstants.ANONYMOUS_OPERATION_NAME;

  @Nullable private final String httpOperationName;
  @Nonnull private final String resolvedOperationName;
  @Nonnull private final List<GraphqlOperationMetadata> operations;
  private final boolean parsed;
  @Nullable private final String queryDocument;

  private GraphqlDocumentMetadata(
      @Nullable String httpOperationName,
      @Nonnull String resolvedOperationName,
      @Nonnull List<GraphqlOperationMetadata> operations,
      boolean parsed,
      @Nullable String queryDocument) {
    this.httpOperationName = httpOperationName;
    this.resolvedOperationName = resolvedOperationName;
    this.operations = List.copyOf(operations);
    this.parsed = parsed;
    this.queryDocument = queryDocument;
  }

  @Nonnull
  public static GraphqlDocumentMetadata unparsed(
      @Nullable String httpOperationName,
      @Nonnull String resolvedOperationName,
      @Nullable String queryDocument) {
    return new GraphqlDocumentMetadata(
        httpOperationName, resolvedOperationName, List.of(), false, queryDocument);
  }

  @Nonnull
  public static GraphqlDocumentMetadata parsed(
      @Nullable String httpOperationName,
      @Nonnull String resolvedOperationName,
      @Nonnull List<GraphqlOperationMetadata> operations,
      @Nullable String queryDocument) {
    return new GraphqlDocumentMetadata(
        httpOperationName, resolvedOperationName, operations, true, queryDocument);
  }

  @Nonnull
  public static GraphqlDocumentMetadata anonymous(
      @Nullable String httpOperationName, @Nullable String queryDocument) {
    return unparsed(httpOperationName, ANONYMOUS_OPERATION_NAME, queryDocument);
  }

  /** Operation selected for execution: HTTP name match, else first named op, else first op. */
  @Nonnull
  public GraphqlOperationMetadata selectedOperation() {
    if (operations.isEmpty()) {
      return new GraphqlOperationMetadata(null, prefixOperationKind(), List.of());
    }
    if (StringUtils.hasText(httpOperationName)) {
      for (GraphqlOperationMetadata operation : operations) {
        if (httpOperationName.equals(operation.getName())) {
          return operation;
        }
      }
    }
    for (GraphqlOperationMetadata operation : operations) {
      if (StringUtils.hasText(operation.getName())) {
        return operation;
      }
    }
    return operations.get(0);
  }

  /**
   * Root fields used for usage classification.
   *
   * <p>When the HTTP operation name is set, returns root fields from {@link #selectedOperation()}
   * only (persisted multi-op documents execute one selected operation). When no HTTP name is
   * present, merges root fields from all operations in document order.
   *
   * <p>Requires {@link #isParsed()} — returns empty when analysis skipped AST parse (tier-2 fast
   * path). Callers with yaml operation-name hits must not need root fields.
   */
  @Nonnull
  public List<String> allRootFields() {
    if (!parsed) {
      return List.of();
    }
    if (StringUtils.hasText(httpOperationName)) {
      return List.copyOf(selectedOperation().getRootFields());
    }
    List<String> fields = new ArrayList<>();
    for (GraphqlOperationMetadata operation : operations) {
      fields.addAll(operation.getRootFields());
    }
    return List.copyOf(fields);
  }

  /** Prefix heuristic on the raw document (no AST). Defaults to QUERY. */
  @Nonnull
  public OperationDefinition.Operation prefixOperationKind() {
    return GraphqlDocumentAnalyzer.detectPrefixOperationKind(queryDocument);
  }

  @Nonnull
  public String resolvedOperationName() {
    return resolvedOperationName;
  }

  /**
   * Rate-limit bucket identity: named operation, else sorted top-level field names, else {@link
   * #ANONYMOUS_OPERATION_NAME}.
   */
  @Nonnull
  public String rateLimitIdentity() {
    if (!ANONYMOUS_OPERATION_NAME.equals(resolvedOperationName)) {
      return resolvedOperationName;
    }
    List<String> fields = allRootFields();
    if (fields.isEmpty()) {
      return ANONYMOUS_OPERATION_NAME;
    }
    List<String> sorted = new ArrayList<>(fields);
    sorted.sort(null);
    return String.join(",", sorted);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GraphqlDocumentMetadata that)) {
      return false;
    }
    return parsed == that.parsed
        && Objects.equals(httpOperationName, that.httpOperationName)
        && resolvedOperationName.equals(that.resolvedOperationName)
        && operations.equals(that.operations)
        && Objects.equals(queryDocument, that.queryDocument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        httpOperationName, resolvedOperationName, operations, parsed, queryDocument);
  }
}
