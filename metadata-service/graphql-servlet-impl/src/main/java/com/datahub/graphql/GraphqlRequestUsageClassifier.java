package com.datahub.graphql;

import com.linkedin.metadata.ratelimit.GraphqlDocumentAnalyzer;
import com.linkedin.metadata.ratelimit.GraphqlDocumentMetadata;
import graphql.language.Document;
import graphql.language.OperationDefinition;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Classifies GraphQL HTTP requests for usage aggregation (operation + operation kind). */
public final class GraphqlRequestUsageClassifier {

  public record Result(
      @Nonnull UsageOperation usageOperation, @Nonnull GraphQLOperationKind kind) {}

  private GraphqlRequestUsageClassifier() {}

  /**
   * The usage operation may come from the client-supplied operation name, but the kind always comes
   * from the operation the engine will execute: reads under a QUERY kind hide entities the caller
   * can't view, so a client must not be able to choose it.
   */
  @Nonnull
  public static Result classify(
      @Nonnull GraphqlDocumentMetadata documentMetadata,
      @Nonnull GraphqlUsageClassificationRegistry registry,
      @Nullable Function<String, Document> cachedDocumentLookup) {
    GraphQLOperationKind kind =
        toOperationKind(
            GraphqlDocumentAnalyzer.executedOperationKind(
                documentMetadata.getHttpOperationName(),
                documentMetadata.getQueryDocument(),
                cachedDocumentLookup));

    UsageOperation fromOperationName =
        registry.resolveByOperationName(documentMetadata.resolvedOperationName()).orElse(null);
    if (fromOperationName != null) {
      return new Result(fromOperationName, kind);
    }

    if (!documentMetadata.isParsed()) {
      UsageOperation usageOperation =
          registry.resolve(
              documentMetadata.resolvedOperationName(),
              toOperationKind(documentMetadata.prefixOperationKind()),
              List.of());
      return new Result(usageOperation, kind);
    }

    UsageOperation usageOperation =
        registry.resolve(
            documentMetadata.resolvedOperationName(),
            toOperationKind(documentMetadata.selectedOperation().getKind()),
            documentMetadata.allRootFields());
    return new Result(usageOperation, kind);
  }

  @Nonnull
  static GraphQLOperationKind toOperationKind(@Nonnull OperationDefinition.Operation operation) {
    return switch (operation) {
      case MUTATION -> GraphQLOperationKind.MUTATION;
      case SUBSCRIPTION -> GraphQLOperationKind.SUBSCRIPTION;
      case QUERY -> GraphQLOperationKind.QUERY;
    };
  }
}
