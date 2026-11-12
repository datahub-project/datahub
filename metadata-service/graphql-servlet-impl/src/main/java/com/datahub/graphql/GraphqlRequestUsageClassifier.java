package com.datahub.graphql;

import com.linkedin.metadata.ratelimit.GraphqlDocumentMetadata;
import graphql.language.OperationDefinition;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import javax.annotation.Nonnull;

/** Classifies GraphQL HTTP requests for usage aggregation (operation + operation kind). */
public final class GraphqlRequestUsageClassifier {

  public record Result(
      @Nonnull UsageOperation usageOperation, @Nonnull GraphQLOperationKind kind) {}

  private GraphqlRequestUsageClassifier() {}

  @Nonnull
  public static Result classify(
      @Nonnull GraphqlDocumentMetadata documentMetadata,
      @Nonnull GraphqlUsageClassificationRegistry registry) {
    UsageOperation fromOperationName =
        registry.resolveByOperationName(documentMetadata.resolvedOperationName()).orElse(null);
    if (fromOperationName != null) {
      return new Result(fromOperationName, fromOperationName.defaultGraphqlOperationKind());
    }

    if (!documentMetadata.isParsed()) {
      GraphQLOperationKind kind = toOperationKind(documentMetadata.prefixOperationKind());
      UsageOperation usageOperation =
          registry.resolve(documentMetadata.resolvedOperationName(), kind, java.util.List.of());
      return new Result(usageOperation, kind);
    }

    GraphQLOperationKind operationKind =
        toOperationKind(documentMetadata.selectedOperation().getKind());
    UsageOperation usageOperation =
        registry.resolve(
            documentMetadata.resolvedOperationName(),
            operationKind,
            documentMetadata.allRootFields());
    return new Result(usageOperation, operationKind);
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
