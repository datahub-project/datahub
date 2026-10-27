package com.linkedin.metadata.ratelimit;

import graphql.language.OperationDefinition;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/** Metadata for a single operation within a GraphQL document. */
@Value
public class GraphqlOperationMetadata {

  @Nullable String name;

  @Nonnull OperationDefinition.Operation kind;

  @Nonnull List<String> rootFields;
}
