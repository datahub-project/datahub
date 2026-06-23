package com.linkedin.metadata.ratelimit;

import graphql.language.OperationDefinition;
import graphql.parser.InvalidSyntaxException;
import graphql.parser.Parser;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.util.StringUtils;

public final class GraphQLOperationNameResolver {

  private static final Parser PARSER = new Parser();

  private GraphQLOperationNameResolver() {}

  @Nonnull
  public static String resolve(@Nullable String jsonOperationName, @Nullable String queryDocument) {
    if (StringUtils.hasText(jsonOperationName)) {
      return jsonOperationName;
    }
    if (!StringUtils.hasText(queryDocument)) {
      return "graphql";
    }

    try {
      List<OperationDefinition> operations =
          PARSER.parseDocument(queryDocument).getDefinitions().stream()
              .filter(def -> def instanceof OperationDefinition)
              .map(def -> (OperationDefinition) def)
              .toList();

      if (operations.isEmpty()) {
        return "graphql";
      }

      return operations.stream()
          .map(OperationDefinition::getName)
          .filter(StringUtils::hasText)
          .findFirst()
          .orElse("graphql");
    } catch (InvalidSyntaxException e) {
      return "graphql";
    }
  }
}
