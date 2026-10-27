package com.linkedin.metadata.config.usage.cigate.graphql;

import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfile;
import com.linkedin.metadata.config.usage.cigate.UsageRegistryCiGateProfiles;
import graphql.language.Document;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ObjectTypeExtensionDefinition;
import graphql.language.OperationDefinition;
import graphql.language.TypeDefinition;
import graphql.parser.Parser;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public final class GraphqlUsageSurfaceExtractor {

  private GraphqlUsageSurfaceExtractor() {}

  @Nonnull
  public static GraphqlUsageSurface extract(@Nonnull Path repoRoot) throws IOException {
    return extract(repoRoot, UsageRegistryCiGateProfiles.active());
  }

  @Nonnull
  public static GraphqlUsageSurface extract(
      @Nonnull Path repoRoot, @Nonnull UsageRegistryCiGateProfile profile) throws IOException {
    Set<String> queryFields = new LinkedHashSet<>();
    Set<String> mutationFields = new LinkedHashSet<>();
    for (String schemaDirSuffix : profile.graphqlSchemaDirSuffixes()) {
      queryFields.addAll(extractServerRootFields(repoRoot.resolve(schemaDirSuffix), "Query"));
      mutationFields.addAll(extractServerRootFields(repoRoot.resolve(schemaDirSuffix), "Mutation"));
    }
    Set<String> clientOps = extractClientOperationNames(repoRoot, profile);
    return new GraphqlUsageSurface(queryFields, mutationFields, clientOps);
  }

  @Nonnull
  private static Set<String> extractServerRootFields(
      @Nonnull Path schemaDir, @Nonnull String typeName) throws IOException {
    if (!Files.isDirectory(schemaDir)) {
      return Set.of();
    }
    TypeDefinitionRegistry registry = new TypeDefinitionRegistry();
    SchemaParser parser = new SchemaParser();
    try (Stream<Path> paths = Files.walk(schemaDir)) {
      paths
          .filter(path -> path.toString().endsWith(".graphql"))
          .forEach(
              path -> {
                try {
                  registry.merge(parser.parse(path.toFile()));
                } catch (Exception e) {
                  throw new IllegalStateException("Failed to parse GraphQL schema: " + path, e);
                }
              });
    }

    Set<String> fields = new LinkedHashSet<>();
    for (TypeDefinition<?> typeDef : registry.types().values()) {
      if (typeDef instanceof ObjectTypeDefinition objectType
          && typeName.equals(objectType.getName())) {
        for (FieldDefinition field : objectType.getFieldDefinitions()) {
          fields.add(field.getName());
        }
      }
      if (typeDef instanceof ObjectTypeExtensionDefinition extension
          && typeName.equals(extension.getName())) {
        for (FieldDefinition field : extension.getFieldDefinitions()) {
          fields.add(field.getName());
        }
      }
    }
    registry.objectTypeExtensions().getOrDefault(typeName, java.util.List.of()).stream()
        .flatMap(extension -> extension.getFieldDefinitions().stream())
        .map(FieldDefinition::getName)
        .forEach(fields::add);
    return Set.copyOf(fields);
  }

  @Nonnull
  private static Set<String> extractClientOperationNames(
      @Nonnull Path repoRoot, @Nonnull UsageRegistryCiGateProfile profile) throws IOException {
    Set<String> names = new LinkedHashSet<>();
    Parser parser = new Parser();
    for (String clientDirSuffix : profile.graphqlClientDirSuffixes()) {
      Path clientDir = repoRoot.resolve(clientDirSuffix);
      if (!Files.isDirectory(clientDir)) {
        continue;
      }
      try (Stream<Path> paths = Files.walk(clientDir)) {
        paths
            .filter(path -> path.toString().endsWith(".graphql"))
            .forEach(
                path -> {
                  try {
                    Document document = parser.parseDocument(Files.readString(path));
                    document.getDefinitions().stream()
                        .filter(OperationDefinition.class::isInstance)
                        .map(OperationDefinition.class::cast)
                        .map(OperationDefinition::getName)
                        .filter(name -> name != null && !name.isBlank())
                        .forEach(names::add);
                  } catch (Exception e) {
                    throw new IllegalStateException("Failed to parse client GraphQL: " + path, e);
                  }
                });
      }
    }
    return Set.copyOf(names);
  }
}
