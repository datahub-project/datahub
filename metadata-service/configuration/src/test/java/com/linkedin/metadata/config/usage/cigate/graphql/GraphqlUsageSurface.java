package com.linkedin.metadata.config.usage.cigate.graphql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nonnull;

@JsonIgnoreProperties(ignoreUnknown = true)
public record GraphqlUsageSurface(
    @Nonnull Set<String> queryRootFields,
    @Nonnull Set<String> mutationRootFields,
    @Nonnull Set<String> clientOperationNames) {

  public GraphqlUsageSurface {
    queryRootFields = sortedCopy(queryRootFields);
    mutationRootFields = sortedCopy(mutationRootFields);
    clientOperationNames = sortedCopy(clientOperationNames);
  }

  @Nonnull
  public GraphqlUsageSurface merge(@Nonnull GraphqlUsageSurface overlay) {
    Set<String> query = new TreeSet<>(queryRootFields);
    query.addAll(overlay.queryRootFields);
    Set<String> mutation = new TreeSet<>(mutationRootFields);
    mutation.addAll(overlay.mutationRootFields);
    Set<String> clientOps = new TreeSet<>(clientOperationNames);
    clientOps.addAll(overlay.clientOperationNames);
    return new GraphqlUsageSurface(query, mutation, clientOps);
  }

  @Nonnull
  public List<GraphqlSurfaceEntry> allEntries() {
    List<GraphqlSurfaceEntry> entries = new ArrayList<>();
    for (String name : queryRootFields) {
      entries.add(new GraphqlSurfaceEntry(name, GraphqlSurfaceKind.QUERY_ROOT_FIELD));
    }
    for (String name : mutationRootFields) {
      entries.add(new GraphqlSurfaceEntry(name, GraphqlSurfaceKind.MUTATION_ROOT_FIELD));
    }
    for (String name : clientOperationNames) {
      entries.add(new GraphqlSurfaceEntry(name, GraphqlSurfaceKind.CLIENT_OPERATION));
    }
    return entries;
  }

  @Nonnull
  public List<GraphqlSurfaceEntry> delta(@Nonnull GraphqlUsageSurface baseline) {
    List<GraphqlSurfaceEntry> added = new ArrayList<>();
    for (String name : queryRootFields) {
      if (!baseline.queryRootFields.contains(name)) {
        added.add(new GraphqlSurfaceEntry(name, GraphqlSurfaceKind.QUERY_ROOT_FIELD));
      }
    }
    for (String name : mutationRootFields) {
      if (!baseline.mutationRootFields.contains(name)) {
        added.add(new GraphqlSurfaceEntry(name, GraphqlSurfaceKind.MUTATION_ROOT_FIELD));
      }
    }
    for (String name : clientOperationNames) {
      if (!baseline.clientOperationNames.contains(name)) {
        added.add(new GraphqlSurfaceEntry(name, GraphqlSurfaceKind.CLIENT_OPERATION));
      }
    }
    return added;
  }

  @Nonnull
  String toJson(@Nonnull ObjectMapper mapper) throws IOException {
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  @Nonnull
  static GraphqlUsageSurface fromJsonPath(@Nonnull Path path, @Nonnull ObjectMapper mapper)
      throws IOException {
    try (InputStream in = Files.newInputStream(path)) {
      return mapper.readValue(in, GraphqlUsageSurface.class);
    }
  }

  @Nonnull
  private static Set<String> sortedCopy(@Nonnull Set<String> values) {
    return new TreeSet<>(values);
  }

  public enum GraphqlSurfaceKind {
    QUERY_ROOT_FIELD,
    MUTATION_ROOT_FIELD,
    CLIENT_OPERATION
  }

  public record GraphqlSurfaceEntry(@Nonnull String name, @Nonnull GraphqlSurfaceKind kind) {

    @Nonnull
    String key() {
      return kind.name() + ":" + name;
    }
  }
}
