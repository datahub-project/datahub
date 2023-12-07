package io.datahubproject.test.fixtures.search;

import com.google.common.collect.Lists;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

@Builder
public class LineageExporter<O> {
  @Builder.Default private int fetchSize = 3000;
  @Builder.Default private int queryStatementSize = 32000;
  @NonNull private FixtureWriter writer;
  private String entityIndexName;

  private String graphIndexName;

  private String entityOutputPath;
  private String graphOutputPath;

  private Class<O> anonymizerClazz;

  private static String idToUrn(String id) {
    return URLDecoder.decode(id, StandardCharsets.UTF_8);
  }

  public <O> void export(Set<String> ids) {
    if (entityIndexName != null) {
      assert (entityOutputPath != null);
      exportEntityIndex(
          ids.stream()
              .map(id -> URLEncoder.encode(id, StandardCharsets.UTF_8))
              .collect(Collectors.toSet()),
          new HashSet<>(),
          0);
    }
    if (graphIndexName != null) {
      assert (graphOutputPath != null);
      exportGraphIndex(ids, new HashSet<>(), new HashSet<>(), 0);
    }
  }

  public void exportGraphIndex(
      Set<String> urns, Set<String> visitedUrns, Set<String> visitedIds, int hops) {
    Set<String> nextIds = new HashSet<>();
    if (!urns.isEmpty()) {
      BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

      boolQueryBuilder.must(QueryBuilders.termQuery("relationshipType", "DownstreamOf"));

      Lists.partition(Arrays.asList(urns.toArray(String[]::new)), queryStatementSize)
          .forEach(
              batch -> {
                boolQueryBuilder.should(
                    QueryBuilders.termsQuery("source.urn", batch.toArray(String[]::new)));
                boolQueryBuilder.should(
                    QueryBuilders.termsQuery("destination.urn", batch.toArray(String[]::new)));
              });
      boolQueryBuilder.minimumShouldMatch(1);

      // Exclude visited
      Lists.partition(Arrays.asList(visitedIds.toArray(String[]::new)), queryStatementSize)
          .forEach(
              batch ->
                  boolQueryBuilder.mustNot(
                      QueryBuilders.idsQuery().addIds(batch.toArray(String[]::new))));

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(fetchSize);
      searchSourceBuilder.query(boolQueryBuilder);
      searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

      SearchRequest searchRequest = new SearchRequest(graphIndexName);
      searchRequest.source(searchSourceBuilder);

      Set<String> docIds = new HashSet<>();
      Set<GraphDocument> docs = new HashSet<>();

      long startTime = System.currentTimeMillis();
      System.out.printf(
          "Hops: %s (Ids: %s) [VisitedIds: %s]", hops, urns.size(), visitedUrns.size());

      writer.write(
          searchRequest,
          graphOutputPath,
          hops != 0,
          anonymizerClazz,
          GraphDocument.class,
          (hit, doc) -> {
            docIds.add(hit.getId());
            docs.add(doc);
          });

      long endTime = System.currentTimeMillis();
      System.out.printf(" Time: %ss%n", (endTime - startTime) / 1000);

      visitedIds.addAll(docIds);
      visitedUrns.addAll(urns);

      Set<String> discoveredUrns =
          docs.stream()
              .flatMap(d -> Stream.of(d.destination.urn, d.source.urn))
              .filter(Objects::nonNull)
              .filter(urn -> !visitedUrns.contains(urn))
              .collect(Collectors.toSet());

      nextIds.addAll(discoveredUrns);
    }

    if (!nextIds.isEmpty()) {
      exportGraphIndex(nextIds, visitedUrns, visitedIds, hops + 1);
    }
  }

  public void exportEntityIndex(Set<String> ids, Set<String> visitedIds, int hops) {
    Set<String> nextIds = new HashSet<>();

    if (!ids.isEmpty()) {
      Set<String> urns = ids.stream().map(LineageExporter::idToUrn).collect(Collectors.toSet());

      BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

      Lists.partition(Arrays.asList(urns.toArray(String[]::new)), queryStatementSize)
          .forEach(
              batch ->
                  boolQueryBuilder.should(
                      QueryBuilders.termsQuery("upstreams.keyword", batch.toArray(String[]::new))));
      Lists.partition(Arrays.asList(ids.toArray(String[]::new)), queryStatementSize)
          .forEach(
              batch ->
                  boolQueryBuilder.should(
                      QueryBuilders.idsQuery().addIds(batch.toArray(String[]::new))));
      boolQueryBuilder.minimumShouldMatch(1);

      // Exclude visited
      Lists.partition(Arrays.asList(visitedIds.toArray(String[]::new)), queryStatementSize)
          .forEach(
              batch ->
                  boolQueryBuilder.mustNot(
                      QueryBuilders.idsQuery().addIds(batch.toArray(String[]::new))));

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(fetchSize);
      searchSourceBuilder.query(boolQueryBuilder);
      searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

      SearchRequest searchRequest = new SearchRequest(entityIndexName);
      searchRequest.source(searchSourceBuilder);

      Set<String> docIds = new HashSet<>();
      Set<UrnDocument> docs = new HashSet<>();

      long startTime = System.currentTimeMillis();
      System.out.printf("Hops: %s (Ids: %s) [VisitedIds: %s]", hops, ids.size(), visitedIds.size());

      writer.write(
          searchRequest,
          entityOutputPath,
          hops != 0,
          anonymizerClazz,
          UrnDocument.class,
          (hit, doc) -> {
            docIds.add(hit.getId());
            docs.add(doc);
          });

      long endTime = System.currentTimeMillis();
      System.out.printf(" Time: %ss%n", (endTime - startTime) / 1000);

      visitedIds.addAll(docIds);

      nextIds.addAll(
          docIds.stream()
              .filter(Objects::nonNull)
              .filter(docId -> !visitedIds.contains(docId))
              .collect(Collectors.toSet()));
      nextIds.addAll(
          docs.stream()
              .filter(doc -> doc.upstreams != null && !doc.upstreams.isEmpty())
              .flatMap(doc -> doc.upstreams.stream())
              .map(urn -> URLEncoder.encode(urn, StandardCharsets.UTF_8))
              .filter(docId -> !visitedIds.contains(docId))
              .collect(Collectors.toSet()));
    }

    if (!nextIds.isEmpty()) {
      exportEntityIndex(nextIds, visitedIds, hops + 1);
    }
  }

  public static class UrnDocument {
    public String urn;
    public List<String> upstreams;
  }

  public static class GraphDocument {
    public String relationshipType;
    public GraphNode source;
    public GraphNode destination;

    public static class GraphNode {
      public String urn;
      public String entityType;
    }
  }
}
