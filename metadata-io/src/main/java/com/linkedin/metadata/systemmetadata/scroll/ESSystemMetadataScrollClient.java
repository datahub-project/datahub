package com.linkedin.metadata.systemmetadata.scroll;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.utils.UrnExtractionUtils;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;

/**
 * Elasticsearch-backed implementation of {@link SystemMetadataScrollClient}.
 *
 * <p>Translates {@link SystemMetadataScrollRequest} into a {@link BoolQueryBuilder} against the
 * {@code system_metadata_service_v1} index and uses {@code search_after} pagination via {@link
 * SearchAfterWrapper}.
 */
@Slf4j
@RequiredArgsConstructor
public class ESSystemMetadataScrollClient implements SystemMetadataScrollClient {

  private static final String FIELD_URN = "urn";
  private static final String FIELD_ASPECT = "aspect";
  private static final String FIELD_ASPECT_MODIFIED = "aspectModifiedTime";
  private static final String FIELD_ASPECT_CREATED = "aspectCreatedTime";
  private static final String SCROLL_KEEP_ALIVE = "5m";

  private final ESSystemMetadataDAO esSystemMetadataDAO;

  @Override
  @Nonnull
  public SystemMetadataScrollResult scrollUrns(
      @Nonnull OperationContext opContext, @Nonnull SystemMetadataScrollRequest request) {
    BoolQueryBuilder query = buildQuery(request);

    SearchResponse response =
        esSystemMetadataDAO.scroll(
            opContext,
            query,
            request.isIncludeSoftDeleted(),
            request.getScrollId(),
            null,
            SCROLL_KEEP_ALIVE,
            request.getBatchSize());

    if (response == null
        || response.getHits() == null
        || response.getHits().getHits().length == 0) {
      return SystemMetadataScrollResult.empty();
    }

    Set<Urn> urns = UrnExtractionUtils.extractUniqueUrns(response);
    String nextScrollId = extractNextScrollId(response);

    return SystemMetadataScrollResult.builder().urns(urns).nextScrollId(nextScrollId).build();
  }

  /**
   * Build the OpenSearch boolean query for a scroll request.
   *
   * <p>Package-private to allow {@link com.linkedin.metadata.aspect.consistency.ConsistencyService}
   * unit tests to inspect the produced query without going through the live DAO.
   */
  @Nonnull
  BoolQueryBuilder buildQuery(@Nonnull SystemMetadataScrollRequest request) {
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    query.filter(QueryBuilders.prefixQuery(FIELD_URN, "urn:li:" + request.getEntityType() + ":"));

    Set<Urn> urns = request.getUrns();
    if (urns != null && !urns.isEmpty()) {
      Set<String> urnStrings = urns.stream().map(Urn::toString).collect(Collectors.toSet());
      query.filter(QueryBuilders.termsQuery(FIELD_URN, urnStrings));
    }

    List<String> aspects = request.getAspects();
    if (aspects != null && !aspects.isEmpty()) {
      if (aspects.size() == 1) {
        query.filter(QueryBuilders.termQuery(FIELD_ASPECT, aspects.get(0)));
      } else {
        query.filter(QueryBuilders.termsQuery(FIELD_ASPECT, aspects));
      }
    }

    Long ge = request.getGePitEpochMs();
    Long le = request.getLePitEpochMs();
    if (ge != null || le != null) {
      // Prefer aspectModifiedTime; fall back to aspectCreatedTime when modified is absent.
      BoolQueryBuilder timestampQuery = QueryBuilders.boolQuery();

      BoolQueryBuilder modifiedTimeQuery = QueryBuilders.boolQuery();
      modifiedTimeQuery.must(QueryBuilders.existsQuery(FIELD_ASPECT_MODIFIED));
      if (ge != null) {
        modifiedTimeQuery.must(QueryBuilders.rangeQuery(FIELD_ASPECT_MODIFIED).gte(ge));
      }
      if (le != null) {
        modifiedTimeQuery.must(QueryBuilders.rangeQuery(FIELD_ASPECT_MODIFIED).lte(le));
      }

      BoolQueryBuilder createdTimeQuery = QueryBuilders.boolQuery();
      createdTimeQuery.mustNot(QueryBuilders.existsQuery(FIELD_ASPECT_MODIFIED));
      if (ge != null) {
        createdTimeQuery.must(QueryBuilders.rangeQuery(FIELD_ASPECT_CREATED).gte(ge));
      }
      if (le != null) {
        createdTimeQuery.must(QueryBuilders.rangeQuery(FIELD_ASPECT_CREATED).lte(le));
      }

      timestampQuery.should(modifiedTimeQuery);
      timestampQuery.should(createdTimeQuery);
      timestampQuery.minimumShouldMatch(1);

      query.filter(timestampQuery);
    }

    return query;
  }

  @Nullable
  private String extractNextScrollId(@Nonnull SearchResponse response) {
    SearchHit[] hits = response.getHits().getHits();
    if (hits.length == 0) {
      return null;
    }
    SearchHit lastHit = hits[hits.length - 1];
    Object[] sortValues = lastHit.getSortValues();
    if (sortValues == null || sortValues.length == 0) {
      return null;
    }
    SearchAfterWrapper wrapper = new SearchAfterWrapper(sortValues, null, 0);
    return wrapper.toScrollId();
  }
}
