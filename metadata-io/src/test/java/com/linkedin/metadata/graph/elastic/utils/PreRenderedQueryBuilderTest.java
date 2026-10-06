package com.linkedin.metadata.graph.elastic.utils;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.testng.annotations.Test;

public class PreRenderedQueryBuilderTest {

  private static final QueryBuilder QUERY =
      QueryBuilders.boolQuery()
          .filter(QueryBuilders.termsQuery("destination.urn", List.of("urn:li:a", "urn:li:b")))
          .filter(QueryBuilders.termQuery("relationshipType", "DownstreamOf"));

  private static String render(QueryBuilder query) throws Exception {
    SearchSourceBuilder source =
        new SearchSourceBuilder().query(query).size(5000).slice(new SliceBuilder(1, 6));
    return XContentHelper.toXContent(source, XContentType.JSON, false).utf8ToString();
  }

  @Test
  public void testRequestBodyMatchesOriginalQuery() throws Exception {
    assertEquals(render(PreRenderedQueryBuilder.of(QUERY)), render(QUERY));
  }

  /** The Elasticsearch 8 client shim converts queries by parsing {@code toString()} as JSON. */
  @Test
  public void testToStringParsesToTheSameJsonAsOriginalQuery() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    assertEquals(
        mapper.readTree(PreRenderedQueryBuilder.of(QUERY).toString()),
        mapper.readTree(QUERY.toString()));
  }

  @Test
  public void testReportsWrappedQueryNameAndBoost() throws Exception {
    PreRenderedQueryBuilder wrapped =
        PreRenderedQueryBuilder.of(QueryBuilders.termQuery("f", "v").boost(2.0f).queryName("n"));
    assertEquals(wrapped.boost(), 2.0f);
    assertEquals(wrapped.queryName(), "n");
  }
}
