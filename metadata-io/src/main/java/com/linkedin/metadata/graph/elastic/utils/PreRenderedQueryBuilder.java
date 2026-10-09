package com.linkedin.metadata.graph.elastic.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.apache.lucene.search.Query;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

/**
 * A client-side query that is rendered to JSON once and then written verbatim into every request
 * that carries it.
 *
 * <p>A lineage hop sends the same query, with its full URN terms list, on every page of every
 * slice. Rendering a {@code TermsQueryBuilder} converts each term back to a string and walks the
 * list again for self-references, so for large hops re-rendering the query per page is the main CPU
 * cost in GMS. Wrapping the hop's query once removes that repeated work; each request only copies
 * the bytes.
 *
 * <p>Only {@link #toXContent} and {@link #toString} are supported: this builder never runs inside
 * OpenSearch and cannot be modified.
 */
public final class PreRenderedQueryBuilder implements QueryBuilder {

  private final String name;
  private final String queryName;
  private final float boost;
  private final byte[] json;

  private PreRenderedQueryBuilder(String name, String queryName, float boost, byte[] json) {
    this.name = name;
    this.queryName = queryName;
    this.boost = boost;
    this.json = json;
  }

  /** Renders {@code query} to JSON now. */
  @Nonnull
  public static PreRenderedQueryBuilder of(@Nonnull QueryBuilder query) throws IOException {
    BytesReference rendered = XContentHelper.toXContent(query, XContentType.JSON, false);
    return new PreRenderedQueryBuilder(
        query.getName(), query.queryName(), query.boost(), BytesReference.toBytes(rendered));
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    // Copied straight into a JSON output; other content types re-parse the bytes.
    builder.rawValue(new ByteArrayInputStream(json), MediaTypeRegistry.JSON);
    return builder;
  }

  @Override
  public String toString() {
    return new String(json, StandardCharsets.UTF_8);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getWriteableName() {
    return name;
  }

  @Override
  public String queryName() {
    return queryName;
  }

  @Override
  public float boost() {
    return boost;
  }

  @Override
  public QueryBuilder queryName(String queryName) {
    throw new UnsupportedOperationException("PreRenderedQueryBuilder is immutable");
  }

  @Override
  public QueryBuilder boost(float boost) {
    throw new UnsupportedOperationException("PreRenderedQueryBuilder is immutable");
  }

  @Override
  public Query toQuery(QueryShardContext context) {
    throw new UnsupportedOperationException("PreRenderedQueryBuilder is client-side only");
  }

  @Override
  public void writeTo(StreamOutput out) {
    throw new UnsupportedOperationException("PreRenderedQueryBuilder is client-side only");
  }
}
