package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.search.SearchComponent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Splits a family's source indices into copy partitions. Each family has a natural first axis (a
 * keyword with modest cardinality, or the index itself); any piece still larger than {@code
 * targetDocsPerPartition} is split again by even ranges of a time field, plus one bucket for
 * documents without that field. Usage events are partitioned per backing index into staging
 * indices, with every generation that received writes since the mirror started sharing one live
 * staging index; a concrete usage index is the write index itself and copies there too.
 */
@Slf4j
public final class PartitionPlanner {
  static final int MAX_TERMS = 10_000;
  static final int MAX_RANGE_SPLITS = 64;

  private final ClusterHttp source;
  private final long targetDocsPerPartition;

  public PartitionPlanner(@Nonnull final ClusterHttp source, final long targetDocsPerPartition) {
    this.source = source;
    this.targetDocsPerPartition = Math.max(1, targetDocsPerPartition);
  }

  /**
   * @param indexSets the family's logical indices on the source, each resolved to its backing
   *     indices
   * @param targetName maps a source backing index (or logical name) onto its target name
   * @param liveStagingIndex for usage events, the staging index the mirror writes to; the source
   *     write index and anything newer copy there, so {@code op_type=create} deduplicates the
   *     mirrored and the copied event exactly
   */
  @Nonnull
  public List<CopyPartition> plan(
      @Nonnull final SearchComponent family,
      @Nonnull final List<SourceIndexSet> indexSets,
      @Nonnull final UnaryOperator<String> targetName,
      @Nullable final String liveStagingIndex)
      throws IOException {
    final List<CopyPartition> partitions = new ArrayList<>();
    for (SourceIndexSet indexSet : indexSets) {
      if (indexSet.isRollover()) {
        planRollover(indexSet, targetName, liveStagingIndex, partitions);
        continue;
      }
      final String logical = indexSet.logicalName();
      if (family == SearchComponent.USAGE) {
        final String liveStaging = requireLiveStaging(liveStagingIndex, indexSet);
        planByRange(
            logical, liveStaging, null, null, partitions, logical, source.count(logical, null));
        continue;
      }
      final String target = targetName.apply(logical);
      switch (family) {
        case GRAPH:
          planByTerms(logical, target, "relationshipType", "createdOn", partitions);
          break;
        case SYSTEM_METADATA:
          planByTerms(logical, target, "aspect", "lastUpdated", partitions);
          break;
        case TIMESERIES:
          planByRange(
              logical,
              target,
              null,
              "@timestamp",
              partitions,
              logical,
              source.count(logical, null));
          break;
        default:
          planByRange(
              logical, target, null, null, partitions, logical, source.count(logical, null));
          break;
      }
    }
    return partitions;
  }

  private void planRollover(
      @Nonnull final SourceIndexSet indexSet,
      @Nonnull final UnaryOperator<String> targetName,
      @Nullable final String liveStagingIndex,
      @Nonnull final List<CopyPartition> partitions)
      throws IOException {
    final String liveStaging = requireLiveStaging(liveStagingIndex, indexSet);
    boolean reachedWriteIndex = false;
    for (String backing : indexSet.backingIndices()) {
      if (backing.equals(indexSet.writeIndex())) {
        reachedWriteIndex = true;
      }
      // The write index at mirror start and every later generation are mirrored into the live
      // staging index; copying them there too lets op_type=create deduplicate exactly.
      final String target = reachedWriteIndex ? liveStaging : targetName.apply(backing);
      final long count = source.count(backing, null);
      partitions.add(
          new CopyPartition(partitions.size(), backing, target, backing, null, count, null));
    }
  }

  @Nonnull
  private static String requireLiveStaging(
      @Nullable final String liveStagingIndex, @Nonnull final SourceIndexSet indexSet) {
    if (liveStagingIndex == null) {
      throw new IllegalArgumentException(
          "Usage events need a live staging index to copy into: " + indexSet.logicalName());
    }
    return liveStagingIndex;
  }

  private void planByTerms(
      @Nonnull final String sourceIndex,
      @Nonnull final String targetIndex,
      @Nonnull final String termsField,
      @Nonnull final String rangeField,
      @Nonnull final List<CopyPartition> partitions)
      throws IOException {
    final ObjectNode body = source.object();
    body.put("size", 0);
    body.putObject("aggs")
        .putObject("terms")
        .putObject("terms")
        .put("field", termsField)
        .put("size", MAX_TERMS);
    final JsonNode response = source.json("POST", "/" + sourceIndex + "/_search", body);
    final JsonNode terms = response.path("aggregations").path("terms");
    long covered = 0;
    for (JsonNode bucket : terms.path("buckets")) {
      final String term = bucket.path("key").asText();
      final long count = bucket.path("doc_count").asLong();
      covered += count;
      final ObjectNode filter = termFilter(termsField, term);
      planByRange(sourceIndex, targetIndex, filter, rangeField, partitions, term, count);
    }
    if (terms.path("sum_other_doc_count").asLong() > 0) {
      throw new IOException(
          "More than "
              + MAX_TERMS
              + " distinct values of "
              + termsField
              + " in "
              + sourceIndex
              + "; the partition plan would miss "
              + terms.path("sum_other_doc_count").asLong()
              + " documents");
    }
    // Documents without the term field are not in any bucket.
    final ObjectNode missing = missingFilter(termsField);
    final long missingCount = source.count(sourceIndex, missing);
    if (missingCount > 0) {
      planByRange(
          sourceIndex,
          targetIndex,
          missing,
          rangeField,
          partitions,
          "missing " + termsField,
          missingCount);
    }
    log.info(
        "Planned {} by {}: {} documents in buckets, {} without the field",
        sourceIndex,
        termsField,
        covered,
        missingCount);
  }

  /**
   * One partition for {@code count} documents, or several split by even ranges of {@code
   * rangeField} once that is more than the target per partition; {@code label} names the piece.
   */
  private void planByRange(
      @Nonnull final String sourceIndex,
      @Nonnull final String targetIndex,
      @Nullable final ObjectNode baseFilter,
      @Nullable final String rangeField,
      @Nonnull final List<CopyPartition> partitions,
      @Nonnull final String label,
      final long count)
      throws IOException {
    if (rangeField == null || count <= targetDocsPerPartition) {
      partitions.add(
          new CopyPartition(
              partitions.size(), sourceIndex, targetIndex, label, baseFilter, count, null));
      return;
    }
    final ObjectNode statsBody = source.object();
    statsBody.put("size", 0);
    if (baseFilter != null) {
      statsBody.set("query", baseFilter);
    }
    final ObjectNode aggs = statsBody.putObject("aggs");
    aggs.putObject("min").putObject("min").put("field", rangeField);
    aggs.putObject("max").putObject("max").put("field", rangeField);
    final JsonNode stats =
        source.json("POST", "/" + sourceIndex + "/_search", statsBody).path("aggregations");
    final JsonNode minNode = stats.path("min").path("value");
    final JsonNode maxNode = stats.path("max").path("value");
    final ObjectNode withoutField = and(baseFilter, missingFilter(rangeField));
    final long withoutFieldCount = source.count(sourceIndex, withoutField);
    if (minNode.isNull() || maxNode.isNull() || minNode.isMissingNode()) {
      // Nothing carries the field: one partition for everything.
      partitions.add(
          new CopyPartition(
              partitions.size(), sourceIndex, targetIndex, label, baseFilter, count, null));
      return;
    }
    final long min = (long) minNode.asDouble();
    final long max = (long) maxNode.asDouble();
    final long withField = count - withoutFieldCount;
    final int splits =
        (int)
            Math.min(
                MAX_RANGE_SPLITS,
                Math.max(2, (withField + targetDocsPerPartition - 1) / targetDocsPerPartition));
    final long width = Math.max(1, (max - min) / splits + 1);
    for (int i = 0; i < splits; i++) {
      final long from = min + i * width;
      final Long to = i == splits - 1 ? null : from + width;
      if (from > max) {
        break;
      }
      final ObjectNode range = and(baseFilter, rangeFilter(rangeField, from, to));
      final long rangeCount = source.count(sourceIndex, range);
      if (rangeCount == 0) {
        continue;
      }
      partitions.add(
          new CopyPartition(
              partitions.size(),
              sourceIndex,
              targetIndex,
              label + " " + rangeField + "[" + from + "," + (to == null ? "max" : to) + ")",
              range,
              rangeCount,
              null));
    }
    if (withoutFieldCount > 0) {
      partitions.add(
          new CopyPartition(
              partitions.size(),
              sourceIndex,
              targetIndex,
              label + " missing " + rangeField,
              withoutField,
              withoutFieldCount,
              null));
    }
  }

  @Nonnull
  private ObjectNode termFilter(@Nonnull final String field, @Nonnull final String value) {
    final ObjectNode filter = source.object();
    filter.putObject("term").put(field, value);
    return filter;
  }

  @Nonnull
  private ObjectNode missingFilter(@Nonnull final String field) {
    final ObjectNode filter = source.object();
    filter.putObject("bool").putObject("must_not").putObject("exists").put("field", field);
    return filter;
  }

  @Nonnull
  private ObjectNode rangeFilter(
      @Nonnull final String field, final long fromInclusive, @Nullable final Long toExclusive) {
    final ObjectNode filter = source.object();
    final ObjectNode range = filter.putObject("range").putObject(field);
    range.put("gte", fromInclusive);
    if (toExclusive != null) {
      range.put("lt", toExclusive);
    }
    return filter;
  }

  @Nonnull
  private ObjectNode and(@Nullable final ObjectNode left, @Nonnull final ObjectNode right) {
    if (left == null) {
      return right;
    }
    final ObjectNode filter = source.object();
    final ArrayNode must = filter.putObject("bool").putArray("filter");
    must.add(left);
    must.add(right);
    return filter;
  }
}
