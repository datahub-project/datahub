package com.linkedin.metadata.entity.ebean;

import static org.testng.Assert.*;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class PartitionedStreamTest {

  @Test
  public void testFilterRetainsMatchingElements() {
    PartitionedStream<Integer> stream =
        PartitionedStream.<Integer>builder().delegateStream(Stream.of(1, 2, 3, 4, 5)).build();

    List<Integer> result;
    try (PartitionedStream<Integer> filtered = stream.filter(n -> n % 2 == 0)) {
      result = filtered.partition(10).flatMap(s -> s).collect(Collectors.toList());
    }

    assertEquals(result, List.of(2, 4));
  }

  @Test
  public void testFilterWithAlwaysFalsePredicateReturnsEmpty() {
    PartitionedStream<String> stream =
        PartitionedStream.<String>builder().delegateStream(Stream.of("a", "b", "c")).build();

    List<String> result;
    try (PartitionedStream<String> filtered = stream.filter(s -> false)) {
      result = filtered.partition(10).flatMap(s -> s).collect(Collectors.toList());
    }

    assertTrue(result.isEmpty());
  }

  @Test
  public void testFilterThenPartitionRespectsBatchSize() {
    PartitionedStream<Integer> stream =
        PartitionedStream.<Integer>builder().delegateStream(Stream.of(1, 2, 3, 4, 5, 6)).build();

    List<List<Integer>> batches;
    try (PartitionedStream<Integer> filtered = stream.filter(n -> n > 2)) {
      batches =
          filtered
              .partition(2)
              .map(s -> s.collect(Collectors.toList()))
              .collect(Collectors.toList());
    }

    // elements > 2: [3, 4, 5, 6] split into batches of 2
    assertEquals(batches.size(), 2);
    assertEquals(batches.get(0), List.of(3, 4));
    assertEquals(batches.get(1), List.of(5, 6));
  }

  @Test
  public void testPartitionOnEmptyStreamProducesNoBatches() {
    PartitionedStream<String> stream =
        PartitionedStream.<String>builder().delegateStream(Stream.empty()).build();

    long batchCount;
    try (PartitionedStream<String> s = stream) {
      batchCount = s.partition(5).count();
    }

    assertEquals(batchCount, 0L);
  }
}
