/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity.ebean;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.experimental.Accessors;

@Builder
@Accessors(fluent = true)
public class PartitionedStream<T> implements AutoCloseable {
  @Nonnull private final Stream<T> delegateStream;

  public Stream<Stream<T>> partition(int size) {
    final Iterator<T> it = delegateStream.iterator();
    final Iterator<Stream<T>> partIt =
        Iterators.transform(Iterators.partition(it, size), List::stream);
    final Iterable<Stream<T>> iterable = () -> partIt;
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  @Override
  public void close() {
    delegateStream.close();
  }
}
