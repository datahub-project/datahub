package org.apache.spark.sql.delta.sources;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.Sink;

/**
 * Test double for Delta's optional runtime class. The production agent identifies this class by
 * name and reads its public {@link #path()} accessor without depending on Delta directly.
 */
public final class DeltaSink implements Sink {

  private final Path path;

  public DeltaSink(Path path) {
    this.path = path;
  }

  public Path path() {
    return path;
  }

  @Override
  public void addBatch(long batchId, Dataset<Row> data) {
    throw new UnsupportedOperationException();
  }
}
