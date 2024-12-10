/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.package$;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import scala.Tuple2;
import scala.collection.immutable.Seq;

/** Utility class to extract paths from RDD nodes. */
@Slf4j
public class RddPathUtils {

  public static Stream<Path> findRDDPaths(RDD rdd) {
    return Stream.<RddPathExtractor>of(
            new HadoopRDDExtractor(),
            new FileScanRDDExtractor(),
            new MapPartitionsRDDExtractor(),
            new ParallelCollectionRDDExtractor())
        .filter(e -> e.isDefinedAt(rdd))
        .findFirst()
        .orElse(new UnknownRDDExtractor())
        .extract(rdd)
        .filter(p -> p != null);
  }

  static class UnknownRDDExtractor implements RddPathExtractor<RDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return true;
    }

    @Override
    public Stream<Path> extract(RDD rdd) {
      // Change to debug to silence error
      log.debug("Unknown RDD class {}", rdd);
      return Stream.empty();
    }
  }

  static class HadoopRDDExtractor implements RddPathExtractor<HadoopRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof HadoopRDD;
    }

    @Override
    public Stream<Path> extract(HadoopRDD rdd) {
      org.apache.hadoop.fs.Path[] inputPaths = FileInputFormat.getInputPaths(rdd.getJobConf());
      Configuration hadoopConf = rdd.getConf();
      return Arrays.stream(inputPaths).map(p -> PlanUtils.getDirectoryPath(p, hadoopConf));
    }
  }

  static class MapPartitionsRDDExtractor implements RddPathExtractor<MapPartitionsRDD> {

    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof MapPartitionsRDD;
    }

    @Override
    public Stream<Path> extract(MapPartitionsRDD rdd) {
      return findRDDPaths(rdd.prev());
    }
  }

  static class FileScanRDDExtractor implements RddPathExtractor<FileScanRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof FileScanRDD;
    }

    @Override
    @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
    public Stream<Path> extract(FileScanRDD rdd) {
      return ScalaConversionUtils.fromSeq(rdd.filePartitions()).stream()
          .flatMap((FilePartition fp) -> Arrays.stream(fp.files()))
          .map(
              f -> {
                if ("3.4".compareTo(package$.MODULE$.SPARK_VERSION()) <= 0) {
                  // filePath returns SparkPath for Spark 3.4
                  return ReflectionUtils.tryExecuteMethod(f, "filePath")
                      .map(o -> ReflectionUtils.tryExecuteMethod(o, "toPath"))
                      .map(o -> (Path) o.get())
                      .get()
                      .getParent();
                } else {
                  return parentOf(f.filePath());
                }
              });
    }
  }

  static class ParallelCollectionRDDExtractor implements RddPathExtractor<ParallelCollectionRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof ParallelCollectionRDD;
    }

    @Override
    public Stream<Path> extract(ParallelCollectionRDD rdd) {
      int SEQ_LIMIT = 1000;
      AtomicBoolean loggingDone = new AtomicBoolean(false);
      try {
        Object data = FieldUtils.readField(rdd, "data", true);
        log.debug("ParallelCollectionRDD data: {}", data);
        if ((data instanceof Seq) && ((Seq) data).head() instanceof Tuple2) {
          // exit if the first element is invalid
          Seq data_slice = (Seq) ((Seq) data).slice(0, SEQ_LIMIT);
          return ScalaConversionUtils.fromSeq(data_slice).stream()
              .map(
                  el -> {
                    Path path = null;
                    if (el instanceof Tuple2) {
                      // we're able to extract path
                      path = parentOf(((Tuple2) el)._1.toString());
                      log.debug("Found input {}", path);
                    } else if (!loggingDone.get()) {
                      log.warn("unable to extract Path from {}", el.getClass().getCanonicalName());
                      loggingDone.set(true);
                    }
                    return path;
                  })
              .filter(Objects::nonNull);
        } else {
          // Changed to debug to silence error
          log.debug("Cannot extract path from ParallelCollectionRDD {}", data);
        }
      } catch (IllegalAccessException | IllegalArgumentException e) {
        // Changed to debug to silence error
        log.debug("Cannot read data field from ParallelCollectionRDD {}", rdd);
      }
      return Stream.empty();
    }
  }

  private static Path parentOf(String path) {
    try {
      return new Path(path).getParent();
    } catch (Exception e) {
      return null;
    }
  }

  interface RddPathExtractor<T extends RDD> {
    boolean isDefinedAt(Object rdd);

    Stream<Path> extract(T rdd);
  }
}
