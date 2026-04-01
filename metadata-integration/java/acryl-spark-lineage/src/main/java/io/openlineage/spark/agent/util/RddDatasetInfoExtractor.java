/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod;

import io.openlineage.client.utils.DatasetIdentifier;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.package$;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.NewHadoopRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.rdd.UnionRDD;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDD;
import org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.collection.mutable.ArrayBuffer;

/**
 * Utility class to extract dataset information from RDD nodes.
 *
 * <p>Uses a chain of specialized extractors to handle different RDD implementations.
 */
@Slf4j
public class RddDatasetInfoExtractor {

  public static Stream<DatasetIdentifier> findDatasetIdentifiers(RDD<?> rdd) {
    return Stream.<RddDatasetIdentifierExtractor>of(
            new HadoopRDDExtractor(),
            new FileScanRDDExtractor(),
            new MapPartitionsRDDExtractor(),
            new UnionRddExctractor(),
            new NewHadoopRDDExtractor(),
            new ParallelCollectionRDDExtractor(),
            new DataSourceRDDExtractor())
        .filter(e -> e.isDefinedAt(rdd))
        .findFirst()
        .orElse(new UnknownRDDExtractor())
        .extract(rdd)
        .filter(p -> p != null);
  }

  public static Optional<StructType> findSchema(RDD<?> rdd) {
    if (rdd instanceof DataSourceRDD) {
      return new DataSourceRDDExtractor().extractSchema((DataSourceRDD) rdd);
    }
    return Optional.empty();
  }

  static class UnionRddExctractor implements RddDatasetIdentifierExtractor<UnionRDD<?>> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof UnionRDD;
    }

    @Override
    public Stream<DatasetIdentifier> extract(UnionRDD<?> rdd) {
      return ScalaConversionUtils.fromSeq(rdd.rdds()).stream()
          .flatMap(RddDatasetInfoExtractor::findDatasetIdentifiers);
    }
  }

  static class UnknownRDDExtractor implements RddDatasetIdentifierExtractor<RDD<?>> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return true;
    }

    @Override
    public Stream<DatasetIdentifier> extract(RDD<?> rdd) {
      log.debug("Unknown RDD class {}", rdd);
      return Stream.empty();
    }
  }

  static class HadoopRDDExtractor implements RddDatasetIdentifierExtractor<HadoopRDD<?, ?>> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof HadoopRDD;
    }

    @Override
    public Stream<DatasetIdentifier> extract(HadoopRDD<?, ?> rdd) {
      org.apache.hadoop.fs.Path[] inputPaths = FileInputFormat.getInputPaths(rdd.getJobConf());
      Configuration hadoopConf = rdd.getConf();
      if (log.isDebugEnabled()) {
        log.debug("Hadoop RDD class {}", rdd.getClass());
        log.debug("Hadoop RDD input paths {}", Arrays.toString(inputPaths));
        log.debug("Hadoop RDD job conf {}", rdd.getJobConf());
      }
      return PlanUtils.getDirectoryPaths(Arrays.asList(inputPaths), hadoopConf).stream()
          .map(PathUtils::fromPath);
    }
  }

  static class NewHadoopRDDExtractor implements RddDatasetIdentifierExtractor<NewHadoopRDD<?, ?>> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof NewHadoopRDD;
    }

    @Override
    public Stream<DatasetIdentifier> extract(NewHadoopRDD<?, ?> rdd) {
      try {
        org.apache.hadoop.fs.Path[] inputPaths =
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.getInputPaths(
                new Job(rdd.getConf()));

        return PlanUtils.getDirectoryPaths(Arrays.asList(inputPaths), rdd.getConf()).stream()
            .map(PathUtils::fromPath);
      } catch (IOException e) {
        log.error("Openlineage spark agent could not get input paths", e);
      }
      return Stream.empty();
    }
  }

  static class MapPartitionsRDDExtractor
      implements RddDatasetIdentifierExtractor<MapPartitionsRDD<?, ?>> {

    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof MapPartitionsRDD;
    }

    @Override
    public Stream<DatasetIdentifier> extract(MapPartitionsRDD<?, ?> rdd) {
      if (log.isDebugEnabled()) {
        log.debug("Parent RDD: {}", rdd.prev());
      }
      return findDatasetIdentifiers(rdd.prev());
    }
  }

  static class FileScanRDDExtractor implements RddDatasetIdentifierExtractor<FileScanRDD> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof FileScanRDD;
    }

    @Override
    @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
    public Stream<DatasetIdentifier> extract(FileScanRDD rdd) {
      return ScalaConversionUtils.fromSeq(rdd.filePartitions()).stream()
          .flatMap((FilePartition fp) -> Arrays.stream(fp.files()))
          .map(
              f -> {
                if ("3.4".compareTo(package$.MODULE$.SPARK_VERSION()) <= 0) {
                  // filePath returns SparkPath for Spark 3.4
                  return tryExecuteMethod(f, "filePath")
                      .map(o -> tryExecuteMethod(o, "toPath"))
                      .map(o -> (Path) o.get())
                      .get()
                      .getParent();
                } else {
                  return parentOf(f.filePath());
                }
              })
          .filter(Objects::nonNull)
          .map(PathUtils::fromPath);
    }
  }

  static class ParallelCollectionRDDExtractor
      implements RddDatasetIdentifierExtractor<ParallelCollectionRDD<?>> {
    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof ParallelCollectionRDD;
    }

    @Override
    public Stream<DatasetIdentifier> extract(ParallelCollectionRDD<?> rdd) {
      int SEQ_LIMIT = 1000;
      AtomicBoolean loggingDone = new AtomicBoolean(false);
      try {
        Object data = FieldUtils.readField(rdd, "data", true);
        log.debug("ParallelCollectionRDD data: {}", data);
        if ((data instanceof Seq)
            && (!((Seq<?>) data).isEmpty())
            && ((Seq) data).head() instanceof Tuple2) {
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
                    return PathUtils.fromPath(path);
                  })
              .filter(Objects::nonNull);
        } else if ((data instanceof ArrayBuffer) && !((ArrayBuffer<?>) data).isEmpty()) {
          ArrayBuffer<?> dataBuffer = (ArrayBuffer<?>) data;
          return ScalaConversionUtils.fromSeq(dataBuffer.toSeq()).stream()
              .map(o -> parentOf(o.toString()))
              .filter(Objects::nonNull)
              .map(PathUtils::fromPath);
        } else {
          log.debug("Cannot extract path from ParallelCollectionRDD {}", data);
        }
      } catch (IllegalAccessException | IllegalArgumentException e) {
        log.debug("Cannot read data field from ParallelCollectionRDD {}", rdd);
      }
      return Stream.empty();
    }
  }

  public static class DataSourceRDDExtractor
      implements RddDatasetIdentifierExtractor<DataSourceRDD> {
    private final List<InputPartitionExtractor> inputPartitionExtractors;

    DataSourceRDDExtractor() {
      this(new InputPartitionExtractorFactory());
    }

    public DataSourceRDDExtractor(InputPartitionExtractorFactory inputPartitionExtractorFactory) {
      this.inputPartitionExtractors =
          inputPartitionExtractorFactory.createInputPartitionExtractors();
    }

    @Override
    public boolean isDefinedAt(Object rdd) {
      return rdd instanceof DataSourceRDD;
    }

    @Override
    public Stream<DatasetIdentifier> extract(DataSourceRDD rdd) {
      return extractInputPartitions(rdd).stream()
          .flatMap(
              ip ->
                  inputPartitionExtractors.stream()
                      .filter(e -> e.isDefinedAt(ip))
                      .flatMap(e -> e.extract(rdd.sparkContext(), ip).stream()));
    }

    public Optional<StructType> extractSchema(DataSourceRDD rdd) {
      return extractInputPartitions(rdd).stream()
          .flatMap(
              ip ->
                  inputPartitionExtractors.stream()
                      .filter(e -> e.isDefinedAt(ip))
                      .map(e -> e.extractSchema(rdd.sparkContext(), ip))
                      .filter(Optional::isPresent)
                      .map(Optional::get))
          .findFirst();
    }

    @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
    private List<InputPartition> extractInputPartitions(DataSourceRDD rdd) {
      List<InputPartition> inputPartitions;
      if ("3.3".compareTo(package$.MODULE$.SPARK_VERSION()) <= 0) {
        inputPartitions =
            Arrays.stream(rdd.getPartitions())
                .filter(p -> p instanceof DataSourceRDDPartition)
                .map(p -> tryExecuteMethod(p, "inputPartitions").orElse(null))
                .filter(Objects::nonNull)
                .flatMap(seq -> ScalaConversionUtils.fromSeq((Seq<?>) seq).stream())
                .filter(ip -> ip instanceof InputPartition)
                .map(ip -> (InputPartition) ip)
                .collect(Collectors.toList());
      } else {
        inputPartitions =
            Arrays.stream(rdd.getPartitions())
                .filter(p -> p instanceof DataSourceRDDPartition)
                .map(p -> ((DataSourceRDDPartition) p).inputPartition())
                .collect(Collectors.toList());
      }
      return inputPartitions;
    }
  }

  public static class InputPartitionExtractorFactory {
    private static final String ICEBERG_INPUT_PARTITION_EXTRACTOR =
        "io.openlineage.spark.agent.vendor.iceberg.util.SparkInputPartitionExtractor";

    public List<InputPartitionExtractor> createInputPartitionExtractors() {
      List<InputPartitionExtractor> inputPartitionExtractors = new ArrayList<>();
      getIcebergInputPartitionExtractor().ifPresent(inputPartitionExtractors::add);
      return inputPartitionExtractors;
    }

    private Optional<InputPartitionExtractor> getIcebergInputPartitionExtractor() {
      try {
        Class<?> clazz = Class.forName(ICEBERG_INPUT_PARTITION_EXTRACTOR);

        if (InputPartitionExtractor.class.isAssignableFrom(clazz)) {
          InputPartitionExtractor icebergInputPartitionExtractor =
              (InputPartitionExtractor) clazz.getDeclaredConstructor().newInstance();
          if (log.isDebugEnabled()) {
            log.debug("Successfully created an instance of {}", ICEBERG_INPUT_PARTITION_EXTRACTOR);
          }
          return Optional.of(icebergInputPartitionExtractor);
        } else {
          if (log.isDebugEnabled()) {
            log.debug(
                "Class {} is not assignable from InputPartitionExtractor.",
                ICEBERG_INPUT_PARTITION_EXTRACTOR);
          }
        }
      } catch (ClassNotFoundException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException
          | NoClassDefFoundError e) {
        if (log.isDebugEnabled()) {
          log.debug(
              "{} is not on classpath: {}", ICEBERG_INPUT_PARTITION_EXTRACTOR, e.getMessage());
        }
      }
      return Optional.empty();
    }
  }

  private static Path parentOf(String path) {
    try {
      return new Path(path).getParent();
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.debug("Cannot get parent of path {}", path, e);
      }
      return null;
    }
  }

  interface RddDatasetIdentifierExtractor<T extends RDD<?>> {
    boolean isDefinedAt(Object rdd);

    Stream<DatasetIdentifier> extract(T rdd);
  }
}
