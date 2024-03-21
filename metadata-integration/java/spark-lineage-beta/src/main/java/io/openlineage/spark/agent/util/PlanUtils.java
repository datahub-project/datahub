/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.spark.conf.SparkLineageConf;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.package$;
import org.apache.spark.rdd.HadoopRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.datasources.FileScanRDD;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class PlanUtils {

  public static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
      "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
  public static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
      "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";

  /**
   * Merge a list of {@link PartialFunction}s and return the first value where the function is
   * defined or empty list if no function matches the input.
   *
   * @param fns
   * @param arg
   * @param <T>
   * @param <R>
   * @return
   */
  public static <T, R> Collection<R> applyAll(
      List<? extends PartialFunction<T, ? extends Collection<R>>> fns, T arg) {
    PartialFunction<T, Collection<R>> fn = merge(fns);
    if (fn.isDefinedAt(arg)) {
      return fn.apply(arg);
    }
    return Collections.emptyList();
  }

  /**
   * Given a list of {@link PartialFunction}s merge to produce a single function that will test the
   * input against each function one by one until a match is found or empty() is returned.
   *
   * @param fns
   * @param <T>
   * @param <D>
   * @return
   */
  public static <T, D> PartialFunction<T, Collection<D>> merge(
      Collection<? extends PartialFunction<T, ? extends Collection<D>>> fns) {
    return new AbstractPartialFunction<T, Collection<D>>() {
      @Override
      public boolean isDefinedAt(T x) {
        return fns.stream()
            .filter(pfn -> PlanUtils.safeIsDefinedAt(pfn, x))
            .findFirst()
            .isPresent();
      }

      private boolean isDefinedAt(T x, PartialFunction<T, ? extends Collection<D>> pfn) {
        return PlanUtils.safeIsDefinedAt(pfn, x);
      }

      @Override
      public Collection<D> apply(T x) {
        return fns.stream()
            .filter(pfn -> PlanUtils.safeIsDefinedAt(pfn, x))
            .map(
                pfn -> {
                  try {
                    Collection<D> collection = pfn.apply(x);
                    if (log.isDebugEnabled()) {
                      log.debug(
                          "Visitor {} visited {}, returned {}",
                          pfn.getClass().getCanonicalName(),
                          x.getClass().getCanonicalName(),
                          collection);
                    }
                    return collection;
                  } catch (RuntimeException | NoClassDefFoundError | NoSuchMethodError e) {
                    log.error("Apply failed:", e);
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
      }
    };
  }

  /**
   * Given a schema, construct a valid {@link OpenLineage.SchemaDatasetFacet}.
   *
   * @param structType
   * @return
   */
  public static OpenLineage.SchemaDatasetFacet schemaFacet(
      OpenLineage openLineage, StructType structType) {
    return openLineage
        .newSchemaDatasetFacetBuilder()
        .fields(transformFields(openLineage, structType.fields()))
        .build();
  }

  private static List<OpenLineage.SchemaDatasetFacetFields> transformFields(
      OpenLineage openLineage, StructField... fields) {
    List<OpenLineage.SchemaDatasetFacetFields> list = new ArrayList<>();
    for (StructField field : fields) {
      list.add(
          openLineage
              .newSchemaDatasetFacetFieldsBuilder()
              .name(field.name())
              .type(field.dataType().typeName())
              .build());
    }
    return list;
  }

  /**
   * Given a list of attributes, constructs a valid {@link OpenLineage.SchemaDatasetFacet}.
   *
   * @param attributes
   * @return
   */
  public static StructType toStructType(List<Attribute> attributes) {
    return new StructType(
        attributes.stream()
            .map(
                attr ->
                    new StructField(attr.name(), attr.dataType(), attr.nullable(), attr.metadata()))
            .collect(Collectors.toList())
            .toArray(new StructField[0]));
  }

  public static String namespaceUri(URI outputPath) {
    return Optional.ofNullable(outputPath.getAuthority())
        .map(a -> String.format("%s://%s", outputPath.getScheme(), a))
        .orElse(outputPath.getScheme());
  }

  /**
   * Construct a {@link OpenLineage.DatasourceDatasetFacet} given a namespace for the datasource.
   *
   * @param namespaceUri
   * @return
   */
  public static OpenLineage.DatasourceDatasetFacet datasourceFacet(
      OpenLineage openLineage, String namespaceUri) {
    return openLineage
        .newDatasourceDatasetFacetBuilder()
        .uri(URI.create(namespaceUri))
        .name(namespaceUri)
        .build();
  }

  /**
   * Construct a {@link OpenLineage.ParentRunFacet} given the parent job's parentRunId, job name,
   * and namespace.
   *
   * @param parentRunId
   * @param parentJob
   * @param parentJobNamespace
   * @return
   */
  public static OpenLineage.ParentRunFacet parentRunFacet(
      UUID parentRunId, String parentJob, String parentJobNamespace) {
    return new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI)
        .newParentRunFacetBuilder()
        .run(new OpenLineage.ParentRunFacetRunBuilder().runId(parentRunId).build())
        .job(
            new OpenLineage.ParentRunFacetJobBuilder()
                .name(parentJob)
                .namespace(parentJobNamespace)
                .build())
        .build();
  }

  public static Path getDirectoryPath(Path p, Configuration hadoopConf) {
    SparkConf conf = SparkEnv.get().conf();
    String propertiesString =
        Arrays.stream(conf.getAllWithPrefix("spark.datahub."))
            .map(tup -> tup._1 + "= \"" + tup._2 + "\"")
            .collect(Collectors.joining("\n"));
    Config datahubConfig = ConfigFactory.parseString(propertiesString);
    SparkLineageConf sparkLineageConf =
        SparkLineageConf.toSparkLineageConf(datahubConfig, null, null);
    HdfsPathDataset hdfsPath = null;
    try {
      URI uri = new URI(p.toString());
      hdfsPath = HdfsPathDataset.create(uri, sparkLineageConf.getOpenLineageConf());
      log.debug("Path {} transformed to {}", p, hdfsPath.getDatasetPath());
      return new Path(hdfsPath.getDatasetPath());
    } catch (InstantiationException | URISyntaxException e) {
      log.warn("Unable to convert path to hdfs path {} the exception was {}", p, e.getMessage());
      return p;
    }

    // try {
    // if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {
    // return p.getParent();
    // } else {
    // return p;
    // }
    // } catch (IOException e) {
    // log.warn("Unable to get file system for path ", e);
    // return p;
    // }
  }

  /**
   * Given a list of RDDs, it collects list of data location directories. For each RDD, a parent
   * directory is taken and list of distinct locations is returned.
   *
   * @param fileRdds
   * @return
   */
  public static List<Path> findRDDPaths(List<RDD<?>> fileRdds) {
    return fileRdds.stream()
        .flatMap(
            rdd -> {
              if (rdd instanceof HadoopRDD) {
                HadoopRDD hadoopRDD = (HadoopRDD) rdd;
                Path[] inputPaths = FileInputFormat.getInputPaths(hadoopRDD.getJobConf());
                Configuration hadoopConf = hadoopRDD.getConf();
                return Arrays.stream(inputPaths)
                    .map(p -> PlanUtils.getDirectoryPath(p, hadoopConf));
              } else if (rdd instanceof FileScanRDD) {
                FileScanRDD fileScanRDD = (FileScanRDD) rdd;
                return ScalaConversionUtils.fromSeq(fileScanRDD.filePartitions()).stream()
                    .flatMap(fp -> Arrays.stream(fp.files()))
                    .map(
                        f -> {
                          if (package$.MODULE$.SPARK_VERSION().compareTo("3.4") > 0) {
                            // filePath returns SparkPath for Spark 3.4
                            return ReflectionUtils.tryExecuteMethod(f, "filePath")
                                .map(o -> ReflectionUtils.tryExecuteMethod(o, "toPath"))
                                .map(o -> (Path) o.get())
                                .get()
                                .getParent();
                          } else {
                            return new Path(f.filePath()).getParent();
                          }
                        });
              } else {
                log.warn("Unknown RDD class {}", rdd.getClass().getCanonicalName());
                return Stream.empty();
              }
            })
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * instanceOf alike implementation which does not fail in case of a missing class.
   *
   * @param instance
   * @param classCanonicalName
   * @return
   */
  public static boolean safeIsInstanceOf(Object instance, String classCanonicalName) {
    try {
      Class c = Class.forName(classCanonicalName);
      return instance.getClass().isAssignableFrom(c);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * isDefinedAt method implementation that should never throw an error or exception
   *
   * @param pfn
   * @param x
   * @return
   */
  public static boolean safeIsDefinedAt(PartialFunction pfn, Object x) {
    try {
      return pfn.isDefinedAt(x);
    } catch (ClassCastException e) {
      // do nothing
      return false;
    } catch (Exception e) {
      if (e != null) {
        log.debug("isDefinedAt method failed on {}", e);
      }
      return false;
    } catch (NoClassDefFoundError e) {
      log.debug("isDefinedAt method failed on {}", e.getMessage());
      return false;
    }
  }

  /**
   * apply method implementation that should never throw an error or exception
   *
   * @param pfn
   * @param x
   * @return
   */
  public static <T, D> List<T> safeApply(PartialFunction<D, List<T>> pfn, D x) {
    try {
      return pfn.apply(x);
    } catch (Exception | NoClassDefFoundError | NoSuchMethodError e) {
      log.info("apply method failed with", e);
      return Collections.emptyList();
    }
  }
}
