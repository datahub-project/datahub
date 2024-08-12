/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import datahub.spark.conf.SparkAppContext;
import datahub.spark.conf.SparkConfigParser;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.dataset.HdfsPathDataset;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Utility class to handle removing path patterns in dataset names. Given a configured regex pattern
 * with "remove" group defined, class methods run regex replacements on all the datasets available
 * within the event
 */
@Slf4j
public class RemovePathPatternUtils {
  public static final String REMOVE_PATTERN_GROUP = "remove";
  public static final String SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN =
      "spark.openlineage.dataset.removePath.pattern";

  private static Optional<SparkConf> sparkConf = Optional.empty();

  public static List<OutputDataset> removeOutputsPathPattern_ol(
      OpenLineageContext context, List<OutputDataset> outputs) {
    return getPattern(context)
        .map(
            pattern ->
                outputs.stream()
                    .map(
                        dataset -> {
                          String newName = removePath(pattern, dataset.getName());
                          if (newName != dataset.getName()) {
                            return context
                                .getOpenLineage()
                                .newOutputDatasetBuilder()
                                .name(removePath(pattern, dataset.getName()))
                                .namespace(dataset.getNamespace())
                                .facets(dataset.getFacets())
                                .outputFacets(dataset.getOutputFacets())
                                .build();
                          } else {
                            return dataset;
                          }
                        })
                    .collect(Collectors.toList()))
        .orElse(outputs);
  }

  // This method was replaced to support Datahub PathSpecs
  public static List<OutputDataset> removeOutputsPathPattern(
      OpenLineageContext context, List<OutputDataset> outputs) {
    return outputs.stream()
        .map(
            dataset -> {
              String newName = removePathPattern(dataset.getName());
              if (!Objects.equals(newName, dataset.getName())) {
                return context
                    .getOpenLineage()
                    .newOutputDatasetBuilder()
                    .name(newName)
                    .namespace(dataset.getNamespace())
                    .facets(dataset.getFacets())
                    .outputFacets(dataset.getOutputFacets())
                    .build();
              } else {
                return dataset;
              }
            })
        .collect(Collectors.toList());
  }

  // This method was replaced to support Datahub PathSpecs
  public static List<InputDataset> removeInputsPathPattern(
      OpenLineageContext context, List<InputDataset> inputs) {
    return inputs.stream()
        .map(
            dataset -> {
              String newName = removePathPattern(dataset.getName());
              if (!Objects.equals(newName, dataset.getName())) {
                return context
                    .getOpenLineage()
                    .newInputDatasetBuilder()
                    .name(newName)
                    .namespace(dataset.getNamespace())
                    .facets(dataset.getFacets())
                    .inputFacets(dataset.getInputFacets())
                    .build();
              } else {
                return dataset;
              }
            })
        .collect(Collectors.toList());
  }

  private static Optional<Pattern> getPattern(OpenLineageContext context) {
    return Optional.of(context.getSparkContext())
        .map(sparkContext -> sparkContext.get().conf())
        .filter(conf -> conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .map(conf -> conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .map(pattern -> Pattern.compile(pattern));
  }

  private static String removePath(Pattern pattern, String name) {
    return Optional.ofNullable(pattern.matcher(name))
        .filter(matcher -> matcher.find())
        .filter(
            matcher -> {
              try {
                matcher.group(REMOVE_PATTERN_GROUP);
                return true;
              } catch (IllegalStateException | IllegalArgumentException e) {
                return false;
              }
            })
        .filter(matcher -> StringUtils.isNotEmpty(matcher.group(REMOVE_PATTERN_GROUP)))
        .map(
            matcher ->
                name.substring(0, matcher.start(REMOVE_PATTERN_GROUP))
                    + name.substring(matcher.end(REMOVE_PATTERN_GROUP), name.length()))
        .orElse(name);
  }

  /**
   * SparkConf does not change through job lifetime but it can get lost once session is closed. It's
   * good to have it set in case of SPARK-29046
   */
  private static Optional<SparkConf> loadSparkConf() {
    if (!sparkConf.isPresent() && SparkSession.getDefaultSession().isDefined()) {
      sparkConf = Optional.of(SparkSession.getDefaultSession().get().sparkContext().getConf());
    }
    return sparkConf;
  }

  private static String removePathPattern(String datasetName) {
    // TODO: The reliance on global-mutable state here should be changed
    //  this led to problems in the PathUtilsTest class, where some tests interfered with others
    log.info("Removing path pattern from dataset name {}", datasetName);
    Optional<SparkConf> conf = loadSparkConf();
    if (!conf.isPresent()) {
      return datasetName;
    }
    try {
      String propertiesString =
          Arrays.stream(conf.get().getAllWithPrefix("spark.datahub."))
              .map(tup -> tup._1 + "= \"" + tup._2 + "\"")
              .collect(Collectors.joining("\n"));
      Config datahubConfig = ConfigFactory.parseString(propertiesString);
      DatahubOpenlineageConfig datahubOpenlineageConfig =
          SparkConfigParser.sparkConfigToDatahubOpenlineageConf(
              datahubConfig, new SparkAppContext());
      HdfsPathDataset hdfsPath =
          HdfsPathDataset.create(new URI(datasetName), datahubOpenlineageConfig);
      log.debug("Transformed path is {}", hdfsPath.getDatasetPath());
      return hdfsPath.getDatasetPath();
    } catch (InstantiationException e) {
      log.warn(
          "Unable to convert dataset {} to path the exception was {}", datasetName, e.getMessage());
      return datasetName;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
