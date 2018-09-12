/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.ingestion.utils;

import com.linkedin.events.metadata.ChangeAuditStamp;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.MetadataChangeEvent;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class ProcessorUtil {

  private ProcessorUtil() {
  }

  /**
   * Remove duplicate datasets in the list
   * @param datasets List of DatasetIdentifier
   * @return de-duped list
   */
  @Nonnull
  public static List<DatasetIdentifier> dedupeDatasets(@Nonnull List<DatasetIdentifier> datasets) {
    return datasets.stream().distinct().collect(Collectors.toList());
  }

  /**
   * Find the diff from existing list to the updated list, with exclusion patterns.
   * @param existing List<String>
   * @param updated List<String>
   * @param exclusions List<Pattern>
   * @return List<String>
   */
  public static List<String> listDiffWithExclusion(@Nonnull List<String> existing, @Nonnull List<String> updated,
      @Nonnull List<Pattern> exclusions) {
    existing.removeAll(updated);

    return existing.stream()
        .filter(s -> exclusions.stream().noneMatch(p -> p.matcher(s).find()))
        .collect(Collectors.toList());
  }

  /**
   * Extract whitelisted actors from the given config and configPath
   * @param config {@link Properties}
   * @param configPath Key for the white list config
   * @return A set of actor names or null if corresponding config doesn't exists
   */
  @Nullable
  public static Set<String> getWhitelistedActors(@Nonnull Properties config, @Nonnull String configPath) {
    String actors = config.getProperty(configPath);
    if (StringUtils.isEmpty(actors)) {
      return null;
    }

    return new HashSet<>(Arrays.asList(actors.split(";")));
  }

  /**
   * Create MCE to DELETE the dataset
   */
  public static MetadataChangeEvent mceDelete(@Nonnull DatasetIdentifier dataset, @Nonnull DeploymentDetail deployment,
      String actor) {
    MetadataChangeEvent mce = new MetadataChangeEvent();
    mce.datasetIdentifier = dataset;

    ChangeAuditStamp auditStamp = new ChangeAuditStamp();
    auditStamp.actorUrn = actor;
    auditStamp.time = System.currentTimeMillis();
    auditStamp.type = ChangeType.DELETE;
    mce.changeAuditStamp = auditStamp;

    mce.deploymentInfo = Collections.singletonList(deployment);

    return mce;
  }
}
