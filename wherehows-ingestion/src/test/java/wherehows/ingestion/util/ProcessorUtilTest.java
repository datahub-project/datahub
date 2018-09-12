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
package wherehows.ingestion.util;

import com.google.common.collect.ImmutableSet;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.events.metadata.DataOrigin;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.DeploymentDetail;
import com.linkedin.events.metadata.MetadataChangeEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.testng.annotations.Test;
import wherehows.ingestion.utils.ProcessorUtil;

import static org.testng.Assert.*;


public class ProcessorUtilTest {

  @Test
  public void testDedupeDatasets() {
    DatasetIdentifier ds1 = makeDataset("test1");
    DatasetIdentifier ds2 = makeDataset("test2");
    DatasetIdentifier ds3 = makeDataset("test1");
    List<DatasetIdentifier> datasets = Arrays.asList(ds1, ds2, ds3);

    List<DatasetIdentifier> deduped = ProcessorUtil.dedupeDatasets(datasets);
    assertEquals(deduped, Arrays.asList(ds1, ds2));
  }

  @Test
  public void testListDiffWithExclusion() {
    List<String> existing = new ArrayList<>(Arrays.asList("a", "b", "c"));
    List<String> updated = new ArrayList<>(Arrays.asList("b", "d"));

    assertEquals(ProcessorUtil.listDiffWithExclusion(existing, updated, Collections.emptyList()),
        Arrays.asList("a", "c"));

    List<Pattern> exclusions = Collections.singletonList(Pattern.compile("a"));

    assertEquals(ProcessorUtil.listDiffWithExclusion(existing, updated, exclusions), Arrays.asList("c"));
  }

  @Test
  public void testGetWhitelistedActors() {
    Properties config = new Properties();
    config.put("whitelist", "foo;bar");

    Set<String> actors = ProcessorUtil.getWhitelistedActors(config, "whitelist");

    assertEquals(actors, ImmutableSet.of("foo", "bar"));
  }

  @Test
  public void testGetWhitelistedActorsNoPath() {
    Properties config = new Properties();
    config.put("whitelist", "foo;bar");

    Set<String> actors = ProcessorUtil.getWhitelistedActors(config, "no");

    assertNull(actors);
  }

  @Test
  public void testGetWhitelistedActorsEmptyValue() {
    Properties config = new Properties();
    config.put("whitelist", "");

    Set<String> actors = ProcessorUtil.getWhitelistedActors(config, "whitelist");

    assertNull(actors);
  }

  @Test
  public void testMceDelete() {
    String actor = "tester";
    DatasetIdentifier dataset = makeDataset("test");

    DeploymentDetail deployment = new DeploymentDetail();
    deployment.cluster = "foo";

    MetadataChangeEvent mce = ProcessorUtil.mceDelete(dataset, deployment, actor);

    assertEquals(mce.datasetIdentifier, dataset);
    assertEquals(mce.deploymentInfo, Collections.singletonList(deployment));
    assertEquals(mce.changeAuditStamp.type, ChangeType.DELETE);
    assertEquals(mce.changeAuditStamp.actorUrn, actor);
  }

  public static DatasetIdentifier makeDataset(String datasetName) {
    DatasetIdentifier identifier = new DatasetIdentifier();
    identifier.nativeName = datasetName;
    identifier.dataPlatformUrn = "urn:li:dataPlatform:hive";
    identifier.dataOrigin = DataOrigin.DEV;
    return identifier;
  }
}
