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
package wherehows.util;

import com.google.common.collect.ImmutableSet;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.events.metadata.DataOrigin;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.MetadataChangeEvent;
import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.testng.annotations.Test;
import wherehows.utils.ProcessorUtil;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ProcessorUtilTest {

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
    Config config = mock(Config.class);
    when(config.hasPath("whitelist")).thenReturn(true);
    when(config.getString("whitelist")).thenReturn("foo;bar");

    Set<String> actors = ProcessorUtil.getWhitelistedActors(config, "whitelist");

    assertEquals(actors, ImmutableSet.of("foo", "bar"));
  }

  @Test
  public void testGetWhitelistedActorsNoPath() {
    Config config = mock(Config.class);
    when(config.hasPath("whitelist")).thenReturn(false);

    Set<String> actors = ProcessorUtil.getWhitelistedActors(config, "whitelist");

    assertEquals(actors, null);
  }

  @Test
  public void testGetWhitelistedActorsEmptyValue() {
    Config config = mock(Config.class);
    when(config.hasPath("whitelist")).thenReturn(true);
    when(config.getString("whitelist")).thenReturn("");

    Set<String> actors = ProcessorUtil.getWhitelistedActors(config, "whitelist");

    assertEquals(actors, null);
  }

  @Test
  public void testMceDelete() {
    String actor = "tester";
    DatasetIdentifier dataset = makeDataset("test");
    MetadataChangeEvent mce = ProcessorUtil.mceDelete(dataset, actor);

    assertEquals(mce.datasetIdentifier, dataset);
    assertEquals(mce.changeAuditStamp.type, ChangeType.DELETE);
    assertEquals(mce.changeAuditStamp.actorUrn, actor);
  }

  private DatasetIdentifier makeDataset(String datasetName) {
    DatasetIdentifier identifier = new DatasetIdentifier();
    identifier.nativeName = datasetName;
    identifier.dataPlatformUrn = "urn:li:dataPlatform:hive";
    identifier.dataOrigin = DataOrigin.DEV;
    return identifier;
  }
}
