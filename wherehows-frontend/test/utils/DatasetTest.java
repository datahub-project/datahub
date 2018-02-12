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
package utils;

import org.junit.Test;

import static org.junit.Assert.*;
import static utils.Dataset.*;


public class DatasetTest {

  @Test
  public void testGetPlatformPrefix() {
    assertEquals(getPlatformPrefix("hdfs", null), "/");
    assertEquals(getPlatformPrefix("hdfs", ""), "/");
    assertEquals(getPlatformPrefix("hdfs", "/"), "/");
    assertEquals(getPlatformPrefix("hdfs", "/a"), "/a/");
    assertEquals(getPlatformPrefix("hdfs", "/a/"), "/a/");
    assertEquals(getPlatformPrefix("oracle", null), "");
    assertEquals(getPlatformPrefix("oracle", ""), "");
    assertEquals(getPlatformPrefix("oracle", "a"), "a.");
    assertEquals(getPlatformPrefix("oracle", "a."), "a.");
  }

  @Test
  public void testListNamePrefixIsDataset() {
    assertFalse(listNamePrefixIsDataset("hdfs", null));
    assertFalse(listNamePrefixIsDataset("hdfs", " "));
    assertTrue(listNamePrefixIsDataset("hdfs", "/a"));
    assertFalse(listNamePrefixIsDataset("hdfs", "/a/"));
    assertTrue(listNamePrefixIsDataset("oracle", "/b/"));
    assertFalse(listNamePrefixIsDataset("oracle", "b."));
  }
}
