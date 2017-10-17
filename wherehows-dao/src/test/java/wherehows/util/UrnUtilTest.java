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

import com.linkedin.events.metadata.DataOrigin;
import com.linkedin.events.metadata.DatasetIdentifier;
import com.linkedin.events.metadata.OwnershipProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static wherehows.util.UrnUtil.*;


public class UrnUtilTest {

  @Test
  public void testGetUrnEntity() {
    assertEquals(getUrnEntity(null), null);

    String urn = "urn:li:user:test";
    String entity = getUrnEntity(urn);

    assertEquals(entity, "test");

    urn = "urn:abc";
    entity = getUrnEntity(urn);
    assertEquals(entity, "abc");
  }

  @Test
  public void testToWhDatasetUrn() {

    DatasetIdentifier identifier = new DatasetIdentifier();
    identifier.dataOrigin = DataOrigin.PROD;
    identifier.dataPlatformUrn = "urn:li:dataPlatform:oracle";
    identifier.nativeName = "orc.test";

    String urn = toWhDatasetUrn(identifier);
    assertEquals(urn, "oracle:///orc/test");

    identifier.dataPlatformUrn = "urn:li:dataPlatform:teradata";
    identifier.nativeName = "tera.test";

    urn = toWhDatasetUrn(identifier);
    assertEquals(urn, "teradata:///tera/test");

    identifier.dataPlatformUrn = "urn:li:dataPlatform:hdfs";
    identifier.nativeName = "/data/abc/test";

    urn = toWhDatasetUrn(identifier);
    assertEquals(urn, "hdfs:///data/abc/test");
  }

  @Test
  public void testSplitWhUrn() {

    String urn = "teradata:///DM/ABC";
    String[] parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "teradata");
    assertEquals(parts[1], "DM.ABC");

    urn = "hdfs:///data/derived/abc";
    parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "hdfs");
    assertEquals(parts[1], "/data/derived/abc");

    urn = "hive:///abook/abc";
    parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "hive");
    assertEquals(parts[1], "abook.abc");

    urn = "mysql:///abook/abc";
    parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "mysql");
    assertEquals(parts[1], "abook.abc");

    urn = "espresso:///identity/profile";
    parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "espresso");
    assertEquals(parts[1], "identity.profile");

    urn = "oracle:///ABOOK/ABC";
    parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "oracle");
    assertEquals(parts[1], "ABOOK.ABC");

    urn = "kafka:///PageViewEvent";
    parts = splitWhUrn(urn);

    assertEquals(parts.length, 2);
    assertEquals(parts[0], "kafka");
    assertEquals(parts[1], "PageViewEvent");
  }

  @Test
  public void testSplitWhDatasetUrn() {
    String urn = "oracle:///orc/test";
    String[] parts = parseWhDatasetUrn(urn);

    assertEquals(parts.length, 4);
    assertEquals(parts[0], "oracle");
    assertEquals(parts[1], "/orc");
    assertEquals(parts[2], "orc");
    assertEquals(parts[3], "test");

    urn = "hdfs:///data/abc/def/test";
    parts = parseWhDatasetUrn(urn);

    assertEquals(parts.length, 4);
    assertEquals(parts[0], "hdfs");
    assertEquals(parts[1], "/data/abc");
    assertEquals(parts[2], "/data/abc/def");
    assertEquals(parts[3], "test");

    urn = "kafka:///test";
    parts = parseWhDatasetUrn(urn);

    assertEquals(parts.length, 4);
    assertEquals(parts[0], "kafka");
    assertEquals(parts[1], "");
    assertEquals(parts[2], "");
    assertEquals(parts[3], "test");
  }

  @Test
  public void testCoalesce() {
    String first = coalesce(null, "a", "b");

    assertEquals(first, "a");

    Object uno = coalesce(1, null, 2, null);

    assertEquals(uno, 1);
  }

  @Test
  public void testToStringOrNull() {
    assertEquals(toStringOrNull(null), null);

    CharSequence cs = "foo";
    assertEquals(toStringOrNull(cs), "foo");
  }

  @Test
  public void testTrimToLength() {
    assertEquals(trimToLength(null, 2), null);

    String s = "foobar";

    assertEquals(trimToLength(s, 3), "foo");
    assertEquals(trimToLength(s, 6), "foobar");
  }

  @Test
  public void testEnumNameOrDefault() {
    assertEquals(enumNameOrDefault(null, "default"), "default");

    OwnershipProvider o = OwnershipProvider.DB;

    assertEquals(enumNameOrDefault(o, "default"), "DB");
  }
}
