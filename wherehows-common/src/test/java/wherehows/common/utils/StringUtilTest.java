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
package wherehows.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static wherehows.common.utils.StringUtil.*;


public class StringUtilTest {

  @Test
  public void testToStringReplaceNull() {
    assertEquals(toStringReplaceNull(null, "NA"), "NA");

    CharSequence cs = "foo";
    assertEquals(toStringReplaceNull(cs, null), "foo");
  }

  @Test
  public void testToStringMap() {
    CharSequence key = "foo";
    Object value = "bar";

    Map<CharSequence, Object> map = new HashMap<>();
    map.put(key, value);

    Map newMap = toStringMap(map);

    assertTrue(newMap.containsKey("foo"));
    assertEquals(newMap.get("foo"), "bar");
  }

  @Test
  public void testToStringList() {
    List<String> testNull = toStringList(null);
    assertEquals(testNull, null);

    List<CharSequence> chars = new ArrayList<>();
    chars.add("abc");

    List<String> strings = toStringList(chars);

    assertEquals(strings.size(), 1);
    assertEquals(strings.get(0), "abc");

    List<Object> objs = new ArrayList<>();
    objs.add("test");
    objs.add(null);

    strings = toStringList(objs);

    assertEquals(strings.size(), 2);
    assertEquals(strings.get(0), "test");
    assertEquals(strings.get(1), null);
  }
}
