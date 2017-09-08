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

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

import static wherehows.common.utils.StringUtil.*;


public class StringUtilTest {

  @Test
  public void testToStringMap() {
    CharSequence key = "foo";
    Object value = "bar";

    Map<CharSequence, Object> map = new HashMap<>();
    map.put(key, value);

    Map newMap = toStringMap(map);

    Assert.assertTrue(newMap.containsKey("foo"));
    Assert.assertEquals(newMap.get("foo"), "bar");
  }
}
