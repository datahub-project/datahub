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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Properties;
import org.testng.annotations.Test;
import wherehows.utils.ConfigUtil;

import static org.testng.Assert.*;


public class ConfigUtilTest {

  @Test
  public void testConfigToProperties() {
    Properties props = new Properties();
    props.put("foo", "foo1");
    props.put("bar", "bar2");

    Config config = ConfigFactory.parseProperties(props);

    assertEquals(ConfigUtil.configToProperties(config), props);
  }
}
