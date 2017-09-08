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

import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;

import static wherehows.utils.KafkaClientUtil.*;


public class KafkaClientUtilTest {

  @Test
  public void testGetPropertyTrimPrefix() {

    Properties props = new Properties();
    props.setProperty("foo.v1", "1");
    props.setProperty("foo.v2", "2");
    props.setProperty("bar.v3", "3");
    props.setProperty("bar.v4", "4");
    System.out.println(props);

    Properties foo = getPropertyTrimPrefix(props, "foo");

    Assert.assertEquals(foo.size(), 2);
    Assert.assertEquals(foo.getProperty("v1"), "1");
    Assert.assertEquals(foo.getProperty("v2"), "2");

    Properties bar = getPropertyTrimPrefix(props, "bar");

    Assert.assertEquals(bar.size(), 2);
    Assert.assertEquals(bar.getProperty("v3"), "3");
    Assert.assertEquals(bar.getProperty("v4"), "4");
  }

}
