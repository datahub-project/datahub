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
package wherehows.utils;

import com.typesafe.config.Config;
import java.util.Properties;
import javax.annotation.Nonnull;


public class ConfigUtil {

  private ConfigUtil() {
  }

  /**
   * Convert typesafe {@link Config} to {@link Properties}
   */
  public static Properties configToProperties(@Nonnull Config config) {
    Properties properties = new Properties();
    config.entrySet().forEach(e -> properties.setProperty(e.getKey(), config.getString(e.getKey())));
    return properties;
  }
}
