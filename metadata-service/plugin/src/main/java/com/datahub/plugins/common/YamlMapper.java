/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.common;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import javax.annotation.Nonnull;

/** A mapper to map plugin configuration to java Pojo classes */
public class YamlMapper<T> {
  private final ObjectMapper objectMapper;

  public YamlMapper() {
    this.objectMapper =
        YAMLMapper.builder().enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS).build();
    objectMapper.registerModule(new Jdk8Module());
  }

  public T fromMap(@Nonnull Map<String, Object> params, Class<T> clazz) {
    return objectMapper.convertValue(params, clazz);
  }

  public T fromFile(@Nonnull Path file, @Nonnull Class clazz) {
    T pojo = null;
    try {
      pojo = (T) objectMapper.readValue(file.toFile(), clazz);
    } catch (IOException e) {
      // Won't occur as we're already checking file existence in ConfigProvider's load method
      throw new RuntimeException(e);
    }
    return pojo;
  }
}
