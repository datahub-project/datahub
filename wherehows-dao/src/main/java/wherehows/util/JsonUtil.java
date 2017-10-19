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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class JsonUtil {

  private JsonUtil() {
  }

  /**
   * Cast json string to java type.
   * If string is null, return null. Throws IOException when convert error.
   * @param jsonString json string
   * @param type TypeReference
   * @param <T> type
   * @return type object
   * @throws IOException
   */
  public static <T> T jsonToTypedObject(@Nullable String jsonString, @Nonnull TypeReference type) throws IOException {
    return StringUtils.isBlank(jsonString) ? null : new ObjectMapper().readValue(jsonString, type);
  }
}
