package com.linkedin.metadata.utils;

import java.io.IOException;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;


public final class TestUtils {
  private TestUtils() {
    // Util class
  }

  @Nonnull
  public static String loadJsonFromResource(@Nonnull String resourceName) throws IOException {
    final String jsonStr =
        IOUtils.toString(ClassLoader.getSystemResourceAsStream(resourceName), Charset.defaultCharset());
    return jsonStr.replaceAll("\\s+", "");
  }
}

