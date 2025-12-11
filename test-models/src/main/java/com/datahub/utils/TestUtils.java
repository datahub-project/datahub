/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.utils;

import com.datahub.test.testing.urn.BarUrn;
import com.datahub.test.testing.urn.FooUrn;
import com.linkedin.common.urn.Urn;
import java.io.IOException;
import java.net.URISyntaxException;
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
        IOUtils.toString(
            ClassLoader.getSystemResourceAsStream(resourceName), Charset.defaultCharset());
    return jsonStr.replaceAll("\\s+", "");
  }

  @Nonnull
  public static Urn makeUrn(@Nonnull Object id) {
    try {
      return new Urn("urn:li:testing:" + id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static FooUrn makeFooUrn(int id) {
    try {
      return new FooUrn(id);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static BarUrn makeBarUrn(int id) {
    return new BarUrn(id);
  }
}
