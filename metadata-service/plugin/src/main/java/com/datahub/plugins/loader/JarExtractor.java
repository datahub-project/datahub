/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.plugins.loader;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JarExtractor {

  private JarExtractor() {}

  /**
   * Write url content to destinationFilePath
   *
   * @param url
   * @param destinationFilePath
   * @throws IOException
   */
  public static void write(@Nonnull URL url, @Nonnull Path destinationFilePath) throws IOException {
    try (InputStream input = url.openStream()) {
      try (FileOutputStream output = new FileOutputStream(destinationFilePath.toFile())) {
        while (input.available() > 0) {
          output.write(input.read());
        }
      }
    }
  }
}
