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

  private JarExtractor() {
  }

  /**
   * Write url content to destinationFilePath
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