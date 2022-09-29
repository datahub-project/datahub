package com.datahub.plugins.loader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class JarExtractor {

  private JarExtractor() { }
  public static void write(URL url, Path destinationFilePath) throws IOException {
    try (InputStream input = url.openStream()) {
      try (FileOutputStream output = new FileOutputStream(destinationFilePath.toFile())) {
        while (input.available() > 0) {
          output.write(input.read());
        }
      }
    }
  }

  public static void unzipJar(Path destinationDir, Path jarPath) throws IOException {
    JarFile jar = new JarFile(jarPath.toFile());

    // Create the destination directory if not exist
    if (!destinationDir.toFile().exists()) {
      destinationDir.toFile().mkdirs();
    }
    log.info(String.format("Extracting %s to %s", jarPath, destinationDir));
    // First enumerate all directories create them on destination path
    for (Enumeration<JarEntry> enums = jar.entries(); enums.hasMoreElements(); ) {
      JarEntry entry = (JarEntry) enums.nextElement();

      String fileName = destinationDir + File.separator + entry.getName();
      File f = new File(fileName);
      if (fileName.endsWith("/")) {
        f.mkdirs();
      }
    }
    // Create all files
    for (Enumeration<JarEntry> enums = jar.entries(); enums.hasMoreElements(); ) {
      JarEntry entry = (JarEntry) enums.nextElement();

      String fileName = destinationDir + File.separator + entry.getName();
      File f = new File(fileName);

      if (fileName.endsWith("/")) {
        // it is directory
        continue;
      }

      try (InputStream input = jar.getInputStream(entry)) {
        try (FileOutputStream output = new FileOutputStream(f)) {
          while (input.available() > 0) {
            output.write(input.read());
          }
        }
      }
    }
  }
}