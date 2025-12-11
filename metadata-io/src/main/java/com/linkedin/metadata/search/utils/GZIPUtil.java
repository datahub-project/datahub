/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPUtil {
  private GZIPUtil() {}

  public static String gzipDecompress(byte[] gzipped) {
    String unzipped;
    try (ByteArrayInputStream bis = new ByteArrayInputStream(gzipped);
        GZIPInputStream gis = new GZIPInputStream(bis);
        ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gis.read(buffer)) != -1) {
        bos.write(buffer, 0, len);
      }
      unzipped = bos.toString(StandardCharsets.UTF_8);
    } catch (IOException ie) {
      throw new IllegalStateException("Error while unzipping value.", ie);
    }
    return unzipped;
  }

  public static byte[] gzipCompress(String unzipped) {
    byte[] gzipped;
    try (ByteArrayInputStream bis =
            new ByteArrayInputStream(unzipped.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bos)) {
      byte[] buffer = new byte[1024];
      int len;
      while ((len = bis.read(buffer)) != -1) {
        gzipOutputStream.write(buffer, 0, len);
      }
      gzipOutputStream.finish();
      gzipped = bos.toByteArray();
    } catch (IOException ie) {
      throw new IllegalStateException("Error while gzipping value: " + unzipped);
    }
    return gzipped;
  }
}
