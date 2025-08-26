package com.linkedin.metadata.test.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestMd5 {
  public static @Nullable String getMd5(@Nullable String input) {
    if (input == null) {
      return null;
    }
    try {
      log.debug("Generating MD5 hash for input: " + input);
      MessageDigest md = MessageDigest.getInstance("MD5");

      // Add password bytes to digest
      md.update(input.getBytes());

      // Get the hash's bytes
      byte[] bytes = md.digest();

      // Convert it to hexadecimal format
      StringBuilder sb = new StringBuilder();
      for (byte aByte : bytes) {
        sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
      }

      String md5Hash = sb.toString();
      log.debug("MD5 hash is:" + md5Hash);

      // Get complete hash
      return md5Hash;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private TestMd5() {}
}
