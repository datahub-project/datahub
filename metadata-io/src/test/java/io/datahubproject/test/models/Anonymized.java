package io.datahubproject.test.models;

import com.fasterxml.jackson.annotation.JsonSetter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.codec.binary.Hex;

public abstract class Anonymized {
  public String urn;

  @JsonSetter
  public void setUrn(String urn) {
    this.urn = anonymizeUrn(urn);
  }

  private static final Pattern URN_REGEX = Pattern.compile("^(.+)[(](.+),(.+),([A-Z]+)[)]$");

  public static String anonymizeUrn(String urn) {
    if (urn != null) {
      Matcher m = URN_REGEX.matcher(urn);
      if (m.find()) {
        return String.format(
            "%s(%s,%s,%s)",
            m.group(1), anonymizeLast(m.group(2), ":"), hashFunction(m.group(3)), m.group(4));
      }
    }
    return urn;
  }

  protected static String anonymizeLast(String s, String sep) {
    String[] splits = s.split(sep);
    splits[splits.length - 1] = hashFunction(splits[splits.length - 1]);
    return String.join(sep, splits);
  }

  protected static String hashFunction(String s) {
    try {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      messageDigest.update(s.getBytes());
      char[] hex = Hex.encodeHex(messageDigest.digest());
      return new String(hex).substring(0, Math.min(s.length() - 1, hex.length - 1));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}
