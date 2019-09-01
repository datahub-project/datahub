package com.linkedin.common.urn;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Urn {

  static final String URN_PREFIX = "urn:li:";

  // the URN format is  urn:li:<entityType>:<content>
  private static final Pattern URN_PATTERN = Pattern.compile("^" + URN_PREFIX + "(\\w+?):(.+)$");

  private final String _entityType;
  private final String _urn;
  private final String _content;

  public Urn(String rawUrn) throws URISyntaxException {
    Matcher matcher = URN_PATTERN.matcher(rawUrn);
    if (matcher.find()) {
      this._urn = rawUrn;
      this._entityType = matcher.group(1);
      this._content = matcher.group(2);
    } else {
      throw new URISyntaxException(rawUrn, "URN deserialization error");
    }
  }

  public Urn(String entityType, String content) {
    this._entityType = entityType;
    this._content = content;
    this._urn = URN_PREFIX + entityType + ":" + content;
  }

  public static Urn createFromString(String rawUrn) throws URISyntaxException {
    return new Urn(rawUrn);
  }

  public String getEntityType() {
    return _entityType;
  }

  public String getContent() {
    return _content;
  }

  public Long getIdAsLong() {
    return Long.valueOf(_content);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && Urn.class.isAssignableFrom(obj.getClass())) {
      Urn other = (Urn) obj;
      return this._urn.equals(other._urn);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this._urn.hashCode();
  }

  @Override
  public String toString() {
    return _urn;
  }
}