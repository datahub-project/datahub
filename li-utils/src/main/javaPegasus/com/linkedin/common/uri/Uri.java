package com.linkedin.common.uri;

import java.net.URI;
import java.net.URISyntaxException;

public class Uri {
  private final String _uri;

  public Uri(String url) {
    if (url == null) {
      throw new NullPointerException("URL must be non-null");
    }
    _uri = url;
  }

  @Override
  public String toString() {
    return _uri;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Uri)) {
      return false;
    } else {
      return _uri.equals(((Uri) obj)._uri);
    }
  }

  @Override
  public int hashCode() {
    return _uri.hashCode();
  }

  public URI toURI() throws URISyntaxException {
    return new URI(_uri);
  }
}
