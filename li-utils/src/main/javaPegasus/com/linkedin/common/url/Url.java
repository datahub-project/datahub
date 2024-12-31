package com.linkedin.common.url;

import java.net.URI;
import java.net.URISyntaxException;

public class Url {
  private final String _url;

  public Url(String url) {
    if (url == null) {
      throw new NullPointerException("URL must be non-null");
    }
    _url = url;
  }

  @Override
  public String toString() {
    return _url;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Url)) {
      return false;
    } else {
      return _url.equals(((Url) obj)._url);
    }
  }

  @Override
  public int hashCode() {
    return _url.hashCode();
  }

  public URI toURI() throws URISyntaxException {
    return new URI(_url);
  }
}
