/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
