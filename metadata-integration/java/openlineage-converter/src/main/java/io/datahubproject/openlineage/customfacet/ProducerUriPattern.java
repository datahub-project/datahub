package io.datahubproject.openlineage.customfacet;

import java.net.URI;
import java.util.Locale;
import java.util.regex.Pattern;

public record ProducerUriPattern(String scheme, String host, Pattern path) {
  public ProducerUriPattern {
    scheme = scheme.toLowerCase(Locale.ROOT);
    host = host.toLowerCase(Locale.ROOT);
  }

  public boolean matches(URI producer) {
    return producer != null
        && producer.getRawUserInfo() == null
        && producer.getPort() == -1
        && producer.getRawQuery() == null
        && producer.getRawFragment() == null
        && scheme.equalsIgnoreCase(producer.getScheme())
        && host.equalsIgnoreCase(producer.getHost())
        && path.matcher(producer.getPath()).matches();
  }
}
