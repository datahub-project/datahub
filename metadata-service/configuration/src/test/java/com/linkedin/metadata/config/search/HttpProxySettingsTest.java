package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.testng.annotations.Test;

public class HttpProxySettingsTest {

  @Test
  public void testExplicitHostWinsOverSystemProperties() {
    HttpProxySettings settings =
        HttpProxySettings.builder()
            .host("corp-proxy")
            .port(8080)
            .scheme("http")
            .useSystemProxyProperties(true)
            .build();
    Map<String, String> jvm = Map.of("http.proxyHost", "jvm-proxy", "http.proxyPort", "3128");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(settings, "search.internal", false, jvm::get);

    assertEquals(resolved.get().getHost(), "corp-proxy");
    assertEquals(resolved.get().getPort(), 8080);
  }

  @Test
  public void testSystemHttpProxyFallback() {
    Function<String, String> jvm = props("http.proxyHost", "jvm-proxy", "http.proxyPort", "3128");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(null, "search.internal", false, jvm);

    assertEquals(resolved.get().getHost(), "jvm-proxy");
    assertEquals(resolved.get().getPort(), 3128);
    assertEquals(resolved.get().getScheme(), "http");
  }

  @Test
  public void testHttpsClusterPrefersHttpsProxyHost() {
    Function<String, String> jvm =
        props(
            "http.proxyHost",
            "http-proxy",
            "http.proxyPort",
            "3128",
            "https.proxyHost",
            "https-proxy",
            "https.proxyPort",
            "8443");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(null, "search.internal", true, jvm);

    assertEquals(resolved.get().getHost(), "https-proxy");
    assertEquals(resolved.get().getPort(), 8443);
  }

  @Test
  public void testHttpsClusterFallsBackToHttpProxyHost() {
    Function<String, String> jvm = props("http.proxyHost", "http-proxy", "http.proxyPort", "3128");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(null, "search.internal", true, jvm);

    assertEquals(resolved.get().getHost(), "http-proxy");
  }

  @Test
  public void testNonProxyHostsBypassesSystemProxy() {
    Function<String, String> jvm =
        props("http.proxyHost", "jvm-proxy", "http.nonProxyHosts", "localhost|*.internal");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(null, "search.internal", false, jvm);

    assertTrue(resolved.isEmpty());
  }

  @Test
  public void testUseSystemProxyPropertiesFalseStaysDirect() {
    HttpProxySettings settings =
        HttpProxySettings.builder().useSystemProxyProperties(false).build();
    Function<String, String> jvm = props("http.proxyHost", "jvm-proxy");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(settings, "search.internal", false, jvm);

    assertTrue(resolved.isEmpty());
  }

  @Test
  public void testUnrelatedSystemPropertiesAreIgnored() {
    Map<String, String> jvm = new HashMap<>();
    jvm.put("http.proxyHost", "jvm-proxy");
    jvm.put("http.maxConnections", "1");
    jvm.put("javax.net.ssl.trustStore", "/tmp/ignored.jks");
    jvm.put("https.protocols", "TLSv1.2");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(null, "search.internal", false, jvm::get);

    assertEquals(resolved.get().getHost(), "jvm-proxy");
    assertEquals(resolved.get().getPort(), 80);
  }

  @Test
  public void testExplicitDefaultPortFromScheme() {
    HttpProxySettings https =
        HttpProxySettings.builder().host("proxy.example").scheme("https").build();
    assertEquals(
        HttpProxySettings.resolve(https, "search", false, key -> null).get().getPort(), 443);

    HttpProxySettings http = HttpProxySettings.builder().host("proxy.example").build();
    assertEquals(HttpProxySettings.resolve(http, "search", false, key -> null).get().getPort(), 80);
  }

  @Test
  public void testNonProxyHostsDoesNotAffectExplicitProxy() {
    HttpProxySettings settings = HttpProxySettings.builder().host("corp-proxy").port(8080).build();
    Function<String, String> jvm = props("http.nonProxyHosts", "search.internal");

    Optional<HttpProxySettings.ResolvedHttpProxy> resolved =
        HttpProxySettings.resolve(settings, "search.internal", false, jvm);

    assertFalse(resolved.isEmpty());
    assertEquals(resolved.get().getHost(), "corp-proxy");
  }

  private static Function<String, String> props(String... keyValues) {
    Map<String, String> map = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      map.put(keyValues[i], keyValues[i + 1]);
    }
    return map::get;
  }
}
