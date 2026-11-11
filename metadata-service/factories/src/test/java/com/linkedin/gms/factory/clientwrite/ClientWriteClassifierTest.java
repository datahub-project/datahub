package com.linkedin.gms.factory.clientwrite;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class ClientWriteClassifierTest {

  private static Optional<ClientWrite> classify(
      String method, String path, String query, Map<String, String> headers) {
    return ClientWriteClassifier.classify(method, path, query, headers::get);
  }

  private static ClientWrite write(
      String method, String path, String query, Map<String, String> h) {
    Optional<ClientWrite> write = classify(method, path, query, h);
    assertTrue(write.isPresent(), method + " " + path + "?" + query + " should be a write");
    return write.get();
  }

  @Test
  public void testRestliIngestActionsAreWrites() {
    assertEquals(write("POST", "/aspects", "action=ingestProposal", Map.of()).getApi(), "restli");
    assertEquals(
        write("POST", "/aspects", "action=ingestProposalBatch", Map.of()).getApi(), "restli");
    assertEquals(write("POST", "/entities", "action=batchIngest", Map.of()).getApi(), "restli");
  }

  @Test
  public void testReadsAreNotWrites() {
    assertFalse(classify("POST", "/entities", "action=search", Map.of()).isPresent());
    assertFalse(classify("GET", "/aspects/urn%3Ali%3Acorpuser%3Ax", null, Map.of()).isPresent());
    assertFalse(
        classify("POST", "/openapi/v3/entity/dataset/batchGet", null, Map.of()).isPresent());
    assertFalse(classify("POST", "/openapi/v3/entity/scroll", null, Map.of()).isPresent());
    assertFalse(classify("POST", "/openapi/v2/entity/batch/dataset", null, Map.of()).isPresent());
    assertFalse(classify("GET", "/openapi/v3/entity/dataset", null, Map.of()).isPresent());
    assertFalse(classify("POST", "/api/graphql", null, Map.of()).isPresent());
  }

  @Test
  public void testOpenApiEntityWritesAreWrites() {
    assertEquals(write("POST", "/openapi/v3/entity/dataset", null, Map.of()).getApi(), "openapi");
    assertEquals(
        write("PATCH", "/openapi/v2/entity/dataset/urn:li:dataset:x/status", null, Map.of())
            .getApi(),
        "openapi");
    assertEquals(
        write("DELETE", "/openapi/v3/entity/dataset/urn:li:dataset:x", null, Map.of()).getApi(),
        "openapi");
    assertEquals(write("POST", "/openapi/entities/v1", null, Map.of()).getApi(), "openapi");
  }

  @Test
  public void testPythonClientFromVersionHeader() {
    ClientWrite write =
        write(
            "POST",
            "/aspects",
            "action=ingestProposal",
            Map.of(
                "X-DataHub-Py-Cli-Version", "1.7.0.5",
                "X-DataHub-Client-Mode", "INGESTION"));
    assertEquals(write.getClient(), "python");
    assertEquals(write.getClientVersion(), "1.7.0.5");
    assertEquals(write.getClientMode(), "INGESTION");
  }

  @Test
  public void testJavaClientFromHeaderAndFromLegacyUserAgent() {
    ClientWrite fromHeader =
        write(
            "POST",
            "/aspects",
            "action=ingestProposal",
            Map.of("X-DataHub-Java-Sdk-Version", "v1.10.0rc1-3-gabc"));
    assertEquals(fromHeader.getClient(), "java");
    assertEquals(fromHeader.getClientVersion(), "1.10.0");

    // Java SDK releases before the version header only identify themselves by User-Agent.
    ClientWrite fromUserAgent =
        write(
            "POST",
            "/aspects",
            "action=ingestProposal",
            Map.of("User-Agent", "DataHub-RestClient/1.6.0"));
    assertEquals(fromUserAgent.getClient(), "java");
    assertEquals(fromUserAgent.getClientVersion(), "1.6.0");
  }

  @Test
  public void testPythonClientFromUserAgentWhenHeaderMissing() {
    ClientWrite write =
        write(
            "POST",
            "/aspects",
            "action=ingestProposal",
            Map.of(
                "User-Agent",
                "DataHub-Client/1.0 (sdk; datahub/script; 1.8.0) python-requests/2.32.3"));
    assertEquals(write.getClient(), "python");
    assertEquals(write.getClientVersion(), "1.8.0");
    assertEquals(write.getClientMode(), "SDK");
  }

  @Test
  public void testHeaderlessWriteIsUnknownAndKeepsUserAgent() {
    ClientWrite write =
        write(
            "POST",
            "/openapi/v3/entity/dataset",
            null,
            Map.of("User-Agent", "python-requests/2.31.0\n\"injected\""));
    assertEquals(write.getClient(), "unknown");
    assertEquals(write.getClientVersion(), "none");
    assertEquals(write.getClientMode(), "none");
    assertEquals(write.getUserAgent(), "python-requests/2.31.0?'injected'");
  }

  @Test
  public void testBrowserWrite() {
    ClientWrite write =
        write(
            "POST",
            "/openapi/v3/entity/dataset",
            null,
            Map.of("X-DataHub-Request-Source", "BROWSER"));
    assertEquals(write.getClient(), "browser");
  }

  @Test
  public void testVersionAndModeNormalization() {
    assertEquals(ClientWriteClassifier.normalizeVersion("1.9.0rc1"), "1.9.0");
    assertEquals(ClientWriteClassifier.normalizeVersion("1!0.0.0.dev0"), "unparseable");
    assertEquals(
        ClientWriteClassifier.normalizeVersion("unavailable (installed in develop mode)"),
        "unparseable");
    assertEquals(ClientWriteClassifier.normalizeVersion(null), "none");
    assertEquals(ClientWriteClassifier.normalizeClientMode("cli"), "CLI");
    assertEquals(ClientWriteClassifier.normalizeClientMode("SOMETHING"), "other");
  }
}
