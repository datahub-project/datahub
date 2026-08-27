package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchVersionInfo;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import org.testng.annotations.Test;

public class Es8SearchClientShimVersionTest {

  @Test
  public void acceptsEs818AndAbove() {
    assertTrue(Es8SearchClientShim.assertSemanticSearchSupported("8.18.0"));
    assertTrue(Es8SearchClientShim.assertSemanticSearchSupported("8.18.5"));
    assertTrue(Es8SearchClientShim.assertSemanticSearchSupported("8.19.0"));
    assertTrue(Es8SearchClientShim.assertSemanticSearchSupported("9.0.0"));
  }

  @Test
  public void rejectsBelowEs818() {
    expectThrows(
        IllegalStateException.class,
        () -> Es8SearchClientShim.assertSemanticSearchSupported("8.17.0"));
    expectThrows(
        IllegalStateException.class,
        () -> Es8SearchClientShim.assertSemanticSearchSupported("8.13.0"));
    expectThrows(
        IllegalStateException.class,
        () -> Es8SearchClientShim.assertSemanticSearchSupported("7.17.0"));
  }

  @Test
  public void rejectsUnknownVersion() {
    expectThrows(
        IllegalStateException.class,
        () -> Es8SearchClientShim.assertSemanticSearchSupported("unknown"));
    expectThrows(
        IllegalStateException.class, () -> Es8SearchClientShim.assertSemanticSearchSupported(null));
    expectThrows(
        IllegalStateException.class, () -> Es8SearchClientShim.assertSemanticSearchSupported(""));
  }

  @Test
  public void verifySemanticSearchSupportThrowsForOldVersion() throws Exception {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    InfoResponse mockInfo = mock(InfoResponse.class);
    ElasticsearchVersionInfo mockVersion = mock(ElasticsearchVersionInfo.class);

    when(mockVersion.number()).thenReturn("8.17.0");
    when(mockInfo.version()).thenReturn(mockVersion);
    when(mockClient.info()).thenReturn(mockInfo);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    IllegalStateException ex =
        expectThrows(IllegalStateException.class, shim::verifySemanticSearchSupport);
    assertTrue(
        ex.getMessage().contains("8.18"),
        "Error message should mention 8.18 requirement, got: " + ex.getMessage());
  }

  @Test
  public void verifySemanticSearchSupportAcceptsEs818() throws Exception {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    InfoResponse mockInfo = mock(InfoResponse.class);
    ElasticsearchVersionInfo mockVersion = mock(ElasticsearchVersionInfo.class);

    when(mockVersion.number()).thenReturn("8.18.0");
    when(mockInfo.version()).thenReturn(mockVersion);
    when(mockClient.info()).thenReturn(mockInfo);

    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    shim.verifySemanticSearchSupport(); // must not throw
  }
}
