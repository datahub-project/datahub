package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class ClientClassifierTest {

  @Test
  public void testRequestSourceBrowserHeader() {
    assertEquals(ClientClassifier.fromRequestSource("BROWSER"), ClientClass.BROWSER);
    assertEquals(ClientClassifier.fromRequestSource("browser"), ClientClass.BROWSER);
  }

  @Test
  public void testRequestSourceSdkOrAbsentIsNonBrowser() {
    assertEquals(ClientClassifier.fromRequestSource("SDK"), ClientClass.NON_BROWSER);
    assertEquals(ClientClassifier.fromRequestSource(null), ClientClass.NON_BROWSER);
    assertEquals(ClientClassifier.fromRequestSource(""), ClientClass.NON_BROWSER);
    assertEquals(ClientClassifier.fromRequestSource("anything-else"), ClientClass.NON_BROWSER);
  }
}
