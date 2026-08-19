package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class AgentClassTest {

  @Test
  public void testIsHuman() {
    assertTrue(AgentClass.BROWSER.isHuman());
    assertTrue(AgentClass.MOBILE_APP.isHuman());
    assertFalse(AgentClass.SDK.isHuman());
    assertFalse(AgentClass.UNKNOWN.isHuman());
  }

  @Test
  public void testToMetricLabel() {
    assertEquals(AgentClass.BROWSER.toMetricLabel(), "browser");
    assertEquals(AgentClass.MOBILE_APP.toMetricLabel(), "mobileapp");
    assertEquals(AgentClass.EMAIL_CLIENT.toMetricLabel(), "emailclient");
  }

  @Test
  public void testFromRawUserAgentClass() {
    assertEquals(AgentClass.fromRawUserAgentClass("Browser"), AgentClass.BROWSER);
    assertEquals(AgentClass.fromRawUserAgentClass("Mobile App"), AgentClass.MOBILE_APP);
    assertEquals(AgentClass.fromRawUserAgentClass("INGESTION"), AgentClass.INGESTION);
    assertEquals(AgentClass.fromRawUserAgentClass(null), AgentClass.UNKNOWN);
    assertEquals(AgentClass.fromRawUserAgentClass(""), AgentClass.UNKNOWN);
    assertEquals(AgentClass.fromRawUserAgentClass("not-a-class"), AgentClass.UNKNOWN);
  }
}
