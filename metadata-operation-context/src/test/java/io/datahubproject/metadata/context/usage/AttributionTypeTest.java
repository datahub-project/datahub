package io.datahubproject.metadata.context.usage;

import io.datahubproject.metadata.context.AgentClass;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AttributionTypeTest {

  @Test
  public void testFromAgentClassHumanClients() {
    Assert.assertEquals(AttributionType.fromAgentClass(AgentClass.BROWSER), AttributionType.HUMAN);
    Assert.assertEquals(AttributionType.fromAgentClass(AgentClass.CLI), AttributionType.HUMAN);
    Assert.assertEquals(
        AttributionType.fromAgentClass(AgentClass.MOBILE_APP), AttributionType.HUMAN);
    Assert.assertEquals(
        AttributionType.fromAgentClass(AgentClass.EMAIL_CLIENT), AttributionType.HUMAN);
  }

  @Test
  public void testFromAgentClassAutomatedClients() {
    Assert.assertEquals(
        AttributionType.fromAgentClass(AgentClass.INGESTION), AttributionType.AUTOMATED);
    Assert.assertEquals(AttributionType.fromAgentClass(AgentClass.SDK), AttributionType.AUTOMATED);
    Assert.assertEquals(
        AttributionType.fromAgentClass(AgentClass.LIBRARY), AttributionType.AUTOMATED);
    Assert.assertEquals(
        AttributionType.fromAgentClass(AgentClass.SYSTEM), AttributionType.AUTOMATED);
  }

  @Test
  public void testFromAgentClassBotClients() {
    Assert.assertEquals(AttributionType.fromAgentClass(AgentClass.ROBOT), AttributionType.BOT);
    Assert.assertEquals(AttributionType.fromAgentClass(AgentClass.CRAWLER), AttributionType.BOT);
  }

  @Test
  public void testFromAgentClassUnknownClients() {
    Assert.assertEquals(AttributionType.fromAgentClass(AgentClass.HACKER), AttributionType.UNKNOWN);
    Assert.assertEquals(
        AttributionType.fromAgentClass(AgentClass.UNKNOWN), AttributionType.UNKNOWN);
  }

  @Test
  public void testDimensionValues() {
    Assert.assertEquals(AttributionType.HUMAN.dimensionValue(), "human");
    Assert.assertEquals(AttributionType.AGENT_MEDIATED.dimensionValue(), "agent_mediated");
    Assert.assertEquals(AttributionType.AUTOMATED.dimensionValue(), "automated");
    Assert.assertEquals(AttributionType.BOT.dimensionValue(), "bot");
    Assert.assertEquals(AttributionType.UNKNOWN.dimensionValue(), "unknown");
  }
}
