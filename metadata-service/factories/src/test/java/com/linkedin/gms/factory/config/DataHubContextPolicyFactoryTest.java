package com.linkedin.gms.factory.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.requestcontext.ContextHeaderConfiguration;
import com.linkedin.metadata.config.requestcontext.RequestContextConfiguration;
import io.datahubproject.metadata.context.request.DataHubContextParsePolicy;
import io.datahubproject.metadata.context.request.DataHubContextParser;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class DataHubContextPolicyFactoryTest {

  private static final String U = DataHubContextParser.UNSPECIFIED;

  private static final String DEFAULT_JSON =
      "[{\"key\":\"skill\",\"values\":[\"datahub-audit\"]},{\"key\":\"caller\",\"values\":[\"claude-code\"]}]";

  @Test
  public void documentedExampleMatchesSingleJsonAllowlists() {
    ContextHeaderConfiguration ch = new ContextHeaderConfiguration();
    ch.setValueAllowlistsJson(DEFAULT_JSON);
    RequestContextConfiguration rc = new RequestContextConfiguration();
    rc.setContextHeader(ch);

    DataHubContextParsePolicy policy = DataHubContextPolicyFactory.from(rc);
    DataHubContextParser.Parsed forward =
        DataHubContextParser.parse("skill=datahub-audit;caller=claude-code", policy);
    assertEquals(forward.getSkill(), "datahub-audit");
    assertEquals(forward.getCaller(), "claude-code");

    DataHubContextParser.Parsed reversed =
        DataHubContextParser.parse("caller=claude-code;skill=datahub-audit", policy);
    assertEquals(reversed.getSkill(), "datahub-audit");
    assertEquals(reversed.getCaller(), "claude-code");
  }

  @Test
  public void metricTagOrderIsLexicographicByKeyNotJsonArrayOrder() {
    ContextHeaderConfiguration ch = new ContextHeaderConfiguration();
    ch.setValueAllowlistsJson(DEFAULT_JSON);
    RequestContextConfiguration rc = new RequestContextConfiguration();
    rc.setContextHeader(ch);
    DataHubContextParsePolicy policy = DataHubContextPolicyFactory.from(rc);
    String[] pairs = DataHubContextParser.parse(null, policy).flatMicrometerTagPairs();
    assertEquals(pairs[0], "agent_caller");
    assertEquals(pairs[2], "agent_skill");
  }

  @Test
  public void buildsAllowlistFromJsonConfiguration() {
    ContextHeaderConfiguration ch = new ContextHeaderConfiguration();
    ch.setMaxValueLength(48);
    ch.setValueAllowlistsJson("[{\"key\":\"skill\",\"values\":[\"audit\",\"cli\"]}]");
    RequestContextConfiguration rc = new RequestContextConfiguration();
    rc.setContextHeader(ch);

    DataHubContextParsePolicy policy = DataHubContextPolicyFactory.from(rc);
    assertEquals(DataHubContextParser.parse("skill=x", policy).getSkill(), "other");
    assertEquals(DataHubContextParser.parse("skill=audit", policy).getSkill(), "audit");
  }

  @Test
  public void parseValueAllowlistsJsonMergesSameKey() {
    Map<String, Set<String>> m =
        DataHubContextPolicyFactory.parseValueAllowlistsJson(
            "[{\"key\":\"skill\",\"values\":[\"audit\"]},{\"key\":\"skill\",\"values\":[\"cli\"]}]",
            48,
            U);
    assertEquals(m.get("skill"), Set.of("audit", "cli"));
  }

  @Test
  public void parseValueAllowlistsJsonEmptyValuesArrayClearsToUnrestricted() {
    Map<String, Set<String>> m =
        DataHubContextPolicyFactory.parseValueAllowlistsJson(
            "[{\"key\":\"skill\",\"values\":[\"audit\"]},{\"key\":\"skill\",\"values\":[]}]",
            48,
            U);
    assertTrue(m.containsKey("skill"));
    assertTrue(m.get("skill").isEmpty());
  }

  @Test
  public void parseValueAllowlistsJsonIgnoresInvalidJson() {
    Map<String, Set<String>> m =
        DataHubContextPolicyFactory.parseValueAllowlistsJson("not-json", 48, U);
    assertTrue(m.isEmpty());
  }

  @Test
  public void parseValueAllowlistsJsonSanitizesValues() {
    Map<String, Set<String>> m =
        DataHubContextPolicyFactory.parseValueAllowlistsJson(
            "[{\"key\":\"skill\",\"values\":[\" My Tool \",\"audit\"]}]", 48, U);
    assertEquals(m.get("skill"), Set.of("my_tool", "audit"));
  }

  @Test
  public void parseValueAllowlistsJsonMissingValuesMeansUnrestricted() {
    Map<String, Set<String>> m =
        DataHubContextPolicyFactory.parseValueAllowlistsJson("[{\"key\":\"skill\"}]", 48, U);
    assertTrue(m.containsKey("skill"));
    assertTrue(m.get("skill").isEmpty());
  }

  @Test
  public void parseValueAllowlistsJsonAcceptsSingleObject() {
    Map<String, Set<String>> m =
        DataHubContextPolicyFactory.parseValueAllowlistsJson(
            "{\"key\":\"skill\",\"values\":[\"x\"]}", 48, U);
    assertEquals(m.get("skill"), Set.of("x"));
  }
}
