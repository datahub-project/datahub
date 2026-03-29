package io.datahubproject.metadata.context.request;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataHubContextParserTest {

  @BeforeMethod
  public void resetHolder() {
    DataHubContextRulesHolder.setPolicy(DataHubContextParsePolicy.defaults());
  }

  @Test
  public void parseNullOrBlank() {
    DataHubContextParser.Parsed p = DataHubContextParser.parse(null);
    assertEquals(p.getSkill(), DataHubContextParser.UNSPECIFIED);
    assertEquals(p.getCaller(), DataHubContextParser.UNSPECIFIED);
    p = DataHubContextParser.parse("   ");
    assertEquals(p.getSkill(), DataHubContextParser.UNSPECIFIED);
  }

  @Test
  public void parseSkillAndCaller() {
    DataHubContextParser.Parsed p =
        DataHubContextParser.parse("skill=datahub-audit;caller=claude-code");
    assertEquals(p.getSkill(), "datahub-audit");
    assertEquals(p.getCaller(), "claude-code");
  }

  @Test
  public void parseTrimsWhitespace() {
    DataHubContextParser.Parsed p =
        DataHubContextParser.parse(" skill = datahub-audit ; caller = claude-code ");
    assertEquals(p.getSkill(), "datahub-audit");
    assertEquals(p.getCaller(), "claude-code");
  }

  @Test
  public void parseIgnoresUnknownKeys() {
    DataHubContextParser.Parsed p =
        DataHubContextParser.parse("foo=bar;skill=audit;evil=value;caller=cli");
    assertEquals(p.getSkill(), "audit");
    assertEquals(p.getCaller(), "cli");
  }

  @Test
  public void parseFirstWinsForDuplicateKeys() {
    DataHubContextParser.Parsed p =
        DataHubContextParser.parse("skill=first;skill=second;caller=a;caller=b");
    assertEquals(p.getSkill(), "first");
    assertEquals(p.getCaller(), "a");
  }

  @Test
  public void sanitizeReplacesInvalidCharacters() {
    DataHubContextParser.Parsed p = DataHubContextParser.parse("skill=data%20hub;caller=My Tool");
    assertEquals(p.getSkill(), "data_20hub");
    assertEquals(p.getCaller(), "my_tool");
  }

  @Test
  public void truncateLongValue() {
    String longVal = "a".repeat(80);
    DataHubContextParser.Parsed p = DataHubContextParser.parse("skill=" + longVal);
    assertEquals(p.getSkill().length(), 48);
    assertEquals(p.getCaller(), DataHubContextParser.UNSPECIFIED);
  }

  @Test
  public void allowlistMapsUnknownSanitizedValueToOther() {
    DataHubContextParsePolicy policy =
        new DataHubContextParsePolicy(
            DataHubContextParser.UNSPECIFIED,
            "other",
            48,
            List.of(
                new DataHubContextKeyRule("skill", MetricUtils.TAG_AGENT_SKILL, Set.of("audit"))));
    DataHubContextParser.Parsed allowed = DataHubContextParser.parse("skill=audit", policy);
    assertEquals(allowed.getSkill(), "audit");
    DataHubContextParser.Parsed rejected = DataHubContextParser.parse("skill=unknown", policy);
    assertEquals(rejected.getSkill(), "other");
  }
}
