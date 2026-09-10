package com.linkedin.metadata.config.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RateLimitConfigLoaderTest {

  private static final String CLASSPATH_FIXTURE = "rate-limit-loader-classpath.yaml";
  private static final String OVERRIDE_FIXTURE = "rate-limit-loader-override.yaml";

  private RateLimitConfigLoader loader;

  @BeforeMethod
  public void setUp() {
    loader = new RateLimitConfigLoader(new ObjectMapper(), new YAMLMapper());
  }

  @Test
  public void defaultClasspathFilePreservesSpringScalars() {
    RateLimitProperties spring = springBase();
    RateLimitProperties effective = loader.loadEffective(spring);

    assertEquals(effective.getMinRetryAfterSeconds(), 60);
    assertEquals(effective.getRetryAfterJitterPercent(), 10);
    assertTrue(effective.getEndpoint().getRules().isEmpty());
  }

  @Test
  public void configuredFileReplacesClasspathDocument() {
    RateLimitProperties classpath = springBase();
    classpath.getConfigFile().setPath(CLASSPATH_FIXTURE);
    RateLimitProperties fromClasspath = loader.loadEffective(classpath);
    assertEquals(fromClasspath.getEndpoint().getRules().get(0).getId(), "classpath-rule");
    assertEquals(fromClasspath.getMinRetryAfterSeconds(), 90);

    RateLimitProperties mounted = springBase();
    mounted.getConfigFile().setPath(OVERRIDE_FIXTURE);
    RateLimitProperties fromFile = loader.loadEffective(mounted);
    assertEquals(fromFile.getEndpoint().getRules().size(), 1);
    assertEquals(fromFile.getEndpoint().getRules().get(0).getId(), "aspects-ingest");
    assertEquals(fromFile.getMinRetryAfterSeconds(), 120);
  }

  @Test
  public void jsonOverlayReplacesEndpointRulesAndMergesHeavyResolvers() {
    RateLimitProperties spring = springBase();
    spring.getConfigFile().setPath(OVERRIDE_FIXTURE);
    spring.setConfigJson(
        "{\"endpoint\":{\"rules\":[{\"id\":\"json-rule\",\"pathPattern\":\"/openapi/**\","
            + "\"methods\":[\"GET\"],\"capacity\":5,\"refillTokens\":5,\"refillPeriodSeconds\":60}]},"
            + "\"scoped\":{\"heavyResolvers\":{\"getEntities\":"
            + "{\"capacity\":250,\"refillTokens\":50,\"refillPeriodSeconds\":60}}}}");

    RateLimitProperties effective = loader.loadEffective(spring);

    List<RateLimitProperties.Rule> rules = effective.getEndpoint().getRules();
    assertEquals(rules.size(), 1);
    assertEquals(rules.get(0).getId(), "json-rule");
    assertEquals(
        effective.getScoped().getHeavyResolvers().get("searchAcrossEntities").getCapacity(), 50);
    assertEquals(effective.getScoped().getHeavyResolvers().get("getEntities").getCapacity(), 250);
    assertEquals(effective.getMinRetryAfterSeconds(), 120);
    assertTrue(effective.isFailOpen());
  }

  @Test
  public void partialJsonOverlayPreservesUnsetFields() {
    RateLimitProperties spring = springBase();
    spring.setFailOpen(true);
    spring.setConfigJson("{\"endpoint\":{\"enabled\":true}}");

    RateLimitProperties effective = loader.loadEffective(spring);
    assertTrue(effective.getEndpoint().isEnabled());
    assertTrue(effective.isFailOpen());
    assertEquals(effective.getMinRetryAfterSeconds(), 60);
  }

  @Test
  public void malformedJsonFailsStartup() {
    RateLimitProperties spring = springBase();
    spring.setConfigJson("{not-json");
    assertThrows(IllegalStateException.class, () -> loader.loadEffective(spring));
  }

  @Test
  public void missingMountedFileFailsStartup() {
    RateLimitProperties spring = springBase();
    spring.getConfigFile().setPath("file:/tmp/datahub-missing-rate-limits-does-not-exist.yaml");
    assertThrows(IllegalStateException.class, () -> loader.loadEffective(spring));
  }

  @Test
  public void wrappedDatahubPathAndBareFragmentsBothBind() {
    RateLimitProperties wrapped = springBase();
    wrapped.getConfigFile().setPath(OVERRIDE_FIXTURE);
    assertEquals(
        loader.loadEffective(wrapped).getEndpoint().getRules().get(0).getId(), "aspects-ingest");

    RateLimitProperties rateLimitsWrapper = springBase();
    rateLimitsWrapper.getConfigFile().setPath("rate-limit-loader-bare-wrapper.yaml");
    assertEquals(
        loader.loadEffective(rateLimitsWrapper).getEndpoint().getRules().get(0).getId(),
        "bare-wrapper-rule");

    RateLimitProperties fragment = springBase();
    fragment.getConfigFile().setPath("rate-limit-loader-bare-fragment.yaml");
    assertEquals(
        loader.loadEffective(fragment).getEndpoint().getRules().get(0).getId(),
        "bare-fragment-rule");
  }

  @Test
  public void fileUriAndBareFilesystemPathBothOpen() throws Exception {
    Path yaml = Files.createTempFile("rate-limits-override", ".yaml");
    Files.writeString(
        yaml,
        "datahub:\n  gms:\n    rateLimits:\n      endpoint:\n        rules:\n"
            + "          - id: fs-rule\n            pathPattern: /aspects**\n"
            + "            methods: [POST]\n            capacity: 1\n"
            + "            refillTokens: 1\n            refillPeriodSeconds: 60\n");

    RateLimitProperties fileUri = springBase();
    fileUri.getConfigFile().setPath("file:" + yaml.toAbsolutePath());
    assertEquals(loader.loadEffective(fileUri).getEndpoint().getRules().get(0).getId(), "fs-rule");

    RateLimitProperties bare = springBase();
    bare.getConfigFile().setPath(yaml.toAbsolutePath().toString());
    assertEquals(loader.loadEffective(bare).getEndpoint().getRules().get(0).getId(), "fs-rule");
  }

  @Test
  public void overlayDoesNotOverwriteConfigFilePointer() {
    RateLimitProperties spring = springBase();
    spring.getConfigFile().setPath(OVERRIDE_FIXTURE);
    RateLimitProperties effective = loader.loadEffective(spring);
    assertEquals(effective.getConfigFile().getPath(), OVERRIDE_FIXTURE);
  }

  @Test
  public void rulesOnlyFileKeepsSpringEndpointEnabledAndGraphqlPath() {
    RateLimitProperties spring = springBase();
    spring.getEndpoint().setEnabled(true);
    spring.getCapacity().setEnabled(true);
    spring.getCapacity().getGraphql().setOperationRulesEnabled(true);
    spring.getConfigFile().setPath(OVERRIDE_FIXTURE);

    RateLimitProperties effective = loader.loadEffective(spring);
    assertTrue(effective.getEndpoint().isEnabled());
    assertTrue(effective.getCapacity().isEnabled());
    assertEquals(effective.getCapacity().getGraphql().getPathPattern(), "/api/graphql");
    assertTrue(effective.getCapacity().getGraphql().isOperationRulesEnabled());
    assertEquals(effective.getEndpoint().getRules().get(0).getId(), "aspects-ingest");
  }

  @Test
  public void capacityRulesOverlayKeepsGraphqlPath() {
    RateLimitProperties spring = springBase();
    spring.getCapacity().setEnabled(true);
    spring.setConfigJson(
        "{\"capacity\":{\"rules\":[{\"id\":\"graphql-search-capacity\","
            + "\"pathPattern\":\"/api/graphql\",\"methods\":[\"POST\"],"
            + "\"graphqlOperationNames\":[\"searchAcrossEntities\"],"
            + "\"initialLimit\":30,\"maxLimit\":400}]}}");

    RateLimitProperties effective = loader.loadEffective(spring);
    assertTrue(effective.getCapacity().isEnabled());
    assertEquals(effective.getCapacity().getGraphql().getPathPattern(), "/api/graphql");
    assertEquals(effective.getCapacity().getRules().get(0).getId(), "graphql-search-capacity");
  }

  @Test
  public void environmentPointersSupplyFileAndJsonWhenBeanOmitsThem() {
    RateLimitProperties spring = springBase();
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV, OVERRIDE_FIXTURE);
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_JSON_ENV, "{\"endpoint\":{\"enabled\":true}}");

    RateLimitProperties effective = loader.loadEffective(spring, environment);
    assertEquals(effective.getEndpoint().getRules().get(0).getId(), "aspects-ingest");
    assertTrue(effective.getEndpoint().isEnabled());
  }

  private static RateLimitProperties springBase() {
    RateLimitProperties spring = new RateLimitProperties();
    spring.setMinRetryAfterSeconds(60);
    spring.setRetryAfterJitterPercent(10);
    spring.setFailOpen(true);
    spring.getCapacity().getGraphql().setPathPattern("/api/graphql");
    return spring;
  }
}
