package com.linkedin.metadata.aliases.sideeffects;

import static com.linkedin.metadata.Constants.ALIASES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_KEY_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Aliases;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AliasesSideEffectTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final AspectPluginConfig CONFIG =
      AspectPluginConfig.builder()
          .className(AliasesSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY", "UPSERT", "RESTATE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(DATASET_ENTITY_NAME)
                      .aspectName(DATASET_KEY_ASPECT_NAME)
                      .build()))
          .build();

  private MockAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private AliasesSideEffect sideEffect;
  private OperationFingerprint mockOpContext;

  @BeforeMethod
  public void setup() {
    mockOpContext = mock(OperationFingerprint.class);
    mockAspectRetriever = new MockAspectRetriever(new HashMap<>());
    mockAspectRetriever.setEntityRegistry(TEST_REGISTRY);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .aspectRetriever(mockAspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
    sideEffect = new AliasesSideEffect().setConfig(CONFIG);
  }

  private ChangeItemImpl keyItem(Urn urn) {
    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    return ChangeItemImpl.builder()
        .urn(urn)
        .aspectName(DATASET_KEY_ASPECT_NAME)
        .entitySpec(entitySpec)
        .aspectSpec(entitySpec.getKeyAspectSpec())
        .recordTemplate(EntityKeyUtils.convertUrnToEntityKey(urn, entitySpec.getKeyAspectSpec()))
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build(mockAspectRetriever);
  }

  private List<MCPItem> applyTo(Urn urn) {
    return sideEffect
        .applyMCPSideEffect(
            mockOpContext, Collections.singletonList(keyItem(urn)), retrieverContext)
        .collect(Collectors.toList());
  }

  @Test
  public void testDerivesLowercasedUrnFromKeyAspect() {
    Urn urn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,DB.SCHEMA.TABLE,PROD)");

    List<MCPItem> results = applyTo(urn);

    assertEquals(results.size(), 1, "Expected a single aliases MCP");
    MCPItem out = results.get(0);
    assertEquals(out.getUrn(), urn);
    assertEquals(out.getAspectName(), ALIASES_ASPECT_NAME);
    Aliases aspect = out.getAspect(Aliases.class);
    assertNotNull(aspect);
    assertEquals(
        aspect.getLowercasedUrn().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");
  }

  @Test
  public void testSkipsUrnThatIsAlreadyItsOwnLowercasedForm() {
    // Uppercase env is deliberate: only the name is lowercased.
    Urn urn =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)");

    assertTrue(applyTo(urn).isEmpty(), "Expected no aliases MCP for an all-lowercase name");
  }
}
