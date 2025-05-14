package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.validation.UserDeleteValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UserDeleteValidatorTest {

  private static final Urn TEST_USER_URN;
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(UserDeleteValidator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("DELETE"))
          .supportedEntityAspectNames(
              List.of(new AspectPluginConfig.EntityAspectName("corpuser", "*")))
          .build();

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private AspectRetriever mockAspectRetriever;

  private EntityRegistry entityRegistry;

  private UserDeleteValidator validator;

  static {
    try {
      TEST_USER_URN = Urn.createFromString("urn:li:corpuser:user");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    entityRegistry = new TestEntityRegistry();
    validator = new UserDeleteValidator();
    validator.setConfig(TEST_PLUGIN_CONFIG);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
  }

  @Test
  public void testAllowed() {
    final CorpUserInfo corpUserInfo = new CorpUserInfo();

    when(mockAspectRetriever.getLatestAspectObject(TEST_USER_URN, CORP_USER_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(corpUserInfo.data()));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_INFO_ASPECT_NAME))
                        .recordTemplate(corpUserInfo)
                        .build()),
                mockRetrieverContext)
            .count(),
        0,
        "Expected deletes on a corpUser that are implicitly non-system to be allowed");

    corpUserInfo.setSystem(false);

    when(mockAspectRetriever.getLatestAspectObject(TEST_USER_URN, CORP_USER_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(corpUserInfo.data()));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_INFO_ASPECT_NAME))
                        .recordTemplate(corpUserInfo)
                        .build()),
                mockRetrieverContext)
            .count(),
        0,
        "Expected deletes on a corpUser that are explicitly non-system to be allowed");
  }

  @Test
  public void testDenied() {
    final CorpUserInfo corpUserInfo = new CorpUserInfo();

    corpUserInfo.setSystem(true);

    when(mockAspectRetriever.getLatestAspectObject(TEST_USER_URN, CORP_USER_INFO_ASPECT_NAME))
        .thenReturn(new Aspect(corpUserInfo.data()));

    assertEquals(
        validator
            .validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(TEST_USER_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_USER_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_USER_URN.getEntityType())
                                .getAspectSpec(CORP_USER_INFO_ASPECT_NAME))
                        .recordTemplate(corpUserInfo)
                        .build()),
                mockRetrieverContext)
            .count(),
        1,
        "Expected deletes on a system corpUser to be rejected");
  }
}
