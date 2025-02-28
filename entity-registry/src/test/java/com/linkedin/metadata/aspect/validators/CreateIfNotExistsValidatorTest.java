package com.linkedin.metadata.aspect.validators;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class CreateIfNotExistsValidatorTest {
  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;

  private static final AspectPluginConfig validatorConfig =
      AspectPluginConfig.builder()
          .supportedOperations(List.of("CREATE", "CREATE_ENTITY"))
          .className(CreateIfNotExistsValidator.class.getName())
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .enabled(true)
          .build();

  @BeforeTest
  public void init() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testCreateIfEntityNotExistsSuccess() {
    CreateIfNotExistsValidator test = new CreateIfNotExistsValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    Set<AspectValidationException> exceptions =
        test.validatePreCommit(
                List.of(
                    // Request aspect
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE_ENTITY)
                        .urn(testEntityUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testEntityUrn.getEntityType())
                                .getAspectSpec("status"))
                        .recordTemplate(new Status().setRemoved(false))
                        .build(),
                    // Required key aspect to indicate non-entity existence
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE)
                        .urn(testEntityUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testEntityUrn.getEntityType())
                                .getKeyAspectSpec())
                        .recordTemplate(new ChartKey().setChartId("looker,baz1"))
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toSet());

    assertEquals(Set.of(), exceptions);
  }

  @Test
  public void testCreateIfEntityNotExistsFail() {
    CreateIfNotExistsValidator test = new CreateIfNotExistsValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    ChangeMCP testItem =
        TestMCP.builder()
            .changeType(ChangeType.CREATE_ENTITY)
            .urn(testEntityUrn)
            .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
            .aspectSpec(
                entityRegistry.getEntitySpec(testEntityUrn.getEntityType()).getAspectSpec("status"))
            .recordTemplate(new Status().setRemoved(false))
            .build();

    // missing key aspect
    Set<AspectValidationException> exceptions =
        test.validatePreCommit(List.of(testItem), mockRetrieverContext).collect(Collectors.toSet());

    assertEquals(
        exceptions,
        Set.of(
            AspectValidationException.forItem(
                testItem,
                "Cannot perform CREATE_ENTITY if not exists since the entity key already exists.")));
  }

  @Test
  public void testCreateIfNotExistsSuccess() {
    CreateIfNotExistsValidator test = new CreateIfNotExistsValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    Set<AspectValidationException> exceptions =
        test.validatePreCommit(
                List.of(
                    TestMCP.builder()
                        .changeType(ChangeType.CREATE)
                        .urn(testEntityUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testEntityUrn.getEntityType())
                                .getAspectSpec("status"))
                        .recordTemplate(new Status().setRemoved(false))
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toSet());

    assertEquals(Set.of(), exceptions);
  }

  @Test
  public void testCreateIfNotExistsFail() {
    CreateIfNotExistsValidator test = new CreateIfNotExistsValidator().setConfig(validatorConfig);
    Urn testEntityUrn = UrnUtils.getUrn("urn:li:chart:(looker,baz1)");

    SystemAspect mockSystemAspect = mock(SystemAspect.class);
    when(mockSystemAspect.getRecordTemplate()).thenReturn(new Status().setRemoved(true));

    TestMCP testItem =
        TestMCP.builder()
            .changeType(ChangeType.CREATE)
            .urn(testEntityUrn)
            .entitySpec(entityRegistry.getEntitySpec(testEntityUrn.getEntityType()))
            .aspectSpec(
                entityRegistry.getEntitySpec(testEntityUrn.getEntityType()).getAspectSpec("status"))
            .recordTemplate(new Status().setRemoved(false))
            .previousSystemAspect(mockSystemAspect)
            .build();

    Set<AspectValidationException> exceptions =
        test.validatePreCommit(List.of(testItem), mockRetrieverContext).collect(Collectors.toSet());

    assertEquals(
        exceptions,
        Set.of(
            AspectValidationException.forItem(
                testItem, "Cannot perform CREATE since the aspect already exists.")));
  }
}
