package com.linkedin.datahub.upgrade.system.cron;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class TweakReplicasStepTest {

  @Mock private ElasticSearchIndexed mockService;

  @Mock private UpgradeContext mockContext;

  @Mock private Urn mockUrn;

  @Mock private StructuredPropertyDefinition mockPropertyDef;

  private List<ElasticSearchIndexed> services;
  private Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties;
  private TweakReplicasStep tweakReplicasStep;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);

    services = new ArrayList<>();
    services.add(mockService);

    structuredProperties = new HashSet<>();
    structuredProperties.add(new Pair<>(mockUrn, mockPropertyDef));

    tweakReplicasStep = new TweakReplicasStep(services, structuredProperties);
  }

  @Test
  public void testId() {
    Assertions.assertEquals("TweakReplicasStep", tweakReplicasStep.id());
  }

  @Test
  public void testCreateArgsWithDryRunTrue() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("dryRun", Optional.of("true"));

    Mockito.when(mockContext.parsedArgs()).thenReturn(parsedArgs);

    CronArgs args = tweakReplicasStep.createArgs(mockContext);

    Assertions.assertNotNull(args);
    Assertions.assertTrue(args.dryRun);
  }

  @Test
  public void testCreateArgsWithDryRunFalse() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("dryRun", Optional.of("false"));

    Mockito.when(mockContext.parsedArgs()).thenReturn(parsedArgs);

    CronArgs args = tweakReplicasStep.createArgs(mockContext);

    Assertions.assertNotNull(args);
    Assertions.assertFalse(args.dryRun);
  }

  @Test
  public void testCreateArgsWithNoDryRunArg() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();

    Mockito.when(mockContext.parsedArgs()).thenReturn(parsedArgs);

    CronArgs args = tweakReplicasStep.createArgs(mockContext);

    Assertions.assertNotNull(args);
    Assertions.assertFalse(args.dryRun);
  }

  @Test
  public void testCreateArgsWithExistingArgs() {
    CronArgs existingArgs = new CronArgs();
    existingArgs.dryRun = true;

    tweakReplicasStep = new TweakReplicasStep(services, structuredProperties);
    tweakReplicasStep.args = existingArgs;

    CronArgs args = tweakReplicasStep.createArgs(mockContext);

    Assertions.assertNotNull(args);
    Assertions.assertEquals(existingArgs, args);
    Assertions.assertTrue(args.dryRun);

    // Verify that context.parsedArgs() was not called
    Mockito.verify(mockContext, Mockito.never()).parsedArgs();
  }

  @Test
  public void testExecutableSuccess() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    parsedArgs.put("dryRun", Optional.of("true"));

    Mockito.when(mockContext.parsedArgs()).thenReturn(parsedArgs);

    UpgradeStepResult result = tweakReplicasStep.executable().apply(mockContext);

    Assertions.assertNotNull(result);
    assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());
    assertEquals("TweakReplicasStep", result.stepId());

    // Verify that tweakReplicasAll was called with the correct parameters
    Mockito.verify(mockService)
        .tweakReplicasAll(ArgumentMatchers.eq(structuredProperties), ArgumentMatchers.eq(true));
  }

  @Test
  public void testExecutableWithException() {
    Map<String, Optional<String>> parsedArgs = new HashMap<>();

    Mockito.when(mockContext.parsedArgs()).thenReturn(parsedArgs);
    Mockito.doThrow(new RuntimeException("Test exception"))
        .when(mockService)
        .tweakReplicasAll(ArgumentMatchers.any(), ArgumentMatchers.anyBoolean());

    UpgradeStepResult result = tweakReplicasStep.executable().apply(mockContext);

    Assertions.assertNotNull(result);
    assertEquals(DataHubUpgradeState.FAILED, result.result());
    assertEquals("TweakReplicasStep", result.stepId());
  }

  @Test
  public void testExecutableWithMultipleServices() {
    ElasticSearchIndexed mockService2 = Mockito.mock(ElasticSearchIndexed.class);
    services.add(mockService2);

    tweakReplicasStep = new TweakReplicasStep(services, structuredProperties);

    Map<String, Optional<String>> parsedArgs = new HashMap<>();
    Mockito.when(mockContext.parsedArgs()).thenReturn(parsedArgs);

    UpgradeStepResult result = tweakReplicasStep.executable().apply(mockContext);

    Assertions.assertNotNull(result);
    assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());

    // Verify that tweakReplicasAll was called on both services
    Mockito.verify(mockService)
        .tweakReplicasAll(ArgumentMatchers.eq(structuredProperties), ArgumentMatchers.eq(false));
    Mockito.verify(mockService2)
        .tweakReplicasAll(ArgumentMatchers.eq(structuredProperties), ArgumentMatchers.eq(false));
  }
}
