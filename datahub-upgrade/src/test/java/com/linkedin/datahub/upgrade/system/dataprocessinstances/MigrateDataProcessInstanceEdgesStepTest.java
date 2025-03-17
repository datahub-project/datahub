package com.linkedin.datahub.upgrade.system.dataprocessinstances;

import static com.linkedin.metadata.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.linkedin.common.EdgeArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MigrateDataProcessInstanceEdgesStepTest {
  private static final String UPGRADE_ID = "MigrateDataProcessInstanceEdgesStep";
  private static final int BATCH_SIZE = 1000;
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final DataPlatformUrn inputPlatform = new DataPlatformUrn("input");
  private static final DataPlatformUrn outputAPlatform = new DataPlatformUrn("outputA");
  private static final DataPlatformUrn outputBPlatform = new DataPlatformUrn("outputB");
  private static final DataPlatformUrn parentPlatform = new DataPlatformUrn("parent");
  private static final DataPlatformUrn otherPlatform = new DataPlatformUrn("other");

  @Mock private OperationContext mockOpContext;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private EntityService<?> mockEntityService;
  @Mock private SearchService mockSearchService;
  @Mock private RetrieverContext mockRetrieverContext;

  private MigrateDataProcessInstanceEdgesStep step;
  private MigrateDataProcessInstanceEdgesStep reprocessStep;

  @BeforeTest
  public void setup() {
    MockitoAnnotations.openMocks(this);
    step =
        new MigrateDataProcessInstanceEdgesStep(
            mockOpContext,
            mockEntityService,
            mockSearchService,
            false,
            List.of(inputPlatform.toString()),
            List.of(outputAPlatform.toString(), outputBPlatform.toString()),
            List.of(parentPlatform.toString()),
            BATCH_SIZE);
    reprocessStep =
        new MigrateDataProcessInstanceEdgesStep(
            mockOpContext,
            mockEntityService,
            mockSearchService,
            true,
            List.of(inputPlatform.toString()),
            List.of(outputAPlatform.toString(), outputBPlatform.toString()),
            List.of(parentPlatform.toString()),
            BATCH_SIZE);
  }

  @Test
  public void testId() {
    assertEquals(UPGRADE_ID, step.id());
  }

  @Test
  public void testSkip() {
    when(mockEntityService.exists(
            any(),
            eq(BootstrapStep.getUpgradeUrn(UPGRADE_ID)),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            anyBoolean()))
        .thenReturn(true);
    assertTrue(step.skip(mockUpgradeContext));
    assertFalse(reprocessStep.skip(mockUpgradeContext));
  }

  @Test
  public void testNoSkip() {
    when(mockEntityService.exists(
            any(),
            eq(BootstrapStep.getUpgradeUrn(UPGRADE_ID)),
            eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME),
            anyBoolean()))
        .thenReturn(false);
    assertFalse(step.skip(mockUpgradeContext));
    assertFalse(reprocessStep.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutable() {
    // Create Mock Data
    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    Urn inputEntity = new DatasetUrn(inputPlatform, "i", FabricType.PROD);
    Urn outputAEntity = new DatasetUrn(outputAPlatform, "o1", FabricType.PROD);
    Urn outputBEntity = new MLModelUrn(outputBPlatform, "o2", FabricType.PROD);
    Urn otherEntity = new MLModelUrn(otherPlatform, "other", FabricType.PROD);

    Urn hasInput = UrnUtils.getUrn("urn:li:dataProcessInstance:hasInput");
    Urn hasOutput = UrnUtils.getUrn("urn:li:dataProcessInstance:hasOutput");
    Urn hasMultipleOutput = UrnUtils.getUrn("urn:li:dataProcessInstance:hasMultipleOutput");
    Urn hasAll = UrnUtils.getUrn("urn:li:dataProcessInstance:hasAll");
    List<Urn> dpiUrns = List.of(hasInput, hasOutput, hasMultipleOutput, hasAll);

    DataProcessInstanceInput hasInputInput =
        new DataProcessInstanceInput().setInputs(new UrnArray(List.of(inputEntity)));
    data.put(hasInput, List.of(hasInputInput));

    DataProcessInstanceOutput hasOutputOutput =
        new DataProcessInstanceOutput().setOutputs(new UrnArray(List.of(outputAEntity)));
    data.put(hasOutput, List.of(hasOutputOutput));

    DataProcessInstanceInput hasAllInput =
        new DataProcessInstanceInput().setInputs(new UrnArray(List.of(inputEntity, otherEntity)));
    DataProcessInstanceOutput hasAllOutput =
        new DataProcessInstanceOutput()
            .setOutputs(new UrnArray(List.of(outputAEntity, outputBEntity, otherEntity)));
    data.put(hasAll, List.of(hasAllInput, hasAllOutput));

    MockAspectRetriever mockAspectRetriever = new MockAspectRetriever(data);
    mockAspectRetriever.setEntityRegistry(TEST_REGISTRY);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
    when(mockOpContext.getEntityRegistry()).thenReturn(TEST_REGISTRY);

    when(mockSearchService.scrollAcrossEntities(
            any(),
            eq(List.of(DATA_PROCESS_INSTANCE_ENTITY_NAME)),
            eq("*"),
            eq(step.dataProcessInstancesInputOutputFilter()),
            any(),
            any(),
            any(),
            eq(BATCH_SIZE),
            any()))
        .thenReturn(
            new ScrollResult()
                .setNumEntities(dpiUrns.size())
                .setEntities(
                    new SearchEntityArray(
                        dpiUrns.stream().map(u -> new SearchEntity().setEntity(u)).toList())));

    // Run Test
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);
    assertEquals(DataHubUpgradeState.SUCCEEDED, result.result());

    verify(mockEntityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            argThat(
                arg -> {
                  List<MCPItem> mcps = arg.getMCPItems();

                  boolean hasInputCheck =
                      mcps.stream()
                          .anyMatch(
                              i -> {
                                if (i.getUrn().equals(hasInput)
                                    && i.getAspectName()
                                        .equals(DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME)) {
                                  EdgeArray edges =
                                      i.getAspect(DataProcessInstanceInput.class).getInputEdges();
                                  return edges.size() == 1
                                      && edges.get(0).getDestinationUrn().equals(inputEntity);
                                }
                                return false;
                              });

                  boolean hasOutputCheck =
                      mcps.stream()
                          .anyMatch(
                              i -> {
                                if (i.getUrn().equals(hasOutput)
                                    && i.getAspectName()
                                        .equals(DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME)) {
                                  EdgeArray edges =
                                      i.getAspect(DataProcessInstanceOutput.class).getOutputEdges();
                                  return edges.size() == 1
                                      && edges.get(0).getDestinationUrn().equals(outputAEntity);
                                }
                                return false;
                              });

                  boolean hasAllInputCheck =
                      mcps.stream()
                          .anyMatch(
                              i -> {
                                if (i.getUrn().equals(hasAll)
                                    && i.getAspectName()
                                        .equals(DATA_PROCESS_INSTANCE_INPUT_ASPECT_NAME)) {
                                  EdgeArray edges =
                                      i.getAspect(DataProcessInstanceInput.class).getInputEdges();
                                  return edges.size() == 2
                                      && edges.get(0).getDestinationUrn().equals(inputEntity)
                                      && edges.get(1).getDestinationUrn().equals(otherEntity);
                                }
                                return false;
                              });

                  boolean hasAllOutputCheck =
                      mcps.stream()
                          .anyMatch(
                              i -> {
                                if (i.getUrn().equals(hasAll)
                                    && i.getAspectName()
                                        .equals(DATA_PROCESS_INSTANCE_OUTPUT_ASPECT_NAME)) {
                                  EdgeArray edges =
                                      i.getAspect(DataProcessInstanceOutput.class).getOutputEdges();
                                  return edges.size() == 3
                                      && edges.get(0).getDestinationUrn().equals(outputAEntity)
                                      && edges.get(1).getDestinationUrn().equals(outputBEntity)
                                      && edges.get(2).getDestinationUrn().equals(otherEntity);
                                }
                                return false;
                              });

                  return hasInputCheck && hasOutputCheck && hasAllInputCheck && hasAllOutputCheck;
                }),
            eq(false));
  }
}
