package com.linkedin.datahub.graphql.resolvers.connection;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.ingest.source.IngestionSourceForEntityResolver;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.AssertionMonitorsConfiguration;
import com.linkedin.mxe.SystemMetadata;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSourceForEntityResolverTest {
  private static final String ENTITY_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test1,DEV)";
  private static final String INGESTION_SOURCE_URN_STRING = "urn:li:dataHubIngestionSource:test";
  private static final String EXECUTION_REQUEST_URN_STRING = "urn:li:dataHubExecutionRequest:test";
  private EntityClient _entityClient;
  private AssertionMonitorsConfiguration _assertionMonitorsConfiguration;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private IngestionSourceForEntityResolver _resolver;
  private Urn _entityUrn;
  private QueryContext _mockContext;
  private EntityResponse _entityResponse;
  private EntityResponse _executionRequestEntityResponse;
  private EntityResponse _ingestionSourceEntityResponse;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _assertionMonitorsConfiguration = mock(AssertionMonitorsConfiguration.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _resolver =
        new IngestionSourceForEntityResolver(_entityClient, _assertionMonitorsConfiguration);
    _mockContext = getMockAllowContext();

    try {
      _entityUrn = Urn.createFromString(ENTITY_URN_STRING);
    } catch (Exception e) {
      fail(e.toString());
    }

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        DATASET_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setSystemMetadata(
                new SystemMetadata().setRunId("real-id-1").setLastObserved(1659107340747L)));
    _entityResponse = new EntityResponse().setAspects(aspectMap);

    EnvelopedAspectMap executionRequestAspects = new EnvelopedAspectMap();
    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    execInput.setSource(
        new ExecutionRequestSource()
            .setType("INGESTION_SOURCE")
            .setIngestionSource(UrnUtils.getUrn(INGESTION_SOURCE_URN_STRING)));
    EnvelopedAspect executionRequestEnvelopedInput =
        new EnvelopedAspect().setValue(new Aspect(execInput.data()));
    executionRequestAspects.put(
        Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME, executionRequestEnvelopedInput);
    _executionRequestEntityResponse =
        new EntityResponse()
            .setUrn(UrnUtils.getUrn(EXECUTION_REQUEST_URN_STRING))
            .setAspects(executionRequestAspects);

    EnvelopedAspectMap ingestionInfoAspects = new EnvelopedAspectMap();
    DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo();
    ingestionSourceInfo.setType("bigquery");
    ingestionSourceInfo.setName("BigQuery");
    ingestionSourceInfo.setSchedule(
        new DataHubIngestionSourceSchedule()
            .setTimezone("America / Los Angeles")
            .setInterval("* * * * *"));
    ingestionSourceInfo.setConfig(
        new DataHubIngestionSourceConfig()
            .setExecutorId("default")
            .setVersion("v0.10.5")
            .setRecipe("{}"));
    EnvelopedAspect ingestionSourceEnvelopedInfo =
        new EnvelopedAspect().setValue(new Aspect(ingestionSourceInfo.data()));
    ingestionInfoAspects.put(Constants.INGESTION_INFO_ASPECT_NAME, ingestionSourceEnvelopedInfo);
    _ingestionSourceEntityResponse =
        new EntityResponse()
            .setUrn(UrnUtils.getUrn(INGESTION_SOURCE_URN_STRING))
            .setAspects(ingestionInfoAspects);

    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(ENTITY_URN_STRING);
  }

  @Test
  public void testFailsNullEntity() {
    when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(null);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testSuccess() throws Exception {
    when(_assertionMonitorsConfiguration.getResolveIngestionSourceForAspects())
        .thenReturn(
            String.format("%s,%s", SCHEMA_METADATA_ASPECT_NAME, DATASET_PROFILE_ASPECT_NAME));
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(_entityUrn.getEntityType()),
            eq(_entityUrn),
            eq(Set.of(SCHEMA_METADATA_ASPECT_NAME, DATASET_PROFILE_ASPECT_NAME))))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(_executionRequestEntityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            eq(UrnUtils.getUrn(INGESTION_SOURCE_URN_STRING)),
            eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(_ingestionSourceEntityResponse);

    assertEquals(
        _resolver.get(_dataFetchingEnvironment).join().getUrn(), INGESTION_SOURCE_URN_STRING);
  }

  @Test
  public void testFailsNoEntity() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(null);

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoRunId() throws Exception {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("blank-run-id", new EnvelopedAspect());

    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(new EntityResponse().setAspects(aspectMap));

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoExecutionRequestEntityResponse() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(null);

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoExecutionRequestAspects() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(new EntityResponse());

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsExecutionRequestAspectDoesNotContainExecutionRequestAspect()
      throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    EnvelopedAspectMap executionRequestAspects = new EnvelopedAspectMap();
    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    EnvelopedAspect executionRequestEnvelopedInput =
        new EnvelopedAspect().setValue(new Aspect(execInput.data()));
    executionRequestAspects.put(
        Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME, executionRequestEnvelopedInput);
    _executionRequestEntityResponse = new EntityResponse().setAspects(executionRequestAspects);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(_executionRequestEntityResponse);

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionSourceEntityResponse() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(_executionRequestEntityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            any(),
            eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(null);

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionInfoAspect() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(_executionRequestEntityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            any(),
            eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse());

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionSourceEnvelopedInfo() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(_executionRequestEntityResponse);

    EnvelopedAspectMap ingestionInfoAspects = new EnvelopedAspectMap();
    ingestionInfoAspects.put(Constants.INGESTION_INFO_ASPECT_NAME, new EnvelopedAspect());
    _ingestionSourceEntityResponse = new EntityResponse().setAspects(ingestionInfoAspects);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            any(),
            eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse());

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionSourceConfig() throws Exception {
    when(_entityClient.getV2(
            any(OperationContext.class), eq(_entityUrn.getEntityType()), eq(_entityUrn), isNull()))
        .thenReturn(_entityResponse);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
            any(),
            eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME))))
        .thenReturn(_executionRequestEntityResponse);

    EnvelopedAspectMap ingestionInfoAspects = new EnvelopedAspectMap();
    DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo();
    ingestionSourceInfo.setType("bigquery");
    EnvelopedAspect ingestionSourceEnvelopedInfo =
        new EnvelopedAspect().setValue(new Aspect(ingestionSourceInfo.data()));
    ingestionInfoAspects.put(Constants.INGESTION_INFO_ASPECT_NAME, ingestionSourceEnvelopedInfo);
    _ingestionSourceEntityResponse = new EntityResponse().setAspects(ingestionInfoAspects);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
            any(),
            eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME))))
        .thenReturn(new EntityResponse());

    assertNull(_resolver.get(_dataFetchingEnvironment).join());
  }
}
