package com.linkedin.datahub.graphql.resolvers.connection;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.mxe.SystemMetadata;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Collections;
import com.linkedin.metadata.Constants;
import static com.linkedin.metadata.Constants.*;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class ConnectionForEntityExistsResolverTest {
  private static final String ENTITY_URN_STRING = "urn:li:dataset:(urn:li:dataPlatform:bigquery,test1,DEV)";

  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private ConnectionForEntityExistsResolver _resolver;
  private Urn _entityUrn;
  private QueryContext _mockContext;
  private EntityResponse _entityResponse;
  private EntityResponse _executionRequestEntityResponse;
  private EntityResponse _ingestionSourceEntityResponse;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _resolver = new ConnectionForEntityExistsResolver(_entityClient);
    _mockContext = getMockAllowContext();

    try {
      _entityUrn = Urn.createFromString(ENTITY_URN_STRING);
    } catch (Exception e) {
      fail(e.toString());
    }

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("real-run-id", new EnvelopedAspect().setSystemMetadata(
        new SystemMetadata().setRunId("real-id-1").setLastObserved(1659107340747L)
    ));
    _entityResponse = new EntityResponse()
        .setAspects(aspectMap);

    EnvelopedAspectMap executionRequestAspects = new EnvelopedAspectMap();
    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    execInput.setSource(new ExecutionRequestSource().setType("test-connection"));
    EnvelopedAspect executionRequestEnvelopedInput = new EnvelopedAspect().setValue(new Aspect(execInput.data()));
    executionRequestAspects.put(
      Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME, executionRequestEnvelopedInput
    );
    _executionRequestEntityResponse = new EntityResponse()
        .setAspects(executionRequestAspects);

    EnvelopedAspectMap ingestionInfoAspects = new EnvelopedAspectMap();
    DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo();
    ingestionSourceInfo.setConfig(new DataHubIngestionSourceConfig()
        .setExecutorId("default")
    );
    EnvelopedAspect ingestionSourceEnvelopedInfo = new EnvelopedAspect().setValue(new Aspect(ingestionSourceInfo.data()));
    ingestionInfoAspects.put(
      Constants.INGESTION_INFO_ASPECT_NAME, ingestionSourceEnvelopedInfo
    );
    _ingestionSourceEntityResponse = new EntityResponse()
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
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_executionRequestEntityResponse);

    when(_entityClient.getV2(
      eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
      any(),
      eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_ingestionSourceEntityResponse);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoEntity() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(null);

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoRunId() throws Exception {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("blank-run-id", new EnvelopedAspect());

    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(
      new EntityResponse()
        .setAspects(aspectMap)
    );

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoExecutionRequestEntityResponse() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(null);

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoExecutionRequestAspects() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(new EntityResponse());

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsExecutionRequestAspectDoesNotContainExecutionRequestAspect() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    EnvelopedAspectMap executionRequestAspects = new EnvelopedAspectMap();
    final ExecutionRequestInput execInput = new ExecutionRequestInput();
    EnvelopedAspect executionRequestEnvelopedInput = new EnvelopedAspect().setValue(new Aspect(execInput.data()));
    executionRequestAspects.put(
      Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME, executionRequestEnvelopedInput
    );
    _executionRequestEntityResponse = new EntityResponse()
        .setAspects(executionRequestAspects);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_executionRequestEntityResponse);

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionSourceEntityResponse() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_executionRequestEntityResponse);

    when(_entityClient.getV2(
      eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
      any(),
      eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(null);

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionInfoAspect() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_executionRequestEntityResponse);

    when(_entityClient.getV2(
      eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
      any(),
      eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(new EntityResponse());

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionSourceEnvelopedInfo() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_executionRequestEntityResponse);

    EnvelopedAspectMap ingestionInfoAspects = new EnvelopedAspectMap();
    ingestionInfoAspects.put(
      Constants.INGESTION_INFO_ASPECT_NAME, new EnvelopedAspect()
    );
    _ingestionSourceEntityResponse = new EntityResponse()
        .setAspects(ingestionInfoAspects);

    when(_entityClient.getV2(
      eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
      any(),
      eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(new EntityResponse());

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testFailsNoIngestionSourceConfig() throws Exception {
    when(_entityClient.getV2(
      eq(_entityUrn.getEntityType()),
      eq(_entityUrn),
      isNull(),
      any(Authentication.class)
    )).thenReturn(_entityResponse);

    when(_entityClient.getV2(
      eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
      any(),
      eq(Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(_executionRequestEntityResponse);

    EnvelopedAspectMap ingestionInfoAspects = new EnvelopedAspectMap();
    DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo();
    EnvelopedAspect ingestionSourceEnvelopedInfo = new EnvelopedAspect().setValue(new Aspect(ingestionSourceInfo.data()));
    ingestionInfoAspects.put(
      Constants.INGESTION_INFO_ASPECT_NAME, ingestionSourceEnvelopedInfo
    );
    _ingestionSourceEntityResponse = new EntityResponse()
        .setAspects(ingestionInfoAspects);

    when(_entityClient.getV2(
      eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
      any(),
      eq(Collections.singleton(INGESTION_INFO_ASPECT_NAME)),
      any(Authentication.class)
    )).thenReturn(new EntityResponse());

    assertFalse(_resolver.get(_dataFetchingEnvironment).join());
  }
}
