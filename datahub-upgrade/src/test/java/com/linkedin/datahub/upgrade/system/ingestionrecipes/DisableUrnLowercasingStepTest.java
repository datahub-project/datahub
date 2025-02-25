package com.linkedin.datahub.upgrade.system.ingestionrecipes;

import static com.linkedin.metadata.Constants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Collections;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

class DisableUrnLowercasingStepTest {

  @Mock private OperationContext mockOpContext;

  @Mock private EntityService<?> mockEntityService;

  @Mock private RetrieverContext mockRetrieverContext;

  @Mock private UpgradeContext upgradeContext;

  private DisableUrnLowercasingStep step;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    step = new DisableUrnLowercasingStep(mockOpContext, mockEntityService);
    when(mockOpContext.getRetrieverContext()).thenReturn(mockRetrieverContext);
  }

  @Test
  public void testDisableUrnLowercasing() throws Exception {
    String jsonString = "{\"source\":{\"config\":{\"job_id\":true}}}";
    String expectedJsonString =
        "{\"source\":{\"config\":{\"job_id\":true,\"convert_urns_to_lowercase\":false}}}";
    String updatedJsonString = "";
    DataHubIngestionSourceInfo dataHubIngestionSourceInfo = new DataHubIngestionSourceInfo();

    dataHubIngestionSourceInfo.setType("bigquery");
    dataHubIngestionSourceInfo.setConfig(
        new DataHubIngestionSourceConfig().setExecutorId("default").setRecipe(jsonString));
    ListResultMetadata listResultMetadata = new ListResultMetadata();
    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setUrn(Urn.createFromString("urn:li:dataset:1"));
    listResultMetadata.setExtraInfos(new ExtraInfoArray(Collections.singletonList(extraInfo)));
    ListResult<RecordTemplate> listResult =
        new ListResult<>(
            Collections.singletonList(dataHubIngestionSourceInfo),
            listResultMetadata,
            0,
            false,
            1,
            1,
            100);

    when(mockEntityService.listLatestAspects(
            any(OperationContext.class),
            eq(INGESTION_SOURCE_ENTITY_NAME),
            eq(INGESTION_INFO_ASPECT_NAME),
            eq(0),
            eq(100)))
        .thenReturn(listResult);

    step.executable().apply(upgradeContext);
    ArgumentCaptor<MetadataChangeProposal> mcpArgumentCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    verify(mockEntityService, times(2))
        .ingestProposal(any(), mcpArgumentCaptor.capture(), any(), eq(false));
    MetadataChangeProposal mcp = mcpArgumentCaptor.getAllValues().get(0);
    DataHubIngestionSourceInfo aspect =
        GenericRecordUtils.deserializeAspect(
            mcp.getAspect().getValue(),
            mcp.getAspect().getContentType(),
            DataHubIngestionSourceInfo.class);
    assertEquals(expectedJsonString, aspect.getConfig().getRecipe());
  }

  @Test
  public void testDisableUrnLowercasingShouldNotTouchExistingValue() throws Exception {
    String jsonString =
        "{\"source\":{\"config\":{\"job_id\":true,\"convert_urns_to_lowercase\":true}}}";
    String expectedJsonString =
        "{\"source\":{\"config\":{\"job_id\":true,\"convert_urns_to_lowercase\":true}}}";
    String updatedJsonString = "";
    DataHubIngestionSourceInfo dataHubIngestionSourceInfo = new DataHubIngestionSourceInfo();

    dataHubIngestionSourceInfo.setType("bigquery");
    dataHubIngestionSourceInfo.setConfig(
        new DataHubIngestionSourceConfig().setExecutorId("default").setRecipe(jsonString));
    ListResultMetadata listResultMetadata = new ListResultMetadata();
    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setUrn(Urn.createFromString("urn:li:dataset:1"));
    listResultMetadata.setExtraInfos(new ExtraInfoArray(Collections.singletonList(extraInfo)));
    ListResult<RecordTemplate> listResult =
        new ListResult<>(
            Collections.singletonList(dataHubIngestionSourceInfo),
            listResultMetadata,
            0,
            false,
            1,
            1,
            100);

    when(mockEntityService.listLatestAspects(
            any(OperationContext.class),
            eq(INGESTION_SOURCE_ENTITY_NAME),
            eq(INGESTION_INFO_ASPECT_NAME),
            eq(0),
            eq(100)))
        .thenReturn(listResult);

    step.executable().apply(upgradeContext);
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), eq(false));

    // verify(mockEntityService, never()).ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testDisableUrnLowercasingShouldNotTouchExistingValueEvenIfItFalse() throws Exception {
    String jsonString =
        "{\"source\":{\"config\":{\"job_id\":true,\"convert_urns_to_lowercase\":false}}}";
    String expectedJsonString =
        "{\"source\":{\"config\":{\"job_id\":true,\"convert_urns_to_lowercase\":false}}}";
    String updatedJsonString = "";
    DataHubIngestionSourceInfo dataHubIngestionSourceInfo = new DataHubIngestionSourceInfo();

    dataHubIngestionSourceInfo.setType("bigquery");
    dataHubIngestionSourceInfo.setConfig(
        new DataHubIngestionSourceConfig().setExecutorId("default").setRecipe(jsonString));
    ListResultMetadata listResultMetadata = new ListResultMetadata();
    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setUrn(Urn.createFromString("urn:li:dataset:1"));
    listResultMetadata.setExtraInfos(new ExtraInfoArray(Collections.singletonList(extraInfo)));
    ListResult<RecordTemplate> listResult =
        new ListResult<>(
            Collections.singletonList(dataHubIngestionSourceInfo),
            listResultMetadata,
            0,
            false,
            1,
            1,
            100);

    when(mockEntityService.listLatestAspects(
            any(OperationContext.class),
            eq(INGESTION_SOURCE_ENTITY_NAME),
            eq(INGESTION_INFO_ASPECT_NAME),
            eq(0),
            eq(100)))
        .thenReturn(listResult);

    step.executable().apply(upgradeContext);
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testDisableUrnLowercasingShouldNotTouchUnsupportedRecipeTypes() throws Exception {
    String jsonString = "{\"source\":{\"config\":{\"job_id\":true}}}";
    String expectedJsonString = "{\"source\":{\"config\":{\"job_id\":true}}}";
    DataHubIngestionSourceInfo dataHubIngestionSourceInfo = new DataHubIngestionSourceInfo();

    dataHubIngestionSourceInfo.setType("tableau");
    dataHubIngestionSourceInfo.setConfig(
        new DataHubIngestionSourceConfig().setExecutorId("default").setRecipe(jsonString));
    ListResultMetadata listResultMetadata = new ListResultMetadata();
    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setUrn(Urn.createFromString("urn:li:dataset:1"));
    listResultMetadata.setExtraInfos(new ExtraInfoArray(Collections.singletonList(extraInfo)));
    ListResult<RecordTemplate> listResult =
        new ListResult<>(
            Collections.singletonList(dataHubIngestionSourceInfo),
            listResultMetadata,
            0,
            false,
            1,
            1,
            100);

    when(mockEntityService.listLatestAspects(
            any(OperationContext.class),
            eq(INGESTION_SOURCE_ENTITY_NAME),
            eq(INGESTION_INFO_ASPECT_NAME),
            eq(0),
            eq(100)))
        .thenReturn(listResult);

    step.executable().apply(upgradeContext);
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testDisableUrnLowercasingShouldNotTouchCliExecutor() throws Exception {
    String jsonString = "{\"source\":{\"config\":{\"job_id\":true}}}";
    String expectedJsonString = "{\"source\":{\"config\":{\"job_id\":true}}}";
    DataHubIngestionSourceInfo dataHubIngestionSourceInfo = new DataHubIngestionSourceInfo();

    dataHubIngestionSourceInfo.setType("bigquery");
    dataHubIngestionSourceInfo.setConfig(
        new DataHubIngestionSourceConfig().setExecutorId("__datahub_cli_").setRecipe(jsonString));
    ListResultMetadata listResultMetadata = new ListResultMetadata();
    ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setUrn(Urn.createFromString("urn:li:dataset:1"));
    listResultMetadata.setExtraInfos(new ExtraInfoArray(Collections.singletonList(extraInfo)));
    ListResult<RecordTemplate> listResult =
        new ListResult<>(
            Collections.singletonList(dataHubIngestionSourceInfo),
            listResultMetadata,
            0,
            false,
            1,
            1,
            100);

    when(mockEntityService.listLatestAspects(
            any(OperationContext.class),
            eq(INGESTION_SOURCE_ENTITY_NAME),
            eq(INGESTION_INFO_ASPECT_NAME),
            eq(0),
            eq(100)))
        .thenReturn(listResult);

    step.executable().apply(upgradeContext);
    verify(mockEntityService, times(1)).ingestProposal(any(), any(), any(), anyBoolean());
  }
}
