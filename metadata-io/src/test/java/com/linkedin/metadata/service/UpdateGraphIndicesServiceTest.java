package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.ArgumentCaptor;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.Script;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class UpdateGraphIndicesServiceTest {
  private static final Urn TEST_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.adoption.pet_profiles,PROD)");
  private static final OperationContext TEST_OP_CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private UpdateGraphIndicesService test;
  private ESBulkProcessor mockESBulkProcessor;
  private ESGraphWriteDAO mockWriteDAO;
  private ESGraphQueryDAO mockReadDAO;

  @BeforeTest
  public void beforeTest() {
    EntityRegistry entityRegistry = new TestEntityRegistry();
    mockESBulkProcessor = mock(ESBulkProcessor.class);
    mockWriteDAO = mock(ESGraphWriteDAO.class);
    mockReadDAO = mock(ESGraphQueryDAO.class);

    test =
        new UpdateGraphIndicesService(
            new ElasticSearchGraphService(
                new LineageRegistry(entityRegistry),
                mockESBulkProcessor,
                IndexConventionImpl.noPrefix("md5"),
                mockWriteDAO,
                mockReadDAO,
                mock(ESIndexBuilder.class),
                "md5"));
  }

  @BeforeMethod
  public void beforeMethod() {
    reset(mockESBulkProcessor, mockWriteDAO, mockReadDAO);
  }

  @Test
  public void testStatusDeleteEvent() {
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.DELETE)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME));

    ArgumentCaptor<Script> scriptCaptor = ArgumentCaptor.forClass(Script.class);
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));

    scriptCaptor
        .getAllValues()
        .forEach(
            script ->
                assertEquals(
                    script.getParams().get("newValue").toString(),
                    "false",
                    "Status delete implies setting the removed status to false"));
  }

  @Test
  public void testStatusUpdateEvent() {
    GenericAspect removedTrue = GenericRecordUtils.serializeAspect(new Status().setRemoved(true));
    GenericAspect removedFalse = GenericRecordUtils.serializeAspect(new Status().setRemoved(false));

    // 1. no previous version
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setAspect(removedTrue));
    ArgumentCaptor<Script> scriptCaptor = ArgumentCaptor.forClass(Script.class);
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));
    scriptCaptor
        .getAllValues()
        .forEach(script -> assertEquals(script.getParams().get("newValue").toString(), "true"));

    // 2. differing previous version
    reset(mockWriteDAO);
    scriptCaptor = ArgumentCaptor.forClass(Script.class);
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setPreviousAspectValue(removedTrue)
            .setAspect(removedFalse));
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));
    scriptCaptor
        .getAllValues()
        .forEach(script -> assertEquals(script.getParams().get("newValue").toString(), "false"));

    // 3. RESTATE with no difference
    reset(mockWriteDAO);
    scriptCaptor = ArgumentCaptor.forClass(Script.class);
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.RESTATE)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setPreviousAspectValue(removedTrue)
            .setAspect(removedTrue));
    verify(mockWriteDAO, times(EdgeUrnType.values().length))
        .updateByQuery(scriptCaptor.capture(), any(QueryBuilder.class));
    scriptCaptor
        .getAllValues()
        .forEach(script -> assertEquals(script.getParams().get("newValue").toString(), "true"));
  }

  @Test
  public void testStatusNoOpEvent() {
    // 1. non status aspect
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.DATASET_PROPERTIES_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(new DatasetProperties())));

    // 2. no change in status
    GenericAspect statusAspect = GenericRecordUtils.serializeAspect(new Status().setRemoved(true));
    test.handleChangeEvent(
        TEST_OP_CONTEXT,
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityType("dataset")
            .setEntityUrn(TEST_URN)
            .setAspectName(Constants.STATUS_ASPECT_NAME)
            .setPreviousAspectValue(statusAspect)
            .setAspect(statusAspect));

    verifyNoInteractions(mockWriteDAO);
  }
}
