package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.Operation;
import com.linkedin.common.OperationSourceType;
import com.linkedin.common.OperationType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.IntendedUserType;
import com.linkedin.datahub.graphql.generated.MLFeatureDataType;
import com.linkedin.datahub.graphql.generated.Notebook;
import com.linkedin.datahub.graphql.generated.PlatformType;
import com.linkedin.datahub.graphql.types.common.mappers.OperationMapper;
import com.linkedin.datahub.graphql.types.dataplatform.mappers.DataPlatformMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.IntendedUseMapper;
import com.linkedin.datahub.graphql.types.mlmodel.mappers.MLFeaturePropertiesMapper;
import com.linkedin.datahub.graphql.types.notebook.mappers.NotebookMapper;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.ml.metadata.IntendedUse;
import com.linkedin.ml.metadata.IntendedUserTypeArray;
import com.linkedin.ml.metadata.MLFeatureProperties;
import com.linkedin.notebook.NotebookCell;
import com.linkedin.notebook.NotebookCellArray;
import com.linkedin.notebook.NotebookCellType;
import com.linkedin.notebook.NotebookContent;
import com.linkedin.notebook.TextCell;
import java.util.List;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Mappers reading an enum value only a newer version defines (read after a rollback): with enum
 * tolerance on, an existing fallback, null, or leaving the value or its holder out; with it off,
 * they fail as before.
 */
public class NewerVersionEnumValuesMappersTest {

  private static final Urn FEATURE_URN =
      UrnUtils.getUrn("urn:li:mlFeature:(test-namespace,test-feature)");
  private static final String NEWER = "VALUE_FROM_NEWER_BUILD";

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testTolerantMapsToFallbackNullOrLeavesOut() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);

    com.linkedin.datahub.graphql.generated.Operation operation =
        OperationMapper.map(null, envelopedTimeseries(newerOperation()));
    assertEquals(
        operation.getOperationType(), com.linkedin.datahub.graphql.generated.OperationType.UNKNOWN);
    assertNull(operation.getSourceType());

    assertEquals(
        MLFeaturePropertiesMapper.map(null, newerFeatureProperties(), FEATURE_URN).getDataType(),
        MLFeatureDataType.UNKNOWN);

    assertEquals(
        IntendedUseMapper.map(null, newerIntendedUse()).getPrimaryUsers(),
        List.of(IntendedUserType.ENTERPRISE));

    DataPlatform platform = DataPlatformMapper.map(null, newerPlatform());
    assertNotNull(platform.getProperties());
    assertEquals(platform.getProperties().getType(), PlatformType.OTHERS);

    Notebook notebook = NotebookMapper.map(null, newerNotebook());
    assertEquals(notebook.getContent().getCells().size(), 1);
    assertEquals(notebook.getContent().getCells().get(0).getTextCell().getCellId(), "known");
  }

  @Test
  public void testStrictStillFails() {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);

    com.linkedin.metadata.aspect.EnvelopedAspect operation = envelopedTimeseries(newerOperation());
    assertThrows(IllegalArgumentException.class, () -> OperationMapper.map(null, operation));
    MLFeatureProperties featureProperties = newerFeatureProperties();
    assertThrows(
        IllegalArgumentException.class,
        () -> MLFeaturePropertiesMapper.map(null, featureProperties, FEATURE_URN));
    IntendedUse intendedUse = newerIntendedUse();
    assertThrows(IllegalArgumentException.class, () -> IntendedUseMapper.map(null, intendedUse));
    EntityResponse platform = newerPlatform();
    assertThrows(IllegalArgumentException.class, () -> DataPlatformMapper.map(null, platform));
    EntityResponse notebook = newerNotebook();
    assertThrows(IllegalArgumentException.class, () -> NotebookMapper.map(null, notebook));
  }

  private static Operation newerOperation() {
    Operation operation =
        new Operation()
            .setTimestampMillis(1L)
            .setLastUpdatedTimestamp(1L)
            .setOperationType(OperationType.INSERT)
            .setSourceType(OperationSourceType.DATA_PLATFORM);
    operation.data().put("operationType", NEWER);
    operation.data().put("sourceType", NEWER);
    return operation;
  }

  private static MLFeatureProperties newerFeatureProperties() {
    MLFeatureProperties properties =
        new MLFeatureProperties().setDataType(com.linkedin.common.MLFeatureDataType.NOMINAL);
    properties.data().put("dataType", NEWER);
    return properties;
  }

  private static IntendedUse newerIntendedUse() {
    IntendedUse intendedUse =
        new IntendedUse()
            .setPrimaryUsers(
                new IntendedUserTypeArray(
                    List.of(com.linkedin.ml.metadata.IntendedUserType.ENTERPRISE)));
    intendedUse.getPrimaryUsers().data().add(NEWER);
    return intendedUse;
  }

  private static EntityResponse newerPlatform() {
    DataPlatformInfo info =
        new DataPlatformInfo()
            .setName("test")
            .setType(com.linkedin.dataplatform.PlatformType.RELATIONAL_DB)
            .setDatasetNameDelimiter(".");
    info.data().put("type", NEWER);
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.DATA_PLATFORM_KEY_ASPECT_NAME,
        envelope(new DataPlatformKey().setPlatformName("test")));
    aspects.put(Constants.DATA_PLATFORM_INFO_ASPECT_NAME, envelope(info));
    return new EntityResponse()
        .setUrn(UrnUtils.getUrn("urn:li:dataPlatform:test"))
        .setEntityName(Constants.DATA_PLATFORM_ENTITY_NAME)
        .setAspects(aspects);
  }

  private static EntityResponse newerNotebook() {
    NotebookCell newer = textCell("newer");
    newer.data().put("type", NEWER);
    NotebookContent content =
        new NotebookContent().setCells(new NotebookCellArray(List.of(textCell("known"), newer)));
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.NOTEBOOK_CONTENT_ASPECT_NAME, envelope(content));
    return new EntityResponse()
        .setUrn(UrnUtils.getUrn("urn:li:notebook:(querybook,1)"))
        .setEntityName(Constants.NOTEBOOK_ENTITY_NAME)
        .setAspects(aspects);
  }

  private static NotebookCell textCell(String cellId) {
    AuditStamp stamp = new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:a"));
    return new NotebookCell()
        .setType(NotebookCellType.TEXT_CELL)
        .setTextCell(
            new TextCell()
                .setCellId(cellId)
                .setText("text")
                .setChangeAuditStamps(
                    new ChangeAuditStamps().setCreated(stamp).setLastModified(stamp)));
  }

  private static EnvelopedAspect envelope(RecordTemplate aspect) {
    return new EnvelopedAspect().setValue(new Aspect(aspect.data()));
  }

  /** A timeseries aspect as the timeseries service returns it, serialized. */
  private static com.linkedin.metadata.aspect.EnvelopedAspect envelopedTimeseries(
      RecordTemplate aspect) {
    return new com.linkedin.metadata.aspect.EnvelopedAspect()
        .setAspect(GenericRecordUtils.serializeAspect(aspect));
  }
}
