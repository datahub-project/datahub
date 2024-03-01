package com.datahub.event.hook;

import static com.datahub.event.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemRestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.service.BusinessAttributeUpdateService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.Parameters;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BusinessAttributeUpdateHookTest {

  private static final String TEST_BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:12668aea-009b-400e-8408-e661c3a230dd";
  private static final String EDITABLE_SCHEMAFIELD_WITH_BUSINESS_ATTRIBUTE =
      "EditableSchemaFieldWithBusinessAttribute";
  private static final Urn datasetUrn = UrnUtils.toDatasetUrn("hive", "test", "DEV");
  private static final String SUB_RESOURCE = "name";
  private static final String TAG_NAME = "test";
  private static final long EVENT_TIME = 123L;
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final String IsPartOfRelationship = "IsPartOf";
  private static Urn actorUrn;

  private static SystemRestliEntityClient _mockClient;

  private GraphService _mockGraphService;
  private EntityService _mockEntityService;
  private BusinessAttributeUpdateHook _businessAttributeUpdateHook;
  private BusinessAttributeUpdateService _businessAttributeServiceHook;

  @BeforeMethod
  public void setupTest() throws URISyntaxException {
    _mockGraphService = Mockito.mock(GraphService.class);
    _mockEntityService = Mockito.mock(EntityService.class);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    _mockClient = Mockito.mock(SystemRestliEntityClient.class);
    _businessAttributeServiceHook =
        new BusinessAttributeUpdateService(_mockGraphService, _mockEntityService, ENTITY_REGISTRY);
    _businessAttributeUpdateHook = new BusinessAttributeUpdateHook(_businessAttributeServiceHook);
  }

  @Test
  public void testMCLOnBusinessAttributeUpdate() throws Exception {
    PlatformEvent platformEvent = createPlatformEventBusinessAttribute();
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(
            0,
            1,
            1,
            ImmutableList.of(new RelatedEntity(IsPartOfRelationship, datasetUrn.toString())));
    // mock response
    Mockito.when(
            _mockGraphService.findRelatedEntities(
                null,
                newFilter("urn", TEST_BUSINESS_ATTRIBUTE_URN),
                null,
                EMPTY_FILTER,
                Arrays.asList(EDITABLE_SCHEMAFIELD_WITH_BUSINESS_ATTRIBUTE),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
                0,
                100000))
        .thenReturn(mockRelatedEntities);
    assertEquals(mockRelatedEntities.getTotal(), 1);

    // mock response
    Map<Urn, EntityResponse> datasetEntityResponse = datasetEntityResponses();
    Mockito.when(
            _mockEntityService.getEntitiesV2(
                Constants.DATASET_ENTITY_NAME,
                new HashSet<>(Collections.singleton(datasetUrn)),
                Collections.singleton(Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(datasetEntityResponse);
    assertEquals(datasetEntityResponse.size(), 1);

    // mock response
    Mockito.when(
            _mockEntityService.alwaysProduceMCLAsync(
                Mockito.any(Urn.class),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(AspectSpec.class),
                Mockito.eq(null),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(ChangeType.class)))
        .thenReturn(Pair.of(Mockito.mock(Future.class), false));

    // invoke
    _businessAttributeServiceHook.handleChangeEvent(platformEvent);

    // verify
    Mockito.verify(_mockGraphService, Mockito.times(1))
        .findRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  @Test
  private void testMCLOnNonBusinessAttributeUpdate() {
    PlatformEvent platformEvent = createBasePlatformEventDataset();

    // invoke
    _businessAttributeServiceHook.handleChangeEvent(platformEvent);

    // verify
    Mockito.verify(_mockGraphService, Mockito.times(0))
        .findRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  @Test
  private void testMCLOnInvalidCategory() throws Exception {
    PlatformEvent platformEvent = createPlatformEventInvalidCategory();

    // invoke
    _businessAttributeServiceHook.handleChangeEvent(platformEvent);

    // verify
    Mockito.verify(_mockGraphService, Mockito.times(0))
        .findRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());
  }

  public static PlatformEvent createPlatformEventBusinessAttribute() throws Exception {
    final GlobalTags newTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn(TAG_NAME);
    newTags.setTags(
        new TagAssociationArray(ImmutableList.of(new TagAssociation().setTag(newTagUrn))));
    PlatformEvent platformEvent =
        createChangeEvent(
            Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
            Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN),
            ChangeCategory.TAG,
            ChangeOperation.ADD,
            newTagUrn.toString(),
            ImmutableMap.of("tagUrn", newTagUrn.toString()),
            actorUrn);
    return platformEvent;
  }

  public static PlatformEvent createBasePlatformEventDataset() {
    final GlobalTags newTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn(TAG_NAME);
    newTags.setTags(
        new TagAssociationArray(ImmutableList.of(new TagAssociation().setTag(newTagUrn))));
    PlatformEvent platformEvent =
        createChangeEvent(
            Constants.DATASET_ENTITY_NAME,
            datasetUrn,
            ChangeCategory.TAG,
            ChangeOperation.ADD,
            newTagUrn.toString(),
            ImmutableMap.of("tagUrn", newTagUrn.toString()),
            actorUrn);
    return platformEvent;
  }

  public static PlatformEvent createPlatformEventInvalidCategory() throws Exception {
    final GlobalTags newTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn(TAG_NAME);
    newTags.setTags(
        new TagAssociationArray(ImmutableList.of(new TagAssociation().setTag(newTagUrn))));
    PlatformEvent platformEvent =
        createChangeEvent(
            Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
            Urn.createFromString(TEST_BUSINESS_ATTRIBUTE_URN),
            ChangeCategory.DOMAIN,
            ChangeOperation.ADD,
            newTagUrn.toString(),
            ImmutableMap.of("tagUrn", newTagUrn.toString()),
            actorUrn);
    return platformEvent;
  }

  private static PlatformEvent createChangeEvent(
      String entityType,
      Urn entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      Map<String, Object> parameters,
      Urn actor) {
    final EntityChangeEvent changeEvent = new EntityChangeEvent();
    changeEvent.setEntityType(entityType);
    changeEvent.setEntityUrn(entityUrn);
    changeEvent.setCategory(category.name());
    changeEvent.setOperation(operation.name());
    if (modifier != null) {
      changeEvent.setModifier(modifier);
    }
    changeEvent.setAuditStamp(
        new AuditStamp().setActor(actor).setTime(BusinessAttributeUpdateHookTest.EVENT_TIME));
    changeEvent.setVersion(0);
    if (parameters != null) {
      changeEvent.setParameters(new Parameters(new DataMap(parameters)));
    }
    final PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(
        new PlatformEventHeader().setTimestampMillis(BusinessAttributeUpdateHookTest.EVENT_TIME));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(changeEvent));
    return platformEvent;
  }

  private Map<Urn, EntityResponse> datasetEntityResponses() {
    Map<String, EnvelopedAspect> datasetInfoAspects = new HashMap<>();
    datasetInfoAspects.put(
        Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(editableSchemaMetadata().data())));
    Map<Urn, EntityResponse> datasetEntityResponses = new HashMap<>();
    datasetEntityResponses.put(
        datasetUrn,
        new EntityResponse()
            .setUrn(datasetUrn)
            .setAspects(new EnvelopedAspectMap(datasetInfoAspects)));
    return datasetEntityResponses;
  }

  private EditableSchemaMetadata editableSchemaMetadata() {
    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
    EditableSchemaFieldInfoArray editableSchemaFieldInfos = new EditableSchemaFieldInfoArray();
    com.linkedin.schema.EditableSchemaFieldInfo editableSchemaFieldInfo =
        new EditableSchemaFieldInfo();
    editableSchemaFieldInfo.setBusinessAttribute(new BusinessAttributeAssociation());
    editableSchemaFieldInfo.setFieldPath(SUB_RESOURCE);
    editableSchemaFieldInfos.add(editableSchemaFieldInfo);
    editableSchemaMetadata.setEditableSchemaFieldInfo(editableSchemaFieldInfos);
    return editableSchemaMetadata;
  }
}
