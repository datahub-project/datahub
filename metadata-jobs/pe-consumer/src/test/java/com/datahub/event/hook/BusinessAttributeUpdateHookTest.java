package com.datahub.event.hook;

import static com.datahub.event.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ASPECT;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;
import static com.linkedin.metadata.search.utils.QueryUtils.newRelationshipFilter;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.service.BusinessAttributeUpdateHookService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.Parameters;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BusinessAttributeUpdateHookTest {

  private static final String TEST_BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:12668aea-009b-400e-8408-e661c3a230dd";
  private static final String BUSINESS_ATTRIBUTE_OF = "BusinessAttributeOf";
  private static final Urn SCHEMA_FIELD_URN =
      UrnUtils.getUrn(
          "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleCypressHiveDataset,PROD),field_bar)");
  private static final String TAG_NAME = "test";
  private static final long EVENT_TIME = 123L;
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static Urn actorUrn;
  private GraphService mockGraphService;
  private EntityService mockEntityService;
  private BusinessAttributeUpdateHook businessAttributeUpdateHook;
  private BusinessAttributeUpdateHookService businessAttributeServiceHook;

  @BeforeMethod
  public void setupTest() throws URISyntaxException {
    mockGraphService = Mockito.mock(GraphService.class);
    mockEntityService = Mockito.mock(EntityService.class);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    businessAttributeServiceHook =
        new BusinessAttributeUpdateHookService(
            mockGraphService, mockEntityService, ENTITY_REGISTRY, 100);
    businessAttributeUpdateHook = new BusinessAttributeUpdateHook(businessAttributeServiceHook);
  }

  @Test
  public void testMCLOnBusinessAttributeUpdate() throws Exception {
    PlatformEvent platformEvent = createPlatformEventBusinessAttribute();
    final RelatedEntitiesResult mockRelatedEntities =
        new RelatedEntitiesResult(
            0,
            1,
            1,
            ImmutableList.of(
                new RelatedEntity(BUSINESS_ATTRIBUTE_OF, SCHEMA_FIELD_URN.toString())));
    // mock response
    Mockito.when(
            mockGraphService.findRelatedEntities(
                null,
                newFilter("urn", TEST_BUSINESS_ATTRIBUTE_URN),
                null,
                EMPTY_FILTER,
                Arrays.asList(BUSINESS_ATTRIBUTE_OF),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
                0,
                100))
        .thenReturn(mockRelatedEntities);
    assertEquals(mockRelatedEntities.getTotal(), 1);

    Mockito.when(
            mockEntityService.getLatestEnvelopedAspect(
                eq(SCHEMA_FIELD_ENTITY_NAME), eq(SCHEMA_FIELD_URN), eq(BUSINESS_ATTRIBUTE_ASPECT)))
        .thenReturn(envelopedAspect());

    // mock response
    Mockito.when(
            mockEntityService.alwaysProduceMCLAsync(
                Mockito.any(Urn.class),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(AspectSpec.class),
                eq(null),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(ChangeType.class)))
        .thenReturn(Pair.of(Mockito.mock(Future.class), false));

    // invoke
    businessAttributeServiceHook.handleChangeEvent(platformEvent);

    // verify
    Mockito.verify(mockGraphService, Mockito.times(1))
        .findRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());

    Mockito.verify(mockEntityService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.any(Urn.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.any(AspectSpec.class),
            eq(null),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(ChangeType.class));
  }

  @Test
  private void testMCLOnInvalidCategory() throws Exception {
    PlatformEvent platformEvent = createPlatformEventInvalidCategory();

    // invoke
    businessAttributeServiceHook.handleChangeEvent(platformEvent);

    // verify
    Mockito.verify(mockGraphService, Mockito.times(0))
        .findRelatedEntities(
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.anyInt());

    Mockito.verify(mockEntityService, Mockito.times(0))
        .alwaysProduceMCLAsync(
            Mockito.any(Urn.class),
            Mockito.anyString(),
            Mockito.anyString(),
            Mockito.any(AspectSpec.class),
            eq(null),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(ChangeType.class));
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

  private EnvelopedAspect envelopedAspect() {
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(new BusinessAttributes().data()));
    envelopedAspect.setSystemMetadata(new SystemMetadata());
    return envelopedAspect;
  }
}
