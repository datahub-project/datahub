package com.datahub.event.hook;

import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ASPECT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

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
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.service.BusinessAttributeUpdateHookService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.Parameters;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
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
  private UpdateIndicesService mockUpdateIndicesService;
  private BusinessAttributeUpdateHook businessAttributeUpdateHook;
  private BusinessAttributeUpdateHookService businessAttributeServiceHook;

  @BeforeMethod
  public void setupTest() throws URISyntaxException {
    mockUpdateIndicesService = mock(UpdateIndicesService.class);
    actorUrn = Urn.createFromString(TEST_ACTOR_URN);
    businessAttributeServiceHook =
        new BusinessAttributeUpdateHookService(mockUpdateIndicesService, 100, 1, 10, 60);
    businessAttributeUpdateHook =
        new BusinessAttributeUpdateHook(businessAttributeServiceHook, true);
  }

  @Test
  public void testMCLOnBusinessAttributeUpdate() throws Exception {
    PlatformEvent platformEvent = createPlatformEventBusinessAttribute();

    // mock response
    OperationContext opContext =
        mockOperationContextWithGraph(
            ImmutableList.of(
                new RelatedEntity(BUSINESS_ATTRIBUTE_OF, SCHEMA_FIELD_URN.toString()),
                new RelatedEntity(BUSINESS_ATTRIBUTE_OF, SCHEMA_FIELD_URN.toString())));

    when(opContext
            .getAspectRetriever()
            .getLatestAspectObjects(
                eq(Set.of(SCHEMA_FIELD_URN)), eq(Set.of(BUSINESS_ATTRIBUTE_ASPECT))))
        .thenReturn(
            Map.of(
                SCHEMA_FIELD_URN,
                Map.of(BUSINESS_ATTRIBUTE_ASPECT, new Aspect(new BusinessAttributes().data()))));

    // invoke
    businessAttributeServiceHook.handleChangeEvent(opContext, platformEvent);

    // verify
    // page 1
    Mockito.verify(opContext.getRetrieverContext().getGraphRetriever(), Mockito.times(1))
        .scrollRelatedEntities(
            isNull(),
            any(Filter.class),
            isNull(),
            any(Filter.class),
            any(),
            any(RelationshipFilter.class),
            eq(Edge.EDGE_SORT_CRITERION),
            isNull(),
            anyInt(),
            isNull(),
            isNull());
    // page 2
    Mockito.verify(opContext.getRetrieverContext().getGraphRetriever(), Mockito.times(1))
        .scrollRelatedEntities(
            isNull(),
            any(Filter.class),
            isNull(),
            any(Filter.class),
            any(),
            any(RelationshipFilter.class),
            eq(Edge.EDGE_SORT_CRITERION),
            eq("1"),
            anyInt(),
            isNull(),
            isNull());

    Mockito.verifyNoMoreInteractions(opContext.getRetrieverContext().getGraphRetriever());

    // 2 pages = 2 ingest proposals
    Mockito.verify(mockUpdateIndicesService, Mockito.times(2))
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  private void testMCLOnInvalidCategory() throws Exception {
    PlatformEvent platformEvent = createPlatformEventInvalidCategory();
    OperationContext opContext = mockOperationContextWithGraph(ImmutableList.of());

    // invoke
    businessAttributeServiceHook.handleChangeEvent(opContext, platformEvent);

    // verify
    Mockito.verifyNoInteractions(opContext.getRetrieverContext().getGraphRetriever());
    Mockito.verifyNoInteractions(opContext.getAspectRetriever());
    Mockito.verifyNoInteractions(mockUpdateIndicesService);
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

  private OperationContext mockOperationContextWithGraph(List<RelatedEntity> graphEdges) {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);

    RetrieverContext mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mock(AspectRetriever.class));
    when(mockRetrieverContext.getCachingAspectRetriever())
        .thenReturn(TestOperationContexts.emptyActiveUsersAspectRetriever(null));
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(graphRetriever);

    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockRetrieverContext);

    // reset mock for test
    reset(opContext.getAspectRetriever());

    if (!graphEdges.isEmpty()) {

      int idx = 0;
      OngoingStubbing<RelatedEntitiesScrollResult> multiStub =
          when(
              graphRetriever.scrollRelatedEntities(
                  isNull(),
                  any(Filter.class),
                  isNull(),
                  any(Filter.class),
                  eq(Arrays.asList(BUSINESS_ATTRIBUTE_OF)),
                  any(RelationshipFilter.class),
                  eq(Edge.EDGE_SORT_CRITERION),
                  nullable(String.class),
                  eq(1),
                  isNull(),
                  isNull()));

      for (RelatedEntity relatedEntity : graphEdges) {
        final String scrollId;
        if (idx < graphEdges.size() - 1) {
          idx += 1;
          scrollId = String.valueOf(idx);
        } else {
          scrollId = null;
        }

        multiStub =
            multiStub.thenReturn(
                new RelatedEntitiesScrollResult(
                    graphEdges.size(),
                    1,
                    scrollId,
                    List.of(
                        new RelatedEntities(
                            relatedEntity.getRelationshipType(),
                            relatedEntity.getUrn(),
                            TEST_BUSINESS_ATTRIBUTE_URN,
                            RelationshipDirection.INCOMING,
                            null))));
      }
    }

    return opContext;
  }
}
