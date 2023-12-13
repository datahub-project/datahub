package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ViewServiceTest {

  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  private void testCreateViewSuccess() throws Exception {

    final EntityClient mockClient = createViewMockEntityClient();
    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    // Case 1: With description
    Urn urn =
        service.createView(
            DataHubViewType.PERSONAL,
            "test view",
            "my description",
            new DataHubViewDefinition()
                .setEntityTypes(
                    new StringArray(ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
                .setFilter(
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                ImmutableList.of(
                                    new ConjunctiveCriterion()
                                        .setAnd(
                                            new CriterionArray(
                                                ImmutableList.of(
                                                    new Criterion()
                                                        .setField("field")
                                                        .setCondition(Condition.EQUAL)
                                                        .setValue("value")))))))),
            mockAuthentication(),
            0L);

    Assert.assertEquals(urn, TEST_VIEW_URN);
    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    // Case 2: Without description
    urn =
        service.createView(
            DataHubViewType.PERSONAL,
            "test view",
            null,
            new DataHubViewDefinition()
                .setEntityTypes(
                    new StringArray(ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
                .setFilter(
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                ImmutableList.of(
                                    new ConjunctiveCriterion()
                                        .setAnd(
                                            new CriterionArray(
                                                ImmutableList.of(
                                                    new Criterion()
                                                        .setField("field")
                                                        .setCondition(Condition.EQUAL)
                                                        .setValue("value")))))))),
            mockAuthentication(),
            0L);

    Assert.assertEquals(urn, TEST_VIEW_URN);
    Mockito.verify(mockClient, Mockito.times(2))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(Authentication.class),
            Mockito.eq(false));
  }

  @Test
  private void testCreateViewErrorMissingInputs() throws Exception {
    final EntityClient mockClient = createViewMockEntityClient();
    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    // Case 1: missing View Type
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createView(
                null,
                "test view",
                "my description",
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(
                            ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
                    .setFilter(
                        new Filter()
                            .setOr(
                                new ConjunctiveCriterionArray(
                                    ImmutableList.of(
                                        new ConjunctiveCriterion()
                                            .setAnd(
                                                new CriterionArray(
                                                    ImmutableList.of(
                                                        new Criterion()
                                                            .setField("field")
                                                            .setCondition(Condition.EQUAL)
                                                            .setValue("value")))))))),
                mockAuthentication(),
                0L));

    // Case 2: missing View name
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createView(
                DataHubViewType.PERSONAL,
                null,
                "my description",
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(
                            ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
                    .setFilter(
                        new Filter()
                            .setOr(
                                new ConjunctiveCriterionArray(
                                    ImmutableList.of(
                                        new ConjunctiveCriterion()
                                            .setAnd(
                                                new CriterionArray(
                                                    ImmutableList.of(
                                                        new Criterion()
                                                            .setField("field")
                                                            .setCondition(Condition.EQUAL)
                                                            .setValue("value")))))))),
                mockAuthentication(),
                0L));

    // Case 3: missing View definition
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createView(
                DataHubViewType.PERSONAL,
                "My name",
                "my description",
                null,
                mockAuthentication(),
                0L));
  }

  @Test
  private void testCreateViewError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createView(
                DataHubViewType.PERSONAL,
                "new name",
                "my description",
                new DataHubViewDefinition()
                    .setEntityTypes(
                        new StringArray(
                            ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
                    .setFilter(
                        new Filter()
                            .setOr(
                                new ConjunctiveCriterionArray(
                                    ImmutableList.of(
                                        new ConjunctiveCriterion()
                                            .setAnd(
                                                new CriterionArray(
                                                    ImmutableList.of(
                                                        new Criterion()
                                                            .setField("field")
                                                            .setCondition(Condition.EQUAL)
                                                            .setValue("value")))))))),
                mockAuthentication(),
                1L));
  }

  @Test
  private void testUpdateViewSuccess() throws Exception {
    final DataHubViewType type = DataHubViewType.PERSONAL;
    final String oldName = "old name";
    final String oldDescription = "old description";
    final DataHubViewDefinition oldDefinition =
        new DataHubViewDefinition()
            .setEntityTypes(new StringArray())
            .setFilter(new Filter().setOr(new ConjunctiveCriterionArray(Collections.emptyList())));

    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    resetUpdateViewMockEntityClient(
        mockClient,
        TEST_VIEW_URN,
        type,
        oldName,
        oldDescription,
        oldDefinition,
        TEST_USER_URN,
        0L,
        0L);

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));
    final String newName = "new name";
    final String newDescription = "new description";
    final DataHubViewDefinition newDefinition =
        new DataHubViewDefinition()
            .setEntityTypes(
                new StringArray(ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            ImmutableList.of(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                new Criterion()
                                                    .setField("field")
                                                    .setCondition(Condition.EQUAL)
                                                    .setValue("value"))))))));

    // Case 1: Update name only
    service.updateView(TEST_VIEW_URN, newName, null, null, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateViewProposal(
                    TEST_VIEW_URN, type, newName, oldDescription, oldDefinition, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    resetUpdateViewMockEntityClient(
        mockClient,
        TEST_VIEW_URN,
        type,
        oldName,
        oldDescription,
        oldDefinition,
        TEST_USER_URN,
        0L,
        0L);

    // Case 2: Update description only
    service.updateView(TEST_VIEW_URN, null, newDescription, null, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateViewProposal(
                    TEST_VIEW_URN, type, oldName, newDescription, oldDefinition, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    resetUpdateViewMockEntityClient(
        mockClient,
        TEST_VIEW_URN,
        type,
        oldName,
        oldDescription,
        oldDefinition,
        TEST_USER_URN,
        0L,
        0L);

    // Case 3: Update definition only
    service.updateView(TEST_VIEW_URN, null, null, newDefinition, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateViewProposal(
                    TEST_VIEW_URN, type, oldName, oldDescription, newDefinition, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));

    resetUpdateViewMockEntityClient(
        mockClient,
        TEST_VIEW_URN,
        type,
        oldName,
        oldDescription,
        oldDefinition,
        TEST_USER_URN,
        0L,
        0L);

    // Case 4: Update all fields at once
    service.updateView(
        TEST_VIEW_URN, newName, newDescription, newDefinition, mockAuthentication(), 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(
            Mockito.eq(
                buildUpdateViewProposal(
                    TEST_VIEW_URN, type, newName, newDescription, newDefinition, 0L, 1L)),
            Mockito.any(Authentication.class),
            Mockito.eq(false));
  }

  @Test
  private void testUpdateViewMissingView() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(TEST_VIEW_URN),
                Mockito.eq(ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(null);

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    final String newName = "new name";

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.updateView(TEST_VIEW_URN, newName, null, null, mockAuthentication(), 1L));
  }

  @Test
  private void testUpdateViewError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            Mockito.eq(DATAHUB_VIEW_ENTITY_NAME),
            Mockito.eq(TEST_VIEW_URN),
            Mockito.eq(ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME)),
            Mockito.any(Authentication.class));

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.updateView(TEST_VIEW_URN, "new name", null, null, mockAuthentication(), 1L));
  }

  @Test
  private void testDeleteViewSuccess() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    service.deleteView(TEST_VIEW_URN, mockAuthentication());

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));
  }

  @Test
  private void testDeleteViewError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .deleteEntity(Mockito.eq(TEST_VIEW_URN), Mockito.any(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class, () -> service.deleteView(TEST_VIEW_URN, mockAuthentication()));
  }

  @Test
  private void testGetViewInfoSuccess() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    final DataHubViewType type = DataHubViewType.PERSONAL;
    final String name = "name";
    final String description = "description";
    final DataHubViewDefinition definition =
        new DataHubViewDefinition()
            .setEntityTypes(
                new StringArray(ImmutableList.of(DATASET_ENTITY_NAME, DASHBOARD_ENTITY_NAME)))
            .setFilter(
                new Filter()
                    .setOr(
                        new ConjunctiveCriterionArray(
                            ImmutableList.of(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(
                                            ImmutableList.of(
                                                new Criterion()
                                                    .setField("field")
                                                    .setCondition(Condition.EQUAL)
                                                    .setValue("value"))))))));

    resetGetViewInfoMockEntityClient(
        mockClient, TEST_VIEW_URN, type, name, description, definition, TEST_USER_URN, 0L, 1L);

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    final DataHubViewInfo info = service.getViewInfo(TEST_VIEW_URN, mockAuthentication());

    // Assert that the info is correct.
    Assert.assertEquals(info.getType(), type);
    Assert.assertEquals((long) info.getCreated().getTime(), 0L);
    Assert.assertEquals((long) info.getLastModified().getTime(), 1L);
    Assert.assertEquals(info.getName(), name);
    Assert.assertEquals(info.getDescription(), description);
    Assert.assertEquals(info.getCreated().getActor(), TEST_USER_URN);
    Assert.assertEquals(info.getDefinition(), definition);
  }

  @Test
  private void testGetViewInfoNoViewExists() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(TEST_VIEW_URN),
                Mockito.eq(ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(null);

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    Assert.assertNull(service.getViewInfo(TEST_VIEW_URN, mockAuthentication()));
  }

  @Test
  private void testGetViewInfoError() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            Mockito.eq(DATAHUB_VIEW_ENTITY_NAME),
            Mockito.eq(TEST_VIEW_URN),
            Mockito.eq(ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME)),
            Mockito.any(Authentication.class));

    final ViewService service = new ViewService(mockClient, Mockito.mock(Authentication.class));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class, () -> service.getViewInfo(TEST_VIEW_URN, mockAuthentication()));
  }

  private static MetadataChangeProposal buildUpdateViewProposal(
      final Urn urn,
      final DataHubViewType newType,
      final String newName,
      final String newDescription,
      final DataHubViewDefinition newDefinition,
      final long createdAtMs,
      final long updatedAtMs) {

    DataHubViewInfo info = new DataHubViewInfo();
    info.setType(newType);
    info.setName(newName);
    info.setDescription(newDescription);
    info.setDefinition(newDefinition);
    info.setCreated(new AuditStamp().setActor(TEST_USER_URN).setTime(createdAtMs));
    info.setLastModified(new AuditStamp().setActor(TEST_USER_URN).setTime(updatedAtMs));

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(DATAHUB_VIEW_ENTITY_NAME);
    mcp.setAspectName(DATAHUB_VIEW_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));
    return mcp;
  }

  private static EntityClient createViewMockEntityClient() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                Mockito.any(MetadataChangeProposal.class),
                Mockito.any(Authentication.class),
                Mockito.eq(false)))
        .thenReturn(TEST_VIEW_URN.toString());
    return mockClient;
  }

  private static void resetUpdateViewMockEntityClient(
      final EntityClient mockClient,
      final Urn viewUrn,
      final DataHubViewType existingType,
      final String existingName,
      final String existingDescription,
      final DataHubViewDefinition existingDefinition,
      final Urn existingOwner,
      final long existingCreatedAt,
      final long existingUpdatedAt)
      throws Exception {

    Mockito.reset(mockClient);

    Mockito.when(
            mockClient.ingestProposal(
                Mockito.any(MetadataChangeProposal.class),
                Mockito.any(Authentication.class),
                Mockito.eq(false)))
        .thenReturn(viewUrn.toString());

    final DataHubViewInfo existingInfo =
        new DataHubViewInfo()
            .setType(existingType)
            .setName(existingName)
            .setDescription(existingDescription)
            .setDefinition(existingDefinition)
            .setCreated(new AuditStamp().setActor(existingOwner).setTime(existingCreatedAt))
            .setLastModified(new AuditStamp().setActor(existingOwner).setTime(existingUpdatedAt));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(viewUrn),
                Mockito.eq(ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setUrn(viewUrn)
                .setEntityName(DATAHUB_VIEW_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            DATAHUB_VIEW_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingInfo.data()))))));
  }

  private static void resetGetViewInfoMockEntityClient(
      final EntityClient mockClient,
      final Urn viewUrn,
      final DataHubViewType existingType,
      final String existingName,
      final String existingDescription,
      final DataHubViewDefinition existingDefinition,
      final Urn existingOwner,
      final long existingCreatedAt,
      final long existingUpdatedAt)
      throws Exception {

    Mockito.reset(mockClient);

    final DataHubViewInfo existingInfo =
        new DataHubViewInfo()
            .setType(existingType)
            .setName(existingName)
            .setDescription(existingDescription)
            .setDefinition(existingDefinition)
            .setCreated(new AuditStamp().setActor(existingOwner).setTime(existingCreatedAt))
            .setLastModified(new AuditStamp().setActor(existingOwner).setTime(existingUpdatedAt));

    Mockito.when(
            mockClient.getV2(
                Mockito.eq(DATAHUB_VIEW_ENTITY_NAME),
                Mockito.eq(viewUrn),
                Mockito.eq(ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME)),
                Mockito.any(Authentication.class)))
        .thenReturn(
            new EntityResponse()
                .setUrn(viewUrn)
                .setEntityName(DATAHUB_VIEW_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            DATAHUB_VIEW_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(existingInfo.data()))))));
  }

  private static Authentication mockAuthentication() {
    Authentication mockAuth = Mockito.mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, TEST_USER_URN.getId()));
    return mockAuth;
  }
}
