package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryServiceTest {

  private static final Urn TEST_QUERY_URN = UrnUtils.getUrn("urn:li:query:test");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

  @Test
  private void testCreateQuerySuccess() throws Exception {

    final SystemEntityClient mockClient = createQueryMockEntityClient();
    final QueryService service = new QueryService(mockClient);

    // Case 1: All fields provided
    Urn urn =
        service.createQuery(
            opContext,
            "test query",
            "my description",
            QuerySource.MANUAL,
            new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
            ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN)),
            0L);

    Assert.assertEquals(urn, TEST_QUERY_URN);

    // Ingests both aspects - properties and subjects
    Mockito.verify(mockClient, Mockito.times(2))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));

    // Case 2: Null fields provided
    urn =
        service.createQuery(
            opContext,
            null,
            null,
            QuerySource.MANUAL,
            new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
            ImmutableList.of(),
            0L);

    Assert.assertEquals(urn, TEST_QUERY_URN);
    Mockito.verify(mockClient, Mockito.times(4))
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));
  }

  @Test
  private void testCreateQueryErrorMissingInputs() throws Exception {
    final SystemEntityClient mockClient = createQueryMockEntityClient();
    final QueryService service = new QueryService(mockClient);

    // Case 1: missing Query Source
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createQuery(
                mock(OperationContext.class),
                null,
                null,
                null, // Cannot be null
                new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
                ImmutableList.of(),
                0L));

    // Case 2: missing Query Statement
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createQuery(
                mock(OperationContext.class),
                null,
                null,
                QuerySource.MANUAL, // Cannot be null
                null,
                ImmutableList.of(),
                0L));

    // Case 3: missing Query Subjects
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createQuery(
                mock(OperationContext.class),
                null,
                null,
                QuerySource.MANUAL, // Cannot be null
                new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
                null,
                0L));
  }

  @Test
  private void testCreateQueryError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false));

    final QueryService service = new QueryService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.createQuery(
                mock(OperationContext.class),
                "test query",
                "my description",
                QuerySource.MANUAL,
                new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
                ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN)),
                0L));
  }

  @Test
  private void testUpdateQuerySuccess() throws Exception {
    final String oldName = "old name";
    final String oldDescription = "old description";
    final QueryStatement oldStatement =
        new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table");

    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    resetQueryPropertiesClient(
        mockClient,
        TEST_QUERY_URN,
        oldName,
        oldDescription,
        QuerySource.MANUAL,
        oldStatement,
        TEST_USER_URN,
        0L,
        0L);

    final QueryService service = new QueryService(mockClient);

    final String newName = "new name";
    final String newDescription = "new description";
    final QueryStatement newStatement =
        new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table");
    final List<QuerySubject> newSubjects =
        ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN_2));

    // Case 1: Update name only
    service.updateQuery(opContext, TEST_QUERY_URN, newName, null, null, null, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                ImmutableList.of(
                    buildUpdateQueryPropertiesProposal(
                        TEST_QUERY_URN,
                        newName,
                        oldDescription,
                        QuerySource.MANUAL,
                        oldStatement,
                        0L,
                        1L))),
            Mockito.eq(false));

    resetQueryPropertiesClient(
        mockClient,
        TEST_QUERY_URN,
        oldName,
        oldDescription,
        QuerySource.MANUAL,
        oldStatement,
        TEST_USER_URN,
        0L,
        0L);

    // Case 2: Update description only
    service.updateQuery(opContext, TEST_QUERY_URN, null, newDescription, null, null, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                ImmutableList.of(
                    buildUpdateQueryPropertiesProposal(
                        TEST_QUERY_URN,
                        oldName,
                        newDescription,
                        QuerySource.MANUAL,
                        oldStatement,
                        0L,
                        1L))),
            Mockito.eq(false));

    resetQueryPropertiesClient(
        mockClient,
        TEST_QUERY_URN,
        oldName,
        oldDescription,
        QuerySource.MANUAL,
        oldStatement,
        TEST_USER_URN,
        0L,
        0L);

    // Case 3: Update definition only
    service.updateQuery(opContext, TEST_QUERY_URN, null, null, newStatement, null, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                ImmutableList.of(
                    buildUpdateQueryPropertiesProposal(
                        TEST_QUERY_URN,
                        oldName,
                        oldDescription,
                        QuerySource.MANUAL,
                        newStatement,
                        0L,
                        1L))),
            Mockito.eq(false));

    resetQueryPropertiesClient(
        mockClient,
        TEST_QUERY_URN,
        oldName,
        oldDescription,
        QuerySource.MANUAL,
        oldStatement,
        TEST_USER_URN,
        0L,
        0L);

    // Case 4: Update subjects only
    service.updateQuery(opContext, TEST_QUERY_URN, null, null, null, newSubjects, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                ImmutableList.of(
                    buildUpdateQueryPropertiesProposal(
                        TEST_QUERY_URN,
                        oldName,
                        oldDescription,
                        QuerySource.MANUAL,
                        oldStatement,
                        0L,
                        1L),
                    buildUpdateQuerySubjectsProposal(TEST_QUERY_URN, newSubjects))),
            Mockito.eq(false));

    // Case 5: Update all fields
    service.updateQuery(
        opContext, TEST_QUERY_URN, newName, newDescription, newStatement, newSubjects, 1L);

    Mockito.verify(mockClient, Mockito.times(1))
        .batchIngestProposals(
            any(OperationContext.class),
            Mockito.eq(
                ImmutableList.of(
                    buildUpdateQueryPropertiesProposal(
                        TEST_QUERY_URN,
                        newName,
                        newDescription,
                        QuerySource.MANUAL,
                        newStatement,
                        0L,
                        1L),
                    buildUpdateQuerySubjectsProposal(TEST_QUERY_URN, newSubjects))),
            Mockito.eq(false));
  }

  @Test
  private void testUpdateQueryMissingQuery() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(QUERY_ENTITY_NAME),
                Mockito.eq(TEST_QUERY_URN),
                Mockito.eq(ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME))))
        .thenReturn(null);

    final QueryService service = new QueryService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.updateQuery(
                mock(OperationContext.class),
                TEST_QUERY_URN,
                "new name",
                null,
                new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
                ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN)),
                1L));
  }

  @Test
  private void testUpdateQueryError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(QUERY_ENTITY_NAME),
            Mockito.eq(TEST_QUERY_URN),
            Mockito.eq(ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME)));

    final QueryService service = new QueryService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () ->
            service.updateQuery(
                mock(OperationContext.class),
                TEST_QUERY_URN,
                "new name",
                null,
                new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table"),
                ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN)),
                1L));
  }

  @Test
  private void testDeleteQuerySuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final QueryService service = new QueryService(mockClient);

    service.deleteQuery(opContext, TEST_QUERY_URN);

    Mockito.verify(mockClient, Mockito.times(1))
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_QUERY_URN));
  }

  @Test
  private void testDeleteQueryError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final QueryService service = new QueryService(mockClient);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .deleteEntity(any(OperationContext.class), Mockito.eq(TEST_QUERY_URN));

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.deleteQuery(any(OperationContext.class), TEST_QUERY_URN));
  }

  @Test
  private void testGetQueryPropertiesSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final String name = "name";
    final String description = "description";
    final QueryStatement statement =
        new QueryStatement().setLanguage(QueryLanguage.SQL).setValue("SELECT * FROM Table");

    resetQueryPropertiesClient(
        mockClient,
        TEST_QUERY_URN,
        name,
        description,
        QuerySource.MANUAL,
        statement,
        TEST_USER_URN,
        0L,
        1L);

    final QueryService service = new QueryService(mockClient);

    final QueryProperties properties = service.getQueryProperties(opContext, TEST_QUERY_URN);

    // Assert that the info is correct.
    Assert.assertEquals((long) properties.getCreated().getTime(), 0L);
    Assert.assertEquals((long) properties.getLastModified().getTime(), 1L);
    Assert.assertEquals(properties.getName(), name);
    Assert.assertEquals(properties.getDescription(), description);
    Assert.assertEquals(properties.getCreated().getActor(), TEST_USER_URN);
    Assert.assertEquals(properties.getStatement(), statement);
    Assert.assertEquals(properties.getSource(), QuerySource.MANUAL);
  }

  @Test
  private void testGetQueryPropertiesNoQueryExists() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(QUERY_ENTITY_NAME),
                Mockito.eq(TEST_QUERY_URN),
                Mockito.eq(
                    ImmutableSet.of(
                        QUERY_PROPERTIES_ASPECT_NAME, Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(null);

    final QueryService service = new QueryService(mockClient);

    Assert.assertNull(service.getQueryProperties(opContext, TEST_QUERY_URN));
  }

  @Test
  private void testGetQueryPropertiesError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(QUERY_ENTITY_NAME),
            Mockito.eq(TEST_QUERY_URN),
            Mockito.eq(
                ImmutableSet.of(
                    QUERY_PROPERTIES_ASPECT_NAME, Constants.QUERY_SUBJECTS_ASPECT_NAME)));

    final QueryService service = new QueryService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.getQueryProperties(mock(OperationContext.class), TEST_QUERY_URN));
  }

  @Test
  private void testGetQuerySubjectsSuccess() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    final QuerySubjects existingSubjects =
        new QuerySubjects()
            .setSubjects(
                new QuerySubjectArray(
                    ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))));

    resetQuerySubjectsClient(mockClient, TEST_QUERY_URN, existingSubjects);

    final QueryService service = new QueryService(mockClient);

    final QuerySubjects querySubjects = service.getQuerySubjects(opContext, TEST_QUERY_URN);

    Assert.assertEquals(querySubjects, existingSubjects);
  }

  @Test
  private void testGetQuerySubjectsNoQueryExists() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(QUERY_ENTITY_NAME),
                Mockito.eq(TEST_QUERY_URN),
                Mockito.eq(
                    ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME, QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(null);

    final QueryService service = new QueryService(mockClient);

    Assert.assertNull(service.getQueryProperties(opContext, TEST_QUERY_URN));
  }

  @Test
  private void testGetQuerySubjectsError() throws Exception {
    final SystemEntityClient mockClient = mock(SystemEntityClient.class);

    Mockito.doThrow(new RemoteInvocationException())
        .when(mockClient)
        .getV2(
            any(OperationContext.class),
            Mockito.eq(QUERY_ENTITY_NAME),
            Mockito.eq(TEST_QUERY_URN),
            Mockito.eq(ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME, QUERY_SUBJECTS_ASPECT_NAME)));

    final QueryService service = new QueryService(mockClient);

    // Throws wrapped exception
    Assert.assertThrows(
        RuntimeException.class,
        () -> service.getQuerySubjects(mock(OperationContext.class), TEST_QUERY_URN));
  }

  private static MetadataChangeProposal buildUpdateQuerySubjectsProposal(
      final Urn urn, final List<QuerySubject> querySubjects) {

    QuerySubjects subjects = new QuerySubjects();
    subjects.setSubjects(new QuerySubjectArray(querySubjects));

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(QUERY_ENTITY_NAME);
    mcp.setAspectName(QUERY_SUBJECTS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(subjects));
    return mcp;
  }

  private static MetadataChangeProposal buildUpdateQueryPropertiesProposal(
      final Urn urn,
      final String newName,
      final String newDescription,
      final QuerySource newSource,
      final QueryStatement newStatement,
      final long createdAtMs,
      final long updatedAtMs) {

    QueryProperties properties = new QueryProperties();
    properties.setName(newName);
    properties.setDescription(newDescription);
    properties.setSource(newSource);
    properties.setStatement(newStatement);
    properties.setCreated(new AuditStamp().setActor(TEST_USER_URN).setTime(createdAtMs));
    properties.setLastModified(new AuditStamp().setActor(TEST_USER_URN).setTime(updatedAtMs));

    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(QUERY_ENTITY_NAME);
    mcp.setAspectName(QUERY_PROPERTIES_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(properties));
    return mcp;
  }

  private static SystemEntityClient createQueryMockEntityClient() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false)))
        .thenReturn(TEST_QUERY_URN.toString());
    return mockClient;
  }

  private static void resetQueryPropertiesClient(
      final EntityClient mockClient,
      final Urn queryUrn,
      final String existingName,
      final String existingDescription,
      final QuerySource existingSource,
      final QueryStatement existingStatement,
      final Urn existingOwner,
      final long existingCreatedAt,
      final long existingUpdatedAt)
      throws Exception {

    Mockito.reset(mockClient);

    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false)))
        .thenReturn(queryUrn.toString());

    final QueryProperties existingProperties =
        new QueryProperties()
            .setSource(existingSource)
            .setName(existingName)
            .setDescription(existingDescription)
            .setStatement(existingStatement)
            .setCreated(new AuditStamp().setActor(existingOwner).setTime(existingCreatedAt))
            .setLastModified(new AuditStamp().setActor(existingOwner).setTime(existingUpdatedAt));

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(QUERY_ENTITY_NAME),
                Mockito.eq(queryUrn),
                Mockito.eq(
                    ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME, QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(queryUrn)
                .setEntityName(QUERY_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            QUERY_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(existingProperties.data()))))));
  }

  private static void resetQuerySubjectsClient(
      final EntityClient mockClient, final Urn queryUrn, final QuerySubjects subjects)
      throws Exception {

    Mockito.reset(mockClient);

    Mockito.when(
            mockClient.ingestProposal(
                any(OperationContext.class), any(MetadataChangeProposal.class), Mockito.eq(false)))
        .thenReturn(queryUrn.toString());

    Mockito.when(
            mockClient.getV2(
                any(OperationContext.class),
                Mockito.eq(QUERY_ENTITY_NAME),
                Mockito.eq(queryUrn),
                Mockito.eq(
                    ImmutableSet.of(QUERY_PROPERTIES_ASPECT_NAME, QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(queryUrn)
                .setEntityName(QUERY_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            QUERY_SUBJECTS_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(subjects.data()))))));
  }

  private static Authentication mockAuthentication() {
    Authentication mockAuth = mock(Authentication.class);
    Mockito.when(mockAuth.getActor()).thenReturn(new Actor(ActorType.USER, TEST_USER_URN.getId()));
    return mockAuth;
  }
}
