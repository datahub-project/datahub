package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LogicalParentAuthorizationValidatorTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  private static final Urn PARENT_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,parent,PROD)");
  private static final Urn SCHEMA_FIELD_URN =
      UrnUtils.getUrn("urn:li:schemaField:(" + TEST_DATASET_URN + ",field1)");
  private static final Urn PARENT_SCHEMA_FIELD_URN =
      UrnUtils.getUrn("urn:li:schemaField:(" + PARENT_URN + ",parent_field1)");

  private LogicalParentAuthorizationValidator validator;
  private AuthorizationSession mockAuthSession;
  private MockedStatic<AuthUtil> authUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    authUtilMockedStatic = Mockito.mockStatic(AuthUtil.class);
    validator = new LogicalParentAuthorizationValidator();
    validator.setConfig(
        AspectPluginConfig.builder()
            .className(LogicalParentAuthorizationValidator.class.getName())
            .enabled(true)
            .supportedOperations(List.of("UPSERT"))
            .supportedEntityAspectNames(
                List.of(
                    AspectPluginConfig.EntityAspectName.builder()
                        .entityName("*")
                        .aspectName(LOGICAL_PARENT_ASPECT_NAME)
                        .build()))
            .build());
    mockAuthSession = Mockito.mock(AuthorizationSession.class);
  }

  @AfterMethod
  public void tearDown() {
    authUtilMockedStatic.close();
  }

  @Test
  public void testDenyWithoutEditEntityOnChild() {
    LogicalParent logicalParent = new LogicalParent();
    logicalParent.setParent(
        new Edge()
            .setDestinationUrn(PARENT_URN)
            .setCreated(
                new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));

    TestMCP item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, logicalParent, new TestEntityRegistry()).stream()
            .map(i -> (TestMCP) i)
            .findFirst()
            .get();

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(mockAuthSession), eq(ApiOperation.UPDATE), any()))
        .thenReturn(false);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testDenyWithoutEditEntityOnParent() {
    LogicalParent logicalParent = new LogicalParent();
    logicalParent.setParent(
        new Edge()
            .setDestinationUrn(PARENT_URN)
            .setCreated(
                new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));

    TestMCP item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, logicalParent, new TestEntityRegistry()).stream()
            .map(i -> (TestMCP) i)
            .findFirst()
            .get();

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(mockAuthSession), eq(ApiOperation.UPDATE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Collection<Urn> urns = invocation.getArgument(2);
              return urns.size() == 1 && urns.contains(TEST_DATASET_URN);
            });

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isPresent());
  }

  @Test
  public void testAllowWithEditEntityOnChildAndParent() {
    LogicalParent logicalParent = new LogicalParent();
    logicalParent.setParent(
        new Edge()
            .setDestinationUrn(PARENT_URN)
            .setCreated(
                new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));

    TestMCP item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, logicalParent, new TestEntityRegistry()).stream()
            .map(i -> (TestMCP) i)
            .findFirst()
            .get();

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(mockAuthSession), eq(ApiOperation.UPDATE), any()))
        .thenReturn(true);

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testAllowClearWithoutEditEntityOnParent() {
    LogicalParent logicalParent = new LogicalParent();
    logicalParent.setParent(null, com.linkedin.data.template.SetMode.REMOVE_IF_NULL);

    TestMCP item =
        TestMCP.ofOneUpsertItem(TEST_DATASET_URN, logicalParent, new TestEntityRegistry()).stream()
            .map(i -> (TestMCP) i)
            .findFirst()
            .get();

    authUtilMockedStatic
        .when(
            () ->
                AuthUtil.isAuthorizedEntityUrns(
                    eq(mockAuthSession), eq(ApiOperation.UPDATE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Collection<Urn> urns = invocation.getArgument(2);
              return urns.size() == 1 && urns.equals(Set.of(TEST_DATASET_URN));
            });

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }

  @Test
  public void testAllowSchemaFieldViaContainingDatasets() {
    LogicalParent logicalParent = new LogicalParent();
    logicalParent.setParent(
        new Edge()
            .setDestinationUrn(PARENT_SCHEMA_FIELD_URN)
            .setCreated(
                new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));

    TestMCP item =
        TestMCP.ofOneUpsertItem(SCHEMA_FIELD_URN, logicalParent, new TestEntityRegistry()).stream()
            .map(i -> (TestMCP) i)
            .findFirst()
            .get();

    authUtilMockedStatic
        .when(() -> AuthUtil.isAuthorizedEntityUrns(eq(mockAuthSession), eq(UPDATE), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Collection<Urn> urns = invocation.getArgument(2);
              Set<Urn> authorized = Set.of(TEST_DATASET_URN, PARENT_URN);
              return authorized.containsAll(urns);
            });

    Stream<AspectValidationException> result =
        validator.validateProposedAspectsWithAuth(
            OperationFingerprint.EMPTY,
            Collections.singletonList(item),
            TestOperationContexts.systemContextNoSearchAuthorization().getRetrieverContext(),
            mockAuthSession);

    Assert.assertTrue(result.findAny().isEmpty());
  }
}
