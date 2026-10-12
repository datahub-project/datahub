package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Runs the validator against a real session {@link OperationContext}, so the request-scoped
 * authorization result cache and field overrides are exercised, not mocked.
 */
public class DataPlatformInstanceWriteAuthorizationValidatorTest {

  private static final Urn PLATFORM = UrnUtils.getUrn("urn:li:dataPlatform:mysql");
  private static final Urn OWN_INSTANCE =
      UrnUtils.getUrn("urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,team_a)");
  private static final Urn OWN_OTHER_INSTANCE =
      UrnUtils.getUrn("urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,team_a_2)");
  private static final Urn FOREIGN_INSTANCE =
      UrnUtils.getUrn("urn:li:dataPlatformInstance:(urn:li:dataPlatform:mysql,team_b)");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)");
  private static final Urn WRITER = UrnUtils.getUrn("urn:li:corpuser:instanceScopedWriter");

  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;
  private OperationContext session;
  private DataPlatformInstanceWriteAuthorizationValidator validator;
  private AspectSpec aspectSpec;

  /**
   * Stands in for a policy: EDIT_ENTITY on any entity whose platform instance is one of the
   * writer's own instances. Reads the stored instance unless the request overrides it.
   */
  private final Authorizer instanceScopedAuthorizer =
      new Authorizer() {
        @Override
        public AuthorizationResult authorize(AuthorizationRequest request) {
          EntitySpec resource = request.getResourceSpec().orElse(null);
          String instance = resolveInstance(resource);
          boolean allowed =
              "EDIT_ENTITY".equals(request.getPrivilege())
                  && (OWN_INSTANCE.toString().equals(instance)
                      || OWN_OTHER_INSTANCE.toString().equals(instance));
          return new AuthorizationResult(
              request,
              allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
              null);
        }
      };

  private final AtomicReference<OperationContext> sessionRef = new AtomicReference<>();
  private Urn storedInstance;

  @BeforeMethod
  public void setUp() {
    storedInstance = null;
    aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation ->
                storedInstance == null
                    ? Map.of()
                    : Map.of(
                        DATASET_URN,
                        Map.of(
                            DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new Aspect(dpi(storedInstance).data()))));
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);

    session =
        TestOperationContexts.userContextNoSearchAuthorization(instanceScopedAuthorizer, WRITER);
    sessionRef.set(session);

    aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(DATA_PLATFORM_INSTANCE_ASPECT_NAME);

    validator =
        new DataPlatformInstanceWriteAuthorizationValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className(DataPlatformInstanceWriteAuthorizationValidator.class.getName())
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT", "CREATE", "PATCH"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName("*")
                                .aspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                                .build()))
                    .build());
  }

  @Test
  public void testMoveToForeignInstanceDenied() {
    storedInstance = OWN_INSTANCE;
    // The URN-level ingest check runs first in the same request and caches ALLOW.
    assertTrue(entityEditAllowed());

    List<AspectValidationException> failures = validate(upsert(FOREIGN_INSTANCE));

    assertEquals(failures.size(), 1);
  }

  @Test
  public void testMoveBetweenOwnInstancesAllowed() {
    storedInstance = OWN_INSTANCE;
    assertTrue(entityEditAllowed());

    assertEquals(validate(upsert(OWN_OTHER_INSTANCE)).size(), 0);
  }

  @Test
  public void testUnchangedInstanceAllowedWithoutAuthorizationCalls() {
    storedInstance = OWN_INSTANCE;
    OperationContext denyAll =
        TestOperationContexts.userContextNoSearchAuthorization(Authorizer.EMPTY, WRITER);

    assertEquals(
        validator
            .validateProposedAspectsWithAuth(
                denyAll, List.of(upsert(OWN_INSTANCE)), retrieverContext, denyAll)
            .count(),
        0);
  }

  @Test
  public void testFirstWriteAllowed() {
    storedInstance = null;

    assertEquals(validate(upsert(FOREIGN_INSTANCE)).size(), 0);
  }

  @Test
  public void testRemovingInstanceAllowedForWriterOnCurrentInstance() {
    storedInstance = OWN_INSTANCE;

    assertEquals(validate(upsert(null)).size(), 0);
  }

  @Test
  public void testRemovingInstanceDeniedWithoutRightsOnCurrentInstance() {
    storedInstance = FOREIGN_INSTANCE;

    assertEquals(validate(upsert(null)).size(), 1);
  }

  @Test
  public void testSystemActorExempt() {
    storedInstance = OWN_INSTANCE;
    OperationContext system = TestOperationContexts.systemContextNoSearchAuthorization();

    assertEquals(
        validator
            .validateProposedAspectsWithAuth(
                system, List.of(upsert(FOREIGN_INSTANCE)), retrieverContext, system)
            .count(),
        0);
  }

  @Test
  public void testNoReadsWhenAspectNotWritten() {
    TestMCP other =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(mock(com.linkedin.metadata.models.EntitySpec.class))
            .aspectSpec(mock(AspectSpec.class))
            .recordTemplate(dpi(FOREIGN_INSTANCE))
            .changeType(ChangeType.UPSERT)
            .build();

    assertEquals(
        validator
            .validateProposedAspectsWithAuth(session, List.of(other), retrieverContext, session)
            .count(),
        0);
    verify(aspectRetriever, never()).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testOverrideIsRemovedAfterCheck() {
    storedInstance = OWN_INSTANCE;
    validate(upsert(FOREIGN_INSTANCE));

    assertTrue(session.getAuthorizationContext().getSessionResourceFieldOverrides().isEmpty());
    assertTrue(entityEditAllowed());
  }

  private boolean entityEditAllowed() {
    return session
            .authorize("EDIT_ENTITY", new EntitySpec("dataset", DATASET_URN.toString()))
            .getType()
        == AuthorizationResult.Type.ALLOW;
  }

  @Nullable
  private String resolveInstance(@Nullable EntitySpec resource) {
    if (resource == null) {
      return null;
    }
    Map<EntityFieldType, FieldResolver> overrides =
        sessionRef.get().getAuthorizationContext().getSessionResourceFieldOverrides().get(resource);
    if (overrides != null && overrides.containsKey(EntityFieldType.DATA_PLATFORM_INSTANCE)) {
      Set<String> values =
          overrides
              .get(EntityFieldType.DATA_PLATFORM_INSTANCE)
              .getFieldValuesFuture()
              .join()
              .getValues();
      return values.stream().findFirst().orElse(null);
    }
    return storedInstance == null ? null : storedInstance.toString();
  }

  private List<AspectValidationException> validate(TestMCP item) {
    return validator
        .validateProposedAspectsWithAuth(session, List.of(item), retrieverContext, session)
        .collect(Collectors.toList());
  }

  private TestMCP upsert(@Nullable Urn instance) {
    return TestMCP.builder()
        .urn(DATASET_URN)
        .entitySpec(mock(com.linkedin.metadata.models.EntitySpec.class))
        .aspectSpec(aspectSpec)
        .recordTemplate(dpi(instance))
        .changeType(ChangeType.UPSERT)
        .build();
  }

  private static DataPlatformInstance dpi(@Nullable Urn instance) {
    DataPlatformInstance dpi = new DataPlatformInstance().setPlatform(PLATFORM);
    if (instance != null) {
      dpi.setInstance(instance);
    }
    return dpi;
  }
}
