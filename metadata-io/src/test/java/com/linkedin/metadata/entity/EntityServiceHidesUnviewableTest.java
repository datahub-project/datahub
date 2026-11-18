package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.SubTypes;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.metadata.AspectGenerationUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import com.linkedin.metadata.entity.ebean.PassThroughScopedTransactionFactory;
import com.linkedin.metadata.entity.ebean.PlainAspectTableResolver;
import com.linkedin.metadata.entity.storage.PrimaryStorageTestUtils;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.DocumentAuthorizationUtils;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextAuthorizer;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Database;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * With a request that hides entities the caller can't view, reads of such an entity answer exactly
 * as reads of an entity that was never written.
 */
public class EntityServiceHidesUnviewableTest {

  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:actor");
  private static final Urn HIDDEN = UrnUtils.getUrn("urn:li:corpuser:hidden");
  private static final Urn VISIBLE = UrnUtils.getUrn("urn:li:corpuser:visible");
  private static final Urn MISSING = UrnUtils.getUrn("urn:li:corpuser:missing");
  private static final AuditStamp AUDIT_STAMP = AspectGenerationUtils.createAuditStamp();
  private static final String INFO = Constants.CORP_USER_INFO_ASPECT_NAME;

  private EntityServiceImpl entityService;
  private OperationContext systemContext;
  private final AtomicInteger authorizerReadsOfActor = new AtomicInteger();

  @BeforeMethod
  public void setup() {
    Database server =
        EbeanTestUtils.createTestServer(EntityServiceHidesUnviewableTest.class.getSimpleName());
    EbeanAspectDao aspectDao =
        new EbeanAspectDao(
            PrimaryStorageTestUtils.ebeanResolver(server),
            EbeanConfiguration.testDefault,
            null,
            List.of(),
            null,
            new PlainAspectTableResolver(),
            new PassThroughScopedTransactionFactory(server));
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    entityService =
        new EntityServiceImpl(
            aspectDao,
            mock(EventProducer.class),
            preProcessHooks,
            new EntityServiceConfiguration().setAlwaysEmitChangeLog(false),
            mock(MetricUtils.class));
    entityService.setUpdateIndicesService(mock(UpdateIndicesService.class));

    EntityRegistry registry = TestOperationContexts.defaultEntityRegistry();
    systemContext =
        TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(true).build())
                    .build(),
            null,
            null,
            () -> registry,
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(
                        EntityServiceAspectRetriever.builder()
                            .entityService(entityService)
                            .entityRegistry(registry)
                            .build())
                    .cachingAspectRetriever(
                        TestOperationContexts.emptyActiveUsersAspectRetriever(() -> registry))
                    .graphRetriever(GraphRetriever.EMPTY)
                    .searchRetriever(SearchRetriever.EMPTY)
                    .build(),
            null,
            opContext ->
                ((EntityServiceAspectRetriever) opContext.getAspectRetriever())
                    .setSystemOperationContext(opContext),
            null);

    for (Urn urn : List.of(ACTOR, HIDDEN, VISIBLE)) {
      ingest(systemContext, urn, AspectGenerationUtils.createCorpUserInfo(urn.getId() + "@x.io"));
    }
  }

  @Test
  public void testLatestAspectsOfHiddenReadAsMissing() {
    OperationContext query = session(GraphQLOperationKind.QUERY);

    Map<Urn, List<RecordTemplate>> result =
        entityService.getLatestAspects(query, Set.of(HIDDEN, MISSING, VISIBLE), Set.of(INFO), true);

    assertEquals(aspectNames(result.get(HIDDEN)), aspectNames(result.get(MISSING)));
    assertEquals(aspectNames(result.get(HIDDEN)), Set.of(Constants.CORP_USER_KEY_ASPECT_NAME));
    assertTrue(aspectNames(result.get(VISIBLE)).contains(INFO));
    assertTrue(
        entityService
            .getLatestAspects(query, Set.of(HIDDEN), Set.of(INFO), false)
            .get(HIDDEN)
            .isEmpty());
    assertNull(entityService.getLatestAspect(query, HIDDEN, INFO));
    assertTrue(entityService.getLatestSystemAspect(query, HIDDEN, INFO).isEmpty());
  }

  @Test
  public void testEntitiesV2OfHiddenReadAsMissing() throws Exception {
    OperationContext query = session(GraphQLOperationKind.QUERY);

    Map<Urn, EntityResponse> result =
        entityService.getEntitiesV2(
            query, Constants.CORP_USER_ENTITY_NAME, Set.of(HIDDEN, MISSING), Set.of(INFO), true);

    assertEquals(
        result.get(HIDDEN).getAspects().keySet(), result.get(MISSING).getAspects().keySet());
    assertEquals(
        result.get(HIDDEN).getAspects().keySet(), Set.of(Constants.CORP_USER_KEY_ASPECT_NAME));
    assertNull(entityService.getLatestEnvelopedAspect(query, "corpuser", HIDDEN, INFO));
  }

  @Test
  public void testVersionedReadsOfHiddenReadAsMissing() throws Exception {
    OperationContext query = session(GraphQLOperationKind.QUERY);

    Map<Urn, List<EnvelopedAspect>> enveloped =
        entityService.getEnvelopedVersionedAspects(
            query, Map.of(HIDDEN, Map.of(INFO, 0L), MISSING, Map.of(INFO, 0L)), true);
    assertEquals(envelopedNames(enveloped.get(HIDDEN)), envelopedNames(enveloped.get(MISSING)));

    Map<Urn, EntityResponse> versioned =
        entityService.getEntitiesVersionedV2(
            query,
            Set.of(
                new VersionedUrn().setUrn(HIDDEN).setVersionStamp(INFO + ":0"),
                new VersionedUrn().setUrn(MISSING).setVersionStamp(INFO + ":0")),
            Set.of(INFO),
            true);
    assertEquals(
        versioned.get(HIDDEN).getAspects().keySet(), versioned.get(MISSING).getAspects().keySet());

    assertEquals(
        entityService.getAspectVersionPair(query, HIDDEN, INFO, -1),
        entityService.getAspectVersionPair(query, MISSING, INFO, -1));
    assertEquals(
        entityService.getAspectVersionPair(query, HIDDEN, INFO, 0),
        entityService.getAspectVersionPair(query, MISSING, INFO, 0));
    assertNull(entityService.getVersionedAspect(query, HIDDEN, INFO, 0));
  }

  @Test
  public void testExistsOfHiddenReadsAsMissing() {
    OperationContext query = session(GraphQLOperationKind.QUERY);

    assertEquals(
        entityService.exists(query, List.of(HIDDEN, MISSING, VISIBLE), null, true, false),
        Set.of(VISIBLE));
    assertEquals(
        entityService.exists(query, List.of(HIDDEN), null, true, true),
        Set.of(HIDDEN),
        "A read for update sees every entity");
  }

  @Test
  public void testReadsWithoutTheFlagOrForUpdateSeeHiddenEntities() {
    OperationContext mutation = session(GraphQLOperationKind.MUTATION);
    OperationContext query = session(GraphQLOperationKind.QUERY);

    assertNotNull(entityService.getLatestAspect(mutation, HIDDEN, INFO));
    assertTrue(entityService.exists(mutation, List.of(HIDDEN), null, true, false).contains(HIDDEN));
    assertNotNull(entityService.getLatestAspect(query.withoutHidingUnviewable(), HIDDEN, INFO));
    assertTrue(
        entityService.getLatestAspectsForUrn(query, HIDDEN, Set.of(INFO), true).containsKey(INFO));
  }

  @Test
  public void testIngestOfHiddenEntityIsUnaffectedByTheFlag() {
    OperationContext query = session(GraphQLOperationKind.QUERY);

    ingest(query, HIDDEN, AspectGenerationUtils.createCorpUserInfo("changed@x.io"));
    ingest(query, HIDDEN, new Status().setRemoved(false));
    ingest(systemContext, VISIBLE, AspectGenerationUtils.createCorpUserInfo("changed@x.io"));

    assertEquals(
        ((CorpUserInfo) entityService.getLatestAspect(systemContext, HIDDEN, INFO)).getEmail(),
        "changed@x.io");
    assertNotNull(entityService.getLatestAspect(systemContext, HIDDEN, "status"));
    assertEquals(
        entityService.getAspectVersionPair(systemContext, HIDDEN, INFO, -1).getSecond(),
        entityService.getAspectVersionPair(systemContext, VISIBLE, INFO, -1).getSecond(),
        "The write saw the stored version, as a write without the flag does");
  }

  @Test
  public void testAuthorizerReadsOfTheActorAreNotHidden() {
    OperationContext query = session(GraphQLOperationKind.QUERY, ACTOR, HIDDEN);

    assertTrue(
        entityService
            .getLatestAspects(query, Set.of(ACTOR), Set.of(INFO), false)
            .get(ACTOR)
            .isEmpty());
    assertTrue(authorizerReadsOfActor.get() > 0);
  }

  @Test
  public void testBridgeDocumentReadsFollowItsSourceEntity() {
    Urn visibleBridge = UrnUtils.getUrn("urn:li:document:bridge-visible");
    Urn hiddenBridge = UrnUtils.getUrn("urn:li:document:bridge-hidden");
    ingestBridgeDocument(visibleBridge, VISIBLE);
    ingestBridgeDocument(hiddenBridge, HIDDEN);
    OperationContext query =
        session(GraphQLOperationKind.QUERY, true, HIDDEN, visibleBridge, hiddenBridge);

    Map<Urn, List<RecordTemplate>> result =
        entityService.getLatestAspects(
            query,
            Set.of(visibleBridge, hiddenBridge),
            Set.of(Constants.DOCUMENT_INFO_ASPECT_NAME),
            false);

    assertEquals(result.get(visibleBridge).size(), 1, "Its source entity is visible");
    assertTrue(result.get(hiddenBridge).isEmpty(), "Its source entity is hidden");
  }

  private void ingestBridgeDocument(Urn documentUrn, Urn sourceUrn) {
    ingest(
        systemContext,
        documentUrn,
        new DocumentInfo()
            .setCustomProperties(
                new StringMap(
                    Map.of(
                        DocumentAuthorizationUtils.BRIDGE_TYPE_PROPERTY,
                        sourceUrn.getEntityType(),
                        DocumentAuthorizationUtils.BRIDGE_SOURCE_ENTITY_PROPERTY,
                        sourceUrn.toString())))
            .setContents(new DocumentContents().setText("text"))
            .setStatus(new DocumentStatus().setState(DocumentState.PUBLISHED))
            .setCreated(AUDIT_STAMP)
            .setLastModified(AUDIT_STAMP));
    ingest(
        systemContext,
        documentUrn,
        new SubTypes()
            .setTypeNames(
                new StringArray(List.of(DocumentAuthorizationUtils.BRIDGE_DOCUMENT_SUBTYPE))));
  }

  private void ingest(OperationContext opContext, Urn urn, RecordTemplate aspect) {
    entityService.ingestAspects(
        opContext,
        urn,
        List.of(Pair.of(AspectGenerationUtils.getAspectName(aspect), aspect)),
        AUDIT_STAMP,
        AspectGenerationUtils.createSystemMetadata());
  }

  private OperationContext session(GraphQLOperationKind kind) {
    return session(kind, HIDDEN);
  }

  /**
   * A user session whose authorizer, like the real one, reads the actor's own entity through the
   * entity service with the context it is given, and denies only the given urns.
   */
  private OperationContext session(GraphQLOperationKind kind, Urn... denied) {
    return session(kind, false, denied);
  }

  private OperationContext session(
      GraphQLOperationKind kind, boolean denyPlatformPrivileges, Urn... denied) {
    Set<String> deniedUrns = Set.of(denied).stream().map(Urn::toString).collect(Collectors.toSet());
    Authorizer authorizer =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    when(((OperationContextAuthorizer) authorizer).authorize(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              OperationContext opContext = invocation.getArgument(2);
              if (!entityService
                  .getLatestAspects(opContext, Set.of(ACTOR), Set.of(INFO), false)
                  .get(ACTOR)
                  .isEmpty()) {
                authorizerReadsOfActor.incrementAndGet();
              }
              boolean deny =
                  request.getResourceSpec().isEmpty()
                      ? denyPlatformPrivileges
                      : deniedUrns.contains(request.getResourceSpec().get().getEntity());
              return new AuthorizationResult(
                  request,
                  deny ? AuthorizationResult.Type.DENY : AuthorizationResult.Type.ALLOW,
                  "");
            });
    return systemContext.asSession(
        RequestContext.TEST.build().toBuilder()
            .actorUrn(ACTOR.toString())
            .graphqlOperationKind(kind),
        authorizer,
        new Authentication(new Actor(ActorType.USER, ACTOR.getId()), ""));
  }

  private static Set<String> aspectNames(List<RecordTemplate> aspects) {
    return aspects.stream()
        .map(a -> a.schema().getName())
        .map(EntityServiceHidesUnviewableTest::aspectName)
        .collect(Collectors.toSet());
  }

  private static String aspectName(String schemaName) {
    return Character.toLowerCase(schemaName.charAt(0)) + schemaName.substring(1);
  }

  private static Set<String> envelopedNames(List<EnvelopedAspect> aspects) {
    return aspects.stream().map(EnvelopedAspect::getName).collect(Collectors.toSet());
  }
}
