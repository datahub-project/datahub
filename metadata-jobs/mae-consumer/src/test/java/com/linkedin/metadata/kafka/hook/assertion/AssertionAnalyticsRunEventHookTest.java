package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.ASSERTION_ANALYTICS_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_RUN_EVENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.entity.AspectUtils.buildMetadataChangeProposal;
import static com.linkedin.metadata.kafka.hook.EntityRegistryTestUtil.ENTITY_REGISTRY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.assertion.AssertionAnalyticsRunEvent;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionResultType;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AssertionAnalyticsRunEventHookTest {
  private static final Urn TEST_ASSERTION_URN = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn TEST_DATA_PLATFORM_URN = UrnUtils.getUrn("urn:li:dataPlatform:test");
  private static final Urn TEST_DATA_PLATFORM_INSTANCE_URN =
      UrnUtils.getUrn("urn:li:dataPlatformInstance:(urn:li:dataPlatform:test,name)");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,name,PROD)");
  private static final Urn TEST_GLOSSARY_TERM_URN_1 = UrnUtils.getUrn("urn:li:glossaryTerm:1");
  private static final Urn TEST_GLOSSARY_TERM_URN_2 = UrnUtils.getUrn("urn:li:glossaryTerm:2");
  private static final Urn TEST_TAG_URN_1 = UrnUtils.getUrn("urn:li:tag:1");
  private static final Urn TEST_TAG_URN_2 = UrnUtils.getUrn("urn:li:tag:2");
  private static final Urn TEST_DOMAIN_URN_1 = UrnUtils.getUrn("urn:li:domain:1");
  private static final Urn TEST_DOMAIN_URN_2 = UrnUtils.getUrn("urn:li:domain:2");
  private static final Urn TEST_OWNER_URN_1 = UrnUtils.getUrn("urn:li:corpuser:1");
  private static final Urn TEST_OWNER_URN_2 = UrnUtils.getUrn("urn:li:corpGroup:2");

  private static OperationContext mockOperationContext() {
    return TestOperationContexts.userContextNoSearchAuthorization(ENTITY_REGISTRY);
  }

  @Test
  public void testInvokeNotEnabled() throws Exception {
    AssertionService assertionService = mock(AssertionService.class);
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    final AssertionAnalyticsRunEventHook hook =
        new AssertionAnalyticsRunEventHook(assertionService, entityClient, false);
    hook.init(mockOperationContext());

    final MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);

    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionEntityResponse(any(OperationContext.class), Mockito.any(Urn.class));
  }

  @Test
  public void testInvokeNotEligibleChange() throws Exception {
    AssertionService assertionService = mock(AssertionService.class);
    SystemEntityClient entityClient = mock(SystemEntityClient.class);

    final AssertionAnalyticsRunEventHook hook =
        new AssertionAnalyticsRunEventHook(assertionService, entityClient, true);
    hook.init(mockOperationContext());

    // Case 1: Incorrect aspect --- Assertion Info
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN, ASSERTION_INFO_ASPECT_NAME, ChangeType.UPSERT, new AssertionInfo());
    hook.invoke(event);

    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionEntityResponse(any(OperationContext.class), Mockito.any(Urn.class));

    // Case 2: Run Event But Delete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.DELETE,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionEntityResponse(any(OperationContext.class), Mockito.any(Urn.class));

    // Case 3: Status aspect but for the wrong entity type
    event =
        buildMetadataChangeLog(
            TEST_DATASET_URN,
            STATUS_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionEntityResponse(any(OperationContext.class), Mockito.any(Urn.class));

    // Case 4: Run event but not complete
    event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.$UNKNOWN, AssertionResultType.SUCCESS));
    hook.invoke(event);
    Mockito.verify(assertionService, Mockito.times(0))
        .getAssertionEntityResponse(any(OperationContext.class), Mockito.any(Urn.class));
  }

  @Test
  public void testInvokeAssertionRunEventEntityHasAllAspects() throws Exception {

    AssertionService assertionService = mock(AssertionService.class);
    Mockito.when(
            assertionService.getAssertionInfo(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new AssertionInfo()
                .setType(AssertionType.FRESHNESS)
                .setFreshnessAssertion(
                    new FreshnessAssertionInfo()
                        .setType(FreshnessAssertionType.DATASET_CHANGE)
                        .setEntity(TEST_DATASET_URN)));

    DataPlatformInstance entityDataPlatformInstance = new DataPlatformInstance();
    entityDataPlatformInstance.setPlatform(TEST_DATA_PLATFORM_URN);
    entityDataPlatformInstance.setInstance(TEST_DATA_PLATFORM_INSTANCE_URN);

    GlossaryTerms entityGlossaryTerms = new GlossaryTerms();
    entityGlossaryTerms.setTerms(
        new GlossaryTermAssociationArray(
            ImmutableList.of(
                new GlossaryTermAssociation()
                    .setUrn(new GlossaryTermUrn(TEST_GLOSSARY_TERM_URN_1.toString())),
                new GlossaryTermAssociation()
                    .setUrn(new GlossaryTermUrn(TEST_GLOSSARY_TERM_URN_2.toString())))));

    GlobalTags entityTags = new GlobalTags();
    entityTags.setTags(
        new TagAssociationArray(
            ImmutableList.of(
                new TagAssociation().setTag(new TagUrn(TEST_TAG_URN_1.toString())),
                new TagAssociation().setTag(new TagUrn(TEST_TAG_URN_2.toString())))));

    Domains entityDomains = new Domains();
    entityDomains.setDomains(new UrnArray(ImmutableList.of(TEST_DOMAIN_URN_1, TEST_DOMAIN_URN_2)));

    Ownership entityOwnership = new Ownership();
    entityOwnership.setOwners(
        new OwnerArray(
            ImmutableList.of(
                new Owner().setOwner(TEST_OWNER_URN_1), new Owner().setOwner(TEST_OWNER_URN_2))));

    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(DATASET_ENTITY_NAME),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(Collections.emptySet())))
        .thenReturn(
            new EntityResponse()
                .setEntityName(DATASET_ENTITY_NAME)
                .setUrn(TEST_DATASET_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            DATA_PLATFORM_INSTANCE_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(entityDataPlatformInstance.data())),
                            GLOSSARY_TERMS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(GLOSSARY_TERMS_ASPECT_NAME)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(entityGlossaryTerms.data())),
                            GLOBAL_TAGS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(GLOBAL_TAGS_ASPECT_NAME)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(entityTags.data())),
                            DOMAINS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(DOMAINS_ASPECT_NAME)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(entityDomains.data())),
                            OWNERSHIP_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(DOMAINS_ASPECT_NAME)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(entityOwnership.data()))))));

    final AssertionAnalyticsRunEventHook hook =
        new AssertionAnalyticsRunEventHook(assertionService, entityClient, true);
    hook.init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the Dataset information correctly.
    Mockito.verify(entityClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(Collections.emptySet()));

    // Verify that the analytics run event was ingested correctly.
    AssertionAnalyticsRunEvent expectedRunEvent =
        buildAssertionAnalyticsRunEvent(
            TEST_ASSERTION_URN,
            AssertionRunStatus.COMPLETE,
            AssertionResultType.SUCCESS,
            entityGlossaryTerms,
            entityDataPlatformInstance,
            entityTags,
            entityDomains,
            entityOwnership);

    MetadataChangeProposal expected =
        buildMetadataChangeProposal(
            TEST_ASSERTION_URN, ASSERTION_ANALYTICS_RUN_EVENT_ASPECT_NAME, expectedRunEvent);

    Mockito.verify(entityClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(new AssertionAnalyticsRunEventHookRunEventMatcher(expected)),
            Mockito.anyBoolean());
  }

  @Test
  public void testInvokeAssertionRunEventEntityHasNoAspects() throws Exception {

    AssertionService assertionService = mock(AssertionService.class);
    Mockito.when(
            assertionService.getAssertionInfo(
                any(OperationContext.class), Mockito.eq(TEST_ASSERTION_URN)))
        .thenReturn(
            new AssertionInfo()
                .setType(AssertionType.FRESHNESS)
                .setFreshnessAssertion(
                    new FreshnessAssertionInfo()
                        .setType(FreshnessAssertionType.DATASET_CHANGE)
                        .setEntity(TEST_DATASET_URN)));

    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    Mockito.when(
            entityClient.getV2(
                any(OperationContext.class),
                Mockito.eq(DATASET_ENTITY_NAME),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(Collections.emptySet())))
        .thenReturn(
            new EntityResponse()
                .setEntityName(DATASET_ENTITY_NAME)
                .setUrn(TEST_DATASET_URN)
                .setAspects(new EnvelopedAspectMap(Collections.emptyMap())));

    final AssertionAnalyticsRunEventHook hook =
        new AssertionAnalyticsRunEventHook(assertionService, entityClient, true);
    hook.init(mockOperationContext());

    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_ASSERTION_URN,
            ASSERTION_RUN_EVENT_ASPECT_NAME,
            ChangeType.UPSERT,
            buildAssertionRunEvent(
                TEST_ASSERTION_URN, AssertionRunStatus.COMPLETE, AssertionResultType.SUCCESS));

    hook.invoke(event);

    // Ensure that we looked up the Dataset information correctly.
    Mockito.verify(entityClient, Mockito.times(1))
        .getV2(
            any(OperationContext.class),
            Mockito.eq(DATASET_ENTITY_NAME),
            Mockito.eq(TEST_DATASET_URN),
            Mockito.eq(Collections.emptySet()));

    // Verify that the analytics run event was ingested correctly.
    AssertionAnalyticsRunEvent expectedRunEvent =
        buildAssertionAnalyticsRunEvent(
            TEST_ASSERTION_URN,
            AssertionRunStatus.COMPLETE,
            AssertionResultType.SUCCESS,
            null,
            null,
            null,
            null,
            null);

    MetadataChangeProposal expected =
        buildMetadataChangeProposal(
            TEST_ASSERTION_URN, ASSERTION_ANALYTICS_RUN_EVENT_ASPECT_NAME, expectedRunEvent);

    Mockito.verify(entityClient, Mockito.times(1))
        .ingestProposal(
            any(OperationContext.class),
            Mockito.argThat(new AssertionAnalyticsRunEventHookRunEventMatcher(expected)),
            Mockito.anyBoolean());
  }

  private AssertionRunEvent buildAssertionRunEvent(
      final Urn urn, final AssertionRunStatus status, final AssertionResultType resultType) {
    AssertionRunEvent event = new AssertionRunEvent();
    event.setTimestampMillis(1L);
    event.setAssertionUrn(urn);
    event.setAsserteeUrn(TEST_DATASET_URN);
    event.setStatus(status);
    event.setRunId("Some value");
    event.setResult(new AssertionResult().setType(resultType).setRowCount(0L));
    return event;
  }

  private AssertionAnalyticsRunEvent buildAssertionAnalyticsRunEvent(
      final Urn urn,
      final AssertionRunStatus status,
      final AssertionResultType resultType,
      @Nullable GlossaryTerms glossaryTerms,
      @Nullable DataPlatformInstance dataPlatformInstance,
      @Nullable GlobalTags globalTags,
      @Nullable Domains domains,
      @Nullable Ownership ownership) {
    AssertionAnalyticsRunEvent event = new AssertionAnalyticsRunEvent();
    event.setType(AssertionType.FRESHNESS);
    event.setTimestampMillis(1L);
    event.setAssertionUrn(urn);
    event.setAsserteeUrn(TEST_DATASET_URN);
    event.setStatus(status);
    event.setRunId("Some value");
    event.setResult(new AssertionResult().setType(resultType).setRowCount(0L));
    if (glossaryTerms != null) {
      event.setAsserteeGlossaryTerms(
          new UrnArray(
              glossaryTerms.getTerms().stream()
                  .map(GlossaryTermAssociation::getUrn)
                  .collect(Collectors.toList())));
    }
    if (dataPlatformInstance != null) {
      event.setAsserteeDataPlatform(dataPlatformInstance.getPlatform());
      if (dataPlatformInstance.hasInstance()) {
        event.setAsserteeDataPlatformInstance(dataPlatformInstance.getInstance());
      }
    }
    if (globalTags != null) {
      event.setAsserteeTags(
          new UrnArray(
              globalTags.getTags().stream()
                  .map(TagAssociation::getTag)
                  .collect(Collectors.toList())));
    }
    if (domains != null) {
      event.setAsserteeDomains(domains.getDomains());
    }
    if (ownership != null) {
      event.setAsserteeOwners(
          new UrnArray(
              ownership.getOwners().stream().map(Owner::getOwner).collect(Collectors.toList())));
    }
    return event;
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(urn.getEntityType());
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    return event;
  }
}
