package com.linkedin.metadata.kafka.hook.assertion;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionAnalyticsRunEvent;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * This hook is responsible for generating a summary on each Assertion event that details
 * information like it's last passing time, last error time, and last failure time.
 */
@Slf4j
@Component
@Import({
  AssertionServiceFactory.class,
  SystemAuthenticationFactory.class,
})
public class AssertionAnalyticsRunEventHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.RESTATE);
  private static final Set<String> SUPPORTED_UPDATE_ASPECTS =
      ImmutableSet.of(ASSERTION_RUN_EVENT_ASPECT_NAME);

  private final AssertionService assertionService;
  private final EntityClient entityClient;
  private final boolean isEnabled;
  private OperationContext systemOperationContext;

  @Getter private final String consumerGroupSuffix;

  @Autowired
  public AssertionAnalyticsRunEventHook(
      @Nonnull final AssertionService assertionService,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull @Value("${assertionAnalytics.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${assertionAnalytics.hook.consumerGroupSuffix}")
          String consumerGroupSuffix) {
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public AssertionAnalyticsRunEventHook(
      @Nonnull final AssertionService assertionService,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull Boolean isEnabled) {
    this(assertionService, entityClient, isEnabled, "");
  }

  @Override
  public AssertionAnalyticsRunEventHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized the assertion analytics un summary hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      log.debug("Urn {} received by Assertion Analytics Run Summary Hook.", event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, systemOperationContext.getEntityRegistry());
      handleAssertionRunCompleted(urn, extractAssertionRunEvent(event));
    }
  }

  /** Handle an assertion update by generating the corresponding Analytics Run Event. */
  private void handleAssertionRunCompleted(
      @Nonnull final Urn assertionUrn, @Nonnull final AssertionRunEvent runEvent) {

    AssertionInfo assertionInfo =
        assertionService.getAssertionInfo(systemOperationContext, assertionUrn);

    // If there isn't yet a summary aspect, add one.
    if (assertionInfo == null) {
      log.warn(
          "Failed to find an assertion info aspect for completed assertion urn {}. Skipping generating Assertion Analytics Run Event!",
          assertionUrn);
      return;
    }

    generateAssertionAnalyticsRunEvent(assertionUrn, runEvent, assertionInfo);
  }

  private void generateAssertionAnalyticsRunEvent(
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo assertionInfo) {

    AssertionAnalyticsRunEvent analyticsRunEvent = new AssertionAnalyticsRunEvent();
    analyticsRunEvent.setTimestampMillis(runEvent.getTimestampMillis());
    analyticsRunEvent.setAssertionUrn(assertionUrn);
    analyticsRunEvent.setResult(runEvent.getResult(GetMode.NULL), SetMode.IGNORE_NULL);
    analyticsRunEvent.setAsserteeUrn(runEvent.getAsserteeUrn(GetMode.NULL), SetMode.IGNORE_NULL);
    analyticsRunEvent.setStatus(runEvent.getStatus(GetMode.NULL), SetMode.IGNORE_NULL);
    analyticsRunEvent.setRunId(runEvent.getRunId());
    analyticsRunEvent.setType(assertionInfo.getType());
    populateAsserteeContext(
        runEvent.getAsserteeUrn(), assertionUrn, analyticsRunEvent, runEvent, assertionInfo);
    emitAssertionAnalyticsRunEvent(assertionUrn, analyticsRunEvent);
  }

  private void populateAsserteeContext(
      @Nonnull final Urn asserteeUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final AssertionAnalyticsRunEvent analyticsRunEvent,
      @Nonnull final AssertionRunEvent runEvent,
      @Nonnull final AssertionInfo assertionInfo) {

    try {
      // Simply fetch the entity one time, and then populate all the context.
      final EntityResponse entityResponse =
          this.entityClient.getV2(
              systemOperationContext,
              asserteeUrn.getEntityType(),
              asserteeUrn,
              Collections.emptySet());

      if (entityResponse != null && entityResponse.hasAspects()) {
        final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        populateAsserteeDataPlatformContext(analyticsRunEvent, aspectMap);
        populateAsserteeTagContext(analyticsRunEvent, aspectMap);
        populateAsserteeGlossaryTermContext(analyticsRunEvent, aspectMap);
        populateAsserteeDomainContext(analyticsRunEvent, aspectMap);
        populateAsserteeOwnershipContext(analyticsRunEvent, aspectMap);
        // populateAsserteeDataProductContext(analyticsRunEvent, aspectMap); TODO: This takes a bit
        // more work.
      } else {
        log.warn(
            "Failed to find assertee entity with urn {} for assertion {}. Skipping generating Assertion Analytics Run Event!",
            asserteeUrn,
            assertionUrn);
      }

    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to attach assertee context to Assertion Analytics Run Event! This may mean that analytics are incorrect for end users! Timestamp: %s, Assertion Urn: %s, Assertee Urn: %s",
              runEvent.getTimestampMillis(), assertionUrn, asserteeUrn),
          e);
    }
  }

  private void populateAsserteeDataPlatformContext(
      @Nonnull final AssertionAnalyticsRunEvent analyticsRunEvent,
      @Nonnull final EnvelopedAspectMap aspectMap) {
    DataPlatformInstance maybeDataPlatformInstance = extractDataPlatformInstance(aspectMap);
    if (maybeDataPlatformInstance != null) {
      analyticsRunEvent.setAsserteeDataPlatform(maybeDataPlatformInstance.getPlatform());
      if (maybeDataPlatformInstance.hasInstance()) {
        analyticsRunEvent.setAsserteeDataPlatformInstance(maybeDataPlatformInstance.getInstance());
      }
    }
  }

  @Nullable
  private DataPlatformInstance extractDataPlatformInstance(
      @Nonnull final EnvelopedAspectMap aspectMap) {
    if (aspectMap.containsKey(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
        && aspectMap.get(DATA_PLATFORM_INSTANCE_ASPECT_NAME) != null) {
      return new DataPlatformInstance(
          aspectMap.get(DATA_PLATFORM_INSTANCE_ASPECT_NAME).getValue().data());
    }
    ;
    return null;
  }

  private void populateAsserteeTagContext(
      @Nonnull final AssertionAnalyticsRunEvent analyticsRunEvent,
      @Nonnull final EnvelopedAspectMap aspectMap) {
    GlobalTags maybeGlobalTags = extractGlobalTags(aspectMap);
    if (maybeGlobalTags != null) {
      analyticsRunEvent.setAsserteeTags(
          new UrnArray(
              maybeGlobalTags.getTags().stream()
                  .map(tagAssociation -> (Urn) tagAssociation.getTag())
                  .collect(Collectors.toList())));
    }
  }

  @Nullable
  private GlobalTags extractGlobalTags(@Nonnull final EnvelopedAspectMap aspectMap) {
    if (aspectMap.containsKey(GLOBAL_TAGS_ASPECT_NAME)
        && aspectMap.get(GLOBAL_TAGS_ASPECT_NAME) != null) {
      return new GlobalTags(aspectMap.get(GLOBAL_TAGS_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  private void populateAsserteeGlossaryTermContext(
      @Nonnull final AssertionAnalyticsRunEvent analyticsRunEvent,
      @Nonnull final EnvelopedAspectMap aspectMap) {
    GlossaryTerms maybeGlossaryTerms = extractGlossaryTerms(aspectMap);
    if (maybeGlossaryTerms != null) {
      analyticsRunEvent.setAsserteeGlossaryTerms(
          new UrnArray(
              maybeGlossaryTerms.getTerms().stream()
                  .map(termAssociation -> (Urn) termAssociation.getUrn())
                  .collect(Collectors.toList())));
    }
  }

  @Nullable
  private GlossaryTerms extractGlossaryTerms(@Nonnull final EnvelopedAspectMap aspectMap) {
    if (aspectMap.containsKey(GLOSSARY_TERMS_ASPECT_NAME)
        && aspectMap.get(GLOSSARY_TERMS_ASPECT_NAME) != null) {
      return new GlossaryTerms(aspectMap.get(GLOSSARY_TERMS_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  private void populateAsserteeDomainContext(
      @Nonnull final AssertionAnalyticsRunEvent analyticsRunEvent,
      @Nonnull final EnvelopedAspectMap aspectMap) {
    Domains maybeDomains = extractDomains(aspectMap);
    if (maybeDomains != null) {
      analyticsRunEvent.setAsserteeDomains(maybeDomains.getDomains());
    }
  }

  @Nullable
  private Domains extractDomains(@Nonnull final EnvelopedAspectMap aspectMap) {
    if (aspectMap.containsKey(DOMAINS_ASPECT_NAME) && aspectMap.get(DOMAINS_ASPECT_NAME) != null) {
      return new Domains(aspectMap.get(DOMAINS_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  private void populateAsserteeOwnershipContext(
      @Nonnull final AssertionAnalyticsRunEvent analyticsRunEvent,
      @Nonnull final EnvelopedAspectMap aspectMap) {
    Ownership maybeOwnership = extractOwnership(aspectMap);
    if (maybeOwnership != null) {
      analyticsRunEvent.setAsserteeOwners(
          new UrnArray(
              maybeOwnership.getOwners().stream()
                  .map(Owner::getOwner)
                  .collect(Collectors.toList())));
    }
  }

  @Nullable
  private Ownership extractOwnership(@Nonnull final EnvelopedAspectMap aspectMap) {
    if (aspectMap.containsKey(OWNERSHIP_ASPECT_NAME)
        && aspectMap.get(OWNERSHIP_ASPECT_NAME) != null) {
      return new Ownership(aspectMap.get(OWNERSHIP_ASPECT_NAME).getValue().data());
    }
    return null;
  }

  private void emitAssertionAnalyticsRunEvent(
      @Nonnull final Urn assertionUrn, @Nonnull final AssertionAnalyticsRunEvent runEvent) {
    try {
      this.entityClient.ingestProposal(
          systemOperationContext,
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, ASSERTION_ANALYTICS_RUN_EVENT_ASPECT_NAME, runEvent),
          true);
    } catch (Exception e) {
      log.error("Failed to patch assertion run summary! Skipping updating the summary", e);
    }
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the
   * assertion status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isAssertionRunCompleteEvent(event);
  }

  /**
   * Returns true if the event represents an assertion run event, which may mean a status change.
   */
  private boolean isAssertionRunCompleteEvent(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && SUPPORTED_UPDATE_ASPECTS.contains(event.getAspectName())
        && isAssertionRunCompleted(event);
  }

  private boolean isAssertionRunCompleted(@Nonnull final MetadataChangeLog event) {
    final AssertionRunEvent runEvent = extractAssertionRunEvent(event);
    return AssertionRunStatus.COMPLETE.equals(runEvent.getStatus()) && runEvent.hasResult();
  }

  @Nonnull
  private AssertionRunEvent extractAssertionRunEvent(@Nonnull final MetadataChangeLog event) {
    return GenericRecordUtils.deserializeAspect(
        event.getAspect().getValue(), event.getAspect().getContentType(), AssertionRunEvent.class);
  }
}
