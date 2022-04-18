package com.linkedin.metadata.kafka.hook.event;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.differ.Aspect;
import com.linkedin.metadata.timeline.differ.AspectDifferRegistry;
import com.linkedin.metadata.timeline.differ.AspectDiffer;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.platform.event.v1.Parameters;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;


/**
 * A {@link MetadataChangeLogHook} responsible for generating Entity Change Events
 * to the Platform Events topic.
 */
@Slf4j
@Component
@Import({
    AspectDifferRegistry.class,
    EntityRegistryFactory.class,
    RestliEntityClientFactory.class,
    SystemAuthenticationFactory.class
})
public class EntityChangeEventHook implements MetadataChangeLogHook {

  /**
   * The list of aspects that are supported for generating semantic change events.
   */
  private static final Set<String> SUPPORTED_ASPECT_NAMES = ImmutableSet.of(
      Constants.GLOBAL_TAGS_ASPECT_NAME,
      Constants.GLOSSARY_TERMS_ASPECT_NAME,
      Constants.OWNERSHIP_ASPECT_NAME,
      Constants.DOMAINS_ASPECT_NAME,
      Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
      Constants.SCHEMA_METADATA_ASPECT_NAME,
      // Entity Lifecycle Event
      Constants.DATASET_KEY_ASPECT_NAME,
      Constants.DASHBOARD_KEY_ASPECT_NAME,
      Constants.CHART_KEY_ASPECT_NAME,
      Constants.CONTAINER_KEY_ASPECT_NAME,
      Constants.DATA_FLOW_KEY_ASPECT_NAME,
      Constants.DATA_JOB_KEY_ASPECT_NAME,
      Constants.GLOSSARY_TERM_KEY_ASPECT_NAME,
      Constants.TAG_KEY_ASPECT_NAME,
      Constants.STATUS_ASPECT_NAME,
      Constants.DEPRECATION_ASPECT_NAME
  );
  private final AspectDifferRegistry _aspectDifferRegistry;
  private final EntityClient _entityClient;
  private final Authentication _systemAuthentication;
  private final EntityRegistry _entityRegistry;

  @Autowired
  public EntityChangeEventHook(
      @Nonnull final AspectDifferRegistry aspectDifferRegistry,
      @Nonnull final RestliEntityClient entityClient,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull final EntityRegistry entityRegistry) {
    _aspectDifferRegistry = Objects.requireNonNull(aspectDifferRegistry);
    _entityClient = Objects.requireNonNull(entityClient);
    _systemAuthentication = Objects.requireNonNull(systemAuthentication);
    _entityRegistry = Objects.requireNonNull(entityRegistry);
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog logEvent) {
    if (isEligibleForProcessing(logEvent)) {
      // Steps:
      // 1. Parse the old and new aspect.
      // 2. Find and invoke a differ.
      // 3. Sink the output of the differ to a specific PDL change event.
      final AspectSpec aspectSpec = _entityRegistry
          .getEntitySpec(logEvent.getEntityType())
          .getAspectSpec(logEvent.getAspectName());

      assert aspectSpec != null;

      final RecordTemplate fromAspect = logEvent.getPreviousAspectValue() != null
          ? GenericRecordUtils.deserializeAspect(
              logEvent.getPreviousAspectValue().getValue(),
              logEvent.getPreviousAspectValue().getContentType(),
              aspectSpec)
          : null;

      final RecordTemplate toAspect = logEvent.getAspect() != null
          ? GenericRecordUtils.deserializeAspect(
          logEvent.getAspect().getValue(),
          logEvent.getAspect().getContentType(),
          aspectSpec)
          : null;

      final List<ChangeEvent> changeEvents = generateChangeEvents(
          logEvent.getEntityUrn(),
          logEvent.getEntityType(),
          logEvent.getAspectName(),
          createAspect(fromAspect, logEvent.getPreviousSystemMetadata()),
          createAspect(toAspect, logEvent.getSystemMetadata()),
          logEvent.getCreated()
      );

      // Iterate through each transaction, emit change events as platform events.
      for (final ChangeEvent event : changeEvents) {
        try {
          PlatformEvent platformEvent = buildPlatformEvent(event);
          emitPlatformEvent(platformEvent);
          log.info("Successfully emitted change event. category: {}, operation: {}, entity urn: {}",
              event.getCategory(),
              event.getOperation(),
              event.getEntityUrn());
        } catch (Exception e) {
          // TODO: Implement a dead letter queue in the case that an event cannot be sinked.
          // For now, simply log the entire payload as a debug log.
          log.error(String.format("Caught exception while attempting to emit semantic Change Event! event: %s", event.toString()), e);
        }
      }
    }
  }

  private  <T extends RecordTemplate> List<ChangeEvent> generateChangeEvents(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final Aspect from,
      @Nonnull final Aspect to,
      @Nonnull AuditStamp auditStamp
  ) {
    final List<AspectDiffer<T>> aspectDiffers = _aspectDifferRegistry.getAspectDiffers(aspectName)
        .stream()
        .map(differ -> (AspectDiffer<T>) differ) // Note: Assumes that correct types have been registered for the aspect.
        .collect(Collectors.toList());
    final List<ChangeEvent> allChangeEvents = new ArrayList<>();
    for (AspectDiffer<T> aspectDiffer : aspectDiffers) {
      allChangeEvents.addAll(aspectDiffer.getChangeEvents(urn, entityName, aspectName, from, to, auditStamp));
    }
    return allChangeEvents;
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog log) {
    return SUPPORTED_ASPECT_NAMES.contains(log.getAspectName());
  }

  // TODO: Partition either by URN or by type of change event.
  private void emitPlatformEvent(@Nonnull final PlatformEvent event) throws Exception {
    _entityClient.producePlatformEvent(
        Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME,
        null,
        event,
        _systemAuthentication
    );
  }

  private PlatformEvent buildPlatformEvent(final ChangeEvent rawChangeEvent) {
    // 1. Convert raw Change Event to a serialized change event.
    RecordTemplate changeEvent = convertRawEventToChangeEvent(rawChangeEvent);
    // 2. Build platform event
    PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(new PlatformEventHeader().setTimestampMillis(System.currentTimeMillis()));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(changeEvent));
    return platformEvent;
  }

  /**
   * Thin mapping from internal Timeline API {@link ChangeEvent} to Kafka Platform Event {@link ChangeEvent}, which serves as a public
   * API for outbound consumption.
   */
  private RecordTemplate convertRawEventToChangeEvent(final ChangeEvent rawChangeEvent) {
    com.linkedin.platform.event.v1.EntityChangeEvent changeEvent = new com.linkedin.platform.event.v1.EntityChangeEvent();
    log.info(String.format("Attempting to convert %s", rawChangeEvent));
    try {
      Urn entityUrn = Urn.createFromString(rawChangeEvent.getEntityUrn());
      changeEvent.setCategory(rawChangeEvent.getCategory().name());
      changeEvent.setOperation(rawChangeEvent.getOperation().name());
      changeEvent.setEntityType(entityUrn.getEntityType());
      changeEvent.setEntityUrn(entityUrn);
      changeEvent.setModifier(rawChangeEvent.getModifier(), SetMode.IGNORE_NULL);
      changeEvent.setAuditStamp(rawChangeEvent.getAuditStamp());
      changeEvent.setVersion(0);
      if (changeEvent.hasParameters()) {
        changeEvent.setParameters(
            // This map should ideally contain only primitives at the leaves - integers, floats, booleans, strings.
            new Parameters(new DataMap(rawChangeEvent.getParameters()))
        );
      }
      return changeEvent;
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert raw change event into PDL change", e);
    }
  }

  private Aspect createAspect(@Nullable final RecordTemplate value, @Nullable final SystemMetadata systemMetadata) {
    return new Aspect(
        value,
        systemMetadata
    );
  }
}
