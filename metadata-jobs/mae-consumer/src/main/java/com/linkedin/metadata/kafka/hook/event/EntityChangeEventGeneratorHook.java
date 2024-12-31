package com.linkedin.metadata.kafka.hook.event;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.eventgenerator.Aspect;
import com.linkedin.metadata.timeline.eventgenerator.ChangeEventGeneratorUtils;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGenerator;
import com.linkedin.metadata.timeline.eventgenerator.EntityChangeEventGeneratorRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.platform.event.v1.Parameters;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * A {@link MetadataChangeLogHook} responsible for generating Entity Change Events to the Platform
 * Events topic.
 */
@Slf4j
@Component
@Import({EntityRegistryFactory.class})
public class EntityChangeEventGeneratorHook implements MetadataChangeLogHook {

  /** The list of aspects that are supported for generating semantic change events. */
  private static final Set<String> SUPPORTED_ASPECT_NAMES =
      ImmutableSet.of(
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.DOMAINS_ASPECT_NAME,
          Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
          Constants.SCHEMA_METADATA_ASPECT_NAME,
          Constants.DEPRECATION_ASPECT_NAME,
          Constants.DATASET_PROPERTIES_ASPECT_NAME,
          Constants.EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
          Constants.ASSERTION_RUN_EVENT_ASPECT_NAME,
          Constants.DATA_PROCESS_INSTANCE_RUN_EVENT_ASPECT_NAME,
          Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
          Constants.BUSINESS_ATTRIBUTE_ASPECT,

          // Entity Lifecycle Event
          Constants.DATASET_KEY_ASPECT_NAME,
          Constants.DASHBOARD_KEY_ASPECT_NAME,
          Constants.CHART_KEY_ASPECT_NAME,
          Constants.CONTAINER_KEY_ASPECT_NAME,
          Constants.DATA_FLOW_KEY_ASPECT_NAME,
          Constants.DATA_JOB_KEY_ASPECT_NAME,
          Constants.GLOSSARY_TERM_KEY_ASPECT_NAME,
          Constants.DOMAIN_KEY_ASPECT_NAME,
          Constants.TAG_KEY_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME,
          Constants.BUSINESS_ATTRIBUTE_KEY_ASPECT_NAME);

  /** The list of change types that are supported for generating semantic change events. */
  private static final Set<String> SUPPORTED_OPERATIONS =
      ImmutableSet.of("CREATE", "UPSERT", "DELETE");

  private final EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry;
  private final OperationContext systemOperationContext;
  private final SystemEntityClient systemEntityClient;
  private final Boolean isEnabled;
  @Getter private final String consumerGroupSuffix;
  private final List<String> entityExclusions;

  @Autowired
  public EntityChangeEventGeneratorHook(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull @Qualifier("entityChangeEventGeneratorRegistry")
          final EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull @Value("${entityChangeEvents.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${entityChangeEvents.consumerGroupSuffix}") String consumerGroupSuffix,
      @Nonnull @Value("#{'${entityChangeEvents.entityExclusions}'.split(',')}")
          List<String> entityExclusions) {
    this.systemOperationContext = systemOperationContext;
    this.entityChangeEventGeneratorRegistry =
        Objects.requireNonNull(entityChangeEventGeneratorRegistry);
    this.systemEntityClient = Objects.requireNonNull(entityClient);
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
    this.entityExclusions = entityExclusions;
  }

  @VisibleForTesting
  public EntityChangeEventGeneratorHook(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull final EntityChangeEventGeneratorRegistry entityChangeEventGeneratorRegistry,
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull Boolean isEnabled) {
    this(
        systemOperationContext,
        entityChangeEventGeneratorRegistry,
        entityClient,
        isEnabled,
        "",
        Collections.emptyList());
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog logEvent) throws Exception {
    if (isEligibleForProcessing(logEvent)) {
      // Steps:
      // 1. Parse the old and new aspect.
      // 2. Find and invoke a EntityChangeEventGenerator.
      // 3. Sink the output of the EntityChangeEventGenerator to a specific PDL change event.
      final AspectSpec aspectSpec =
          systemOperationContext
              .getEntityRegistry()
              .getEntitySpec(logEvent.getEntityType())
              .getAspectSpec(logEvent.getAspectName());

      assert aspectSpec != null;

      final RecordTemplate fromAspect =
          logEvent.getPreviousAspectValue() != null
              ? GenericRecordUtils.deserializeAspect(
                  logEvent.getPreviousAspectValue().getValue(),
                  logEvent.getPreviousAspectValue().getContentType(),
                  aspectSpec)
              : null;

      final RecordTemplate toAspect =
          logEvent.getAspect() != null
              ? GenericRecordUtils.deserializeAspect(
                  logEvent.getAspect().getValue(),
                  logEvent.getAspect().getContentType(),
                  aspectSpec)
              : null;

      final List<ChangeEvent> changeEvents =
          ChangeEventGeneratorUtils.generateChangeEvents(
              entityChangeEventGeneratorRegistry,
              logEvent.getEntityUrn(),
              logEvent.getEntityType(),
              logEvent.getAspectName(),
              createAspect(fromAspect, logEvent.getPreviousSystemMetadata()),
              createAspect(toAspect, logEvent.getSystemMetadata()),
              logEvent.getCreated());

      // Iterate through each transaction, emit change events as platform events.
      for (final ChangeEvent event : changeEvents) {
        PlatformEvent platformEvent = buildPlatformEvent(event);
        emitPlatformEvent(
            platformEvent,
            String.format(
                "%s-%s", Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME, event.getEntityUrn()));
        log.debug(
            "Successfully emitted change event. category: {}, operation: {}, entity urn: {}",
            event.getCategory(),
            event.getOperation(),
            event.getEntityUrn());
      }
    }
  }

  private <T extends RecordTemplate> List<ChangeEvent> generateChangeEvents(
      @Nonnull final Urn urn,
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      @Nonnull final Aspect from,
      @Nonnull final Aspect to,
      @Nonnull AuditStamp auditStamp) {
    final List<EntityChangeEventGenerator<T>> entityChangeEventGenerators =
        entityChangeEventGeneratorRegistry.getEntityChangeEventGenerators(aspectName).stream()
            // Note: Assumes that correct types have been registered for the aspect.
            .map(changeEventGenerator -> (EntityChangeEventGenerator<T>) changeEventGenerator)
            .collect(Collectors.toList());
    final List<ChangeEvent> allChangeEvents = new ArrayList<>();
    for (EntityChangeEventGenerator<T> entityChangeEventGenerator : entityChangeEventGenerators) {
      allChangeEvents.addAll(
          entityChangeEventGenerator.getChangeEvents(
              urn, entityName, aspectName, from, to, auditStamp));
    }
    return allChangeEvents;
  }

  private boolean isEligibleForProcessing(final MetadataChangeLog log) {
    return SUPPORTED_OPERATIONS.contains(log.getChangeType().toString())
        && SUPPORTED_ASPECT_NAMES.contains(log.getAspectName())
        && !entityExclusions.contains(log.getEntityType());
  }

  private void emitPlatformEvent(
      @Nonnull final PlatformEvent event, @Nonnull final String partitioningKey) throws Exception {
    systemEntityClient.producePlatformEvent(
        systemOperationContext, Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME, partitioningKey, event);
  }

  private PlatformEvent buildPlatformEvent(final ChangeEvent rawChangeEvent) {
    // 1. Convert raw Change Event to a serialized change event.
    RecordTemplate changeEvent = convertRawEventToChangeEvent(rawChangeEvent);
    // 2. Build platform event
    PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(Constants.CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(
        new PlatformEventHeader().setTimestampMillis(rawChangeEvent.getAuditStamp().getTime()));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(changeEvent));
    return platformEvent;
  }

  /**
   * Thin mapping from internal Timeline API {@link ChangeEvent} to Kafka Platform Event {@link
   * ChangeEvent}, which serves as a public API for outbound consumption.
   */
  private RecordTemplate convertRawEventToChangeEvent(final ChangeEvent rawChangeEvent) {
    com.linkedin.platform.event.v1.EntityChangeEvent changeEvent =
        new com.linkedin.platform.event.v1.EntityChangeEvent();
    log.debug(String.format("Attempting to convert %s", rawChangeEvent));
    try {
      Urn entityUrn = Urn.createFromString(rawChangeEvent.getEntityUrn());
      changeEvent.setEntityType(entityUrn.getEntityType());
      changeEvent.setEntityUrn(entityUrn);
      changeEvent.setCategory(rawChangeEvent.getCategory().name());
      changeEvent.setOperation(rawChangeEvent.getOperation().name());
      changeEvent.setModifier(rawChangeEvent.getModifier(), SetMode.IGNORE_NULL);
      changeEvent.setAuditStamp(rawChangeEvent.getAuditStamp());
      changeEvent.setVersion(0);
      if (rawChangeEvent.getParameters() != null) {
        // This map should ideally contain only primitives at the leaves - integers, floats,
        // booleans, strings.
        changeEvent.setParameters(new Parameters(new DataMap(rawChangeEvent.getParameters())));
      }
      return changeEvent;
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert raw change event into PDL change", e);
    }
  }

  private Aspect createAspect(
      @Nullable final RecordTemplate value, @Nullable final SystemMetadata systemMetadata) {
    return new Aspect(value, systemMetadata);
  }
}
