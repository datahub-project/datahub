package com.linkedin.metadata.kafka.hook.ingestion;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.ownership.OwnerServiceFactory;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.OwnerService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * This hook ensures that the actor who triggered an ingestion job is set as an owner of the
 * corresponding ingestion source.
 */
@Slf4j
@Component
@Import({OwnerServiceFactory.class, SystemAuthenticationFactory.class})
public class IngestionSetActorAsOwnerHook implements MetadataChangeLogHook {
  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.CREATE, ChangeType.UPSERT);
  private static final Set<String> SUPPORTED_INGESTION_SOURCE_TYPES =
      ImmutableSet.of(EXECUTION_REQUEST_SOURCE_CLI_INGESTION_SOURCE);

  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;
  private final OwnerService ownerService;

  private OperationContext systemOperationContext;

  /**
   * Constructs a new instance of IngestionSetActorAsOwnerHook.
   *
   * @param isEnabled whether this hook should be active
   * @param consumerGroupSuffix suffix used to differentiate consumer groups
   * @param ownerService service used to perform operations on owners
   */
  @Autowired
  public IngestionSetActorAsOwnerHook(
      @Nonnull @Value("${ingestion.setActorAsOwnerHook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${ingestion.setActorAsOwnerHook.consumerGroupSuffix}")
          String consumerGroupSuffix,
      @Nonnull final OwnerService ownerService) {
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
    this.ownerService = Objects.requireNonNull(ownerService, "ownerService is required");
  }

  @VisibleForTesting
  public IngestionSetActorAsOwnerHook(
      @Nonnull Boolean isEnabled, @Nonnull final OwnerService ownerService) {
    this(isEnabled, "", ownerService);
  }

  /**
   * Initializes the hook with the provided operation context.
   *
   * @param systemOperationContext the system-level operation context
   * @return this instance after initialization
   */
  @Override
  public IngestionSetActorAsOwnerHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  /**
   * Checks whether this hook is enabled.
   *
   * @return true if enabled, false otherwise
   */
  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  /**
   * Invokes the hook logic when a MetadataChangeLog event is received.
   *
   * @param event the MetadataChangeLog event to process
   */
  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) throws Exception {
    if (isEnabled() && isEligibleForProcessing(event)) {
      handleExecutionRequestInputUpdate(event);
    }
  }

  /**
   * Handles the update of an ExecutionRequestInput aspect by assigning the actor as an owner of the
   * ingestion source if they are not already one.
   *
   * @param event the MetadataChangeLog event containing the update
   */
  private void handleExecutionRequestInputUpdate(@Nonnull final MetadataChangeLog event)
      throws RuntimeException {
    try {
      final ExecutionRequestInput executionRequestInput =
          extractDeserializedExecutionRequestAspect(event);
      final String ingestionSourceType =
          extractTypeOfIngestionSourceFromExecutionRequestInput(executionRequestInput);

      if (!isIngestionSourceTypeSupported(ingestionSourceType)) {
        log.info(
            "Type of the ingestion source is not supported {}. Event: {}",
            ingestionSourceType,
            event);
        return;
      }

      final Urn actorUrn = extractActorUrnFromEvent(event);
      final Urn ingestionSourceUrn =
          extractIngestionSourceUrnFromExecutionRequestInput(executionRequestInput);

      log.info(
          "Start to set actor {} as owner of ingestion source {}", actorUrn, ingestionSourceUrn);

      final boolean isActorAlreadyOwnerOfIngestionSource =
          isActorOwnerOfIngestionSource(actorUrn, ingestionSourceUrn);

      if (!isActorAlreadyOwnerOfIngestionSource) {
        addActorAsOwnerOfIngestionSource(actorUrn, ingestionSourceUrn);
        log.info(
            "Actor {} has been successfully set as owner of ingestion source {}",
            actorUrn,
            ingestionSourceUrn);
      } else {
        log.info("Actor {} is already owner of {}", actorUrn, ingestionSourceUrn);
      }
    } catch (Exception e) {
      log.error("Can't set ingestion source owner from this event {}", event, e);
      throw new RuntimeException(
          String.format("Can't set ingestion source owner from this event %s", event), e);
    }
  }

  /**
   * Determines whether this hook should process the given event.
   *
   * @param event the MetadataChangeLog event to evaluate
   * @return true if the event is eligible for processing, false otherwise
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isExecutionRequestInputUpdated(event);
  }

  /**
   * Checks whether the event corresponds to an update of the ExecutionRequestInput aspect.
   *
   * @param event the MetadataChangeLog event to check
   * @return true if the event involves the ExecutionRequestInput aspect and a supported change type
   */
  private boolean isExecutionRequestInputUpdated(@Nonnull final MetadataChangeLog event) {
    return SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && EXECUTION_REQUEST_INPUT_ASPECT_NAME.equals(event.getAspectName());
  }

  /**
   * Extracts the actor URN from the event.
   *
   * @param event the MetadataChangeLog event to extract from
   * @return the actor URN
   * @throws RuntimeException if the actor cannot be extracted
   */
  private Urn extractActorUrnFromEvent(@Nonnull final MetadataChangeLog event)
      throws RuntimeException {
    if (event.getCreated() != null) {
      return event.getCreated().getActor();
    }
    throw new RuntimeException("Can't extract actor from event");
  }

  /**
   * Extracts the ingestion source URN from the event.
   *
   * @param executionRequestInput the ExecutionRequestInput aspect from event to extract from
   * @return the ingestion source URN
   * @throws RuntimeException if the URN cannot be extracted
   */
  private Urn extractIngestionSourceUrnFromExecutionRequestInput(
      @Nonnull final ExecutionRequestInput executionRequestInput) throws RuntimeException {
    Urn urn = executionRequestInput.getSource().getIngestionSource();
    if (urn == null) {
      throw new RuntimeException("Can't extract urn of ingestion source from event");
    }

    return urn;
  }

  /**
   * Extracts the ingestion source TYPE from the ExecutionRequestInput.
   *
   * @param executionRequestInput the ExecutionRequestInput aspect from event to extract from
   * @return the ingestion source TYPE
   */
  private String extractTypeOfIngestionSourceFromExecutionRequestInput(
      @Nonnull final ExecutionRequestInput executionRequestInput) {
    return executionRequestInput.getSource().getType();
  }

  /**
   * Checks whether a type of the ingestion source is supported
   *
   * @param ingestionSourceType the ingestion source TYPE
   * @return true if a type of the ingestion source is supported, false otherwise
   */
  private boolean isIngestionSourceTypeSupported(@Nonnull final String ingestionSourceType) {
    return SUPPORTED_INGESTION_SOURCE_TYPES.contains(ingestionSourceType);
  }

  /**
   * Deserializes the ExecutionRequestInput aspect from the event.
   *
   * @param event the MetadataChangeLog event containing the aspect
   * @return the deserialized ExecutionRequestInput object
   * @throws RuntimeException if deserialization fails or the aspect is missing
   */
  private ExecutionRequestInput extractDeserializedExecutionRequestAspect(
      @Nonnull final MetadataChangeLog event) throws RuntimeException {
    GenericAspect aspect = event.getAspect();

    if (aspect == null) {
      throw new RuntimeException("Can't extract aspect from event");
    }

    return GenericRecordUtils.deserializeAspect(
        aspect.getValue(), aspect.getContentType(), ExecutionRequestInput.class);
  }

  /**
   * Checks whether the specified actor is already an owner of the ingestion source.
   *
   * @param actorUrn the actor's URN
   * @param ingestionSourceUrn the ingestion source's URN
   * @return true if the actor is already an owner, false otherwise
   * @throws Exception if there's an issue retrieving ownership information
   */
  private boolean isActorOwnerOfIngestionSource(Urn actorUrn, Urn ingestionSourceUrn)
      throws Exception {
    List<Owner> owners = ownerService.getEntityOwners(systemOperationContext, ingestionSourceUrn);
    return owners.stream().anyMatch(owner -> owner.getOwner() == actorUrn);
  }

  /**
   * Adds the specified actor as an owner of the ingestion source.
   *
   * @param actorUrn the actor's URN
   * @param ingestionSourceUrn the ingestion source's URN
   */
  private void addActorAsOwnerOfIngestionSource(Urn actorUrn, Urn ingestionSourceUrn)
      throws Exception {
    Owner owner =
        new Owner()
            .setType(OwnershipType.TECHNICAL_OWNER)
            .setSource(new OwnershipSource().setType(OwnershipSourceType.OTHER))
            .setOwner(actorUrn);
    ownerService.addOwners(systemOperationContext, ingestionSourceUrn, List.of(owner));
  }
}
