package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.AcrylConstants.PERSONA_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.PERSONA_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_VIEW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.persona.DataHubPersonaInfo;
import com.linkedin.view.DataHubViewInfo;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.net.URISyntaxException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;

@Slf4j
@RequiredArgsConstructor
public class IngestDefaultPersonasAndViews implements BootstrapStep {
  private static final int SLEEP_SECONDS = 30;

  private final EntityService<?> _entityService;
  private final EntityRegistry _entityRegistry;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public void execute(@Nonnull OperationContext systemOperationContext) throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    mapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());

    // Sleep to ensure deployment process finishes.
    Thread.sleep(SLEEP_SECONDS * 1000);

    // Ingest default persona views
    ingestPersonaViews(mapper, systemOperationContext);

    // Ingest default personas
    ingestPersonas(mapper, systemOperationContext);
  }

  private void ingestPersonaViews(
      @Nonnull final ObjectMapper mapper, @Nonnull final OperationContext systemOperationContext)
      throws Exception {
    // 0. Execute preflight check to see whether we need to ingest personas
    log.info("Ingesting default Persona Views...");

    // 1. Read from the file into JSON.
    final JsonNode viewsObj =
        mapper.readTree(new ClassPathResource("./boot/persona_views.json").getInputStream());

    if (!viewsObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed persona views file, expected an Array but found %s",
              viewsObj.getNodeType()));
    }

    final AspectSpec viewsInfoAspectSpec =
        _entityRegistry
            .getEntitySpec(DATAHUB_VIEW_ENTITY_NAME)
            .getAspectSpec(DATAHUB_VIEW_INFO_ASPECT_NAME);

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    for (final JsonNode viewObj : viewsObj) {
      final Urn urn = Urn.createFromString(viewObj.get("urn").asText());

      // If the info is not there, it means that the persona was there before, but must now be
      // removed
      if (!viewObj.has("info")) {
        _entityService.deleteUrn(systemOperationContext, urn);
        continue;
      }

      // If the view has been edited, simply leave it.
      if (!_entityService.exists(systemOperationContext, urn)) {
        log.info(String.format("Ingesting default persona view with urn %s", urn));
        final DataHubViewInfo info =
            RecordUtils.toRecordTemplate(DataHubViewInfo.class, viewObj.get("info").toString());
        info.setCreated(auditStamp);
        info.setLastModified(auditStamp);
        ingestPersonaView(systemOperationContext, urn, info, auditStamp, viewsInfoAspectSpec);
      } else {
        log.info(String.format("Skipping ingestion of view with urn %s", urn));
      }
    }

    log.info("Successfully ingested default persona views.");
  }

  private void ingestPersonaView(
      @Nonnull OperationContext systemOperationContext,
      final Urn viewUrn,
      final DataHubViewInfo dataHubViewInfo,
      final AuditStamp auditStamp,
      final AspectSpec dataHubViewInfoAspectSpec)
      throws URISyntaxException {

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(viewUrn);
    proposal.setEntityType(DATAHUB_VIEW_ENTITY_NAME);
    proposal.setAspectName(DATAHUB_VIEW_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dataHubViewInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        systemOperationContext,
        AspectsBatchImpl.builder()
            .mcps(
                List.of(proposal),
                new AuditStamp()
                    .setActor(Urn.createFromString(SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()),
                systemOperationContext.getRetrieverContext())
            .build(systemOperationContext),
        false);

    _entityService.alwaysProduceMCLAsync(
        systemOperationContext,
        viewUrn,
        PERSONA_ENTITY_NAME,
        PERSONA_INFO_ASPECT_NAME,
        dataHubViewInfoAspectSpec,
        null,
        dataHubViewInfo,
        null,
        null,
        auditStamp,
        ChangeType.RESTATE);
  }

  private void ingestPersonas(
      @Nonnull final ObjectMapper mapper, @Nonnull final OperationContext systemOperationContext)
      throws Exception {
    // 0. Execute preflight check to see whether we need to ingest personas
    log.info("Ingesting default Personas...");

    // 1. Read from the file into JSON.
    final JsonNode personasObj =
        mapper.readTree(new ClassPathResource("./boot/personas.json").getInputStream());

    if (!personasObj.isArray()) {
      throw new RuntimeException(
          String.format(
              "Found malformed personas file, expected an Array but found %s",
              personasObj.getNodeType()));
    }

    final AspectSpec personaInfoAspectSpec =
        _entityRegistry.getEntitySpec(PERSONA_ENTITY_NAME).getAspectSpec(PERSONA_INFO_ASPECT_NAME);

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    for (final JsonNode personaObj : personasObj) {
      final Urn urn = Urn.createFromString(personaObj.get("urn").asText());

      // If the info is not there, it means that the persona was there before, but must now be
      // removed
      if (!personaObj.has("info")) {
        _entityService.deleteUrn(systemOperationContext, urn);
        continue;
      }

      final DataHubPersonaInfo info =
          RecordUtils.toRecordTemplate(DataHubPersonaInfo.class, personaObj.get("info").toString());
      ingestPersona(systemOperationContext, urn, info, auditStamp, personaInfoAspectSpec);
    }

    log.info("Successfully ingested default personas.");
  }

  private void ingestPersona(
      @Nonnull OperationContext systemOperationContext,
      final Urn personaUrn,
      final DataHubPersonaInfo dataHubPersonaInfo,
      final AuditStamp auditStamp,
      final AspectSpec personaInfoAspectSpec)
      throws URISyntaxException {

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(personaUrn);
    proposal.setEntityType(PERSONA_ENTITY_NAME);
    proposal.setAspectName(PERSONA_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(dataHubPersonaInfo));
    proposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(
        systemOperationContext,
        AspectsBatchImpl.builder()
            .mcps(
                List.of(proposal),
                new AuditStamp()
                    .setActor(Urn.createFromString(SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()),
                systemOperationContext.getRetrieverContext())
            .build(systemOperationContext),
        false);

    _entityService.alwaysProduceMCLAsync(
        systemOperationContext,
        personaUrn,
        PERSONA_ENTITY_NAME,
        PERSONA_INFO_ASPECT_NAME,
        personaInfoAspectSpec,
        null,
        dataHubPersonaInfo,
        null,
        null,
        auditStamp,
        ChangeType.RESTATE);
  }
}
