package io.datahubproject.openapi.openlineage.controller;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.exception.UnprocessableEntityException;
import io.datahubproject.openapi.openlineage.config.DatahubOpenlineageProperties;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.generated.controller.LineageApi;
import io.datahubproject.openlineage.model.LineageBatchResult;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/openlineage/api/v1")
@Slf4j
public class LineageApiImpl implements LineageApi {
  private static final ObjectMapper OBJECT_MAPPER = OpenLineageClientUtils.newObjectMapper();

  @Autowired private RunEventMapper.MappingConfig _mappingConfig;

  @Autowired private EntityServiceImpl _entityService;

  @Autowired private AuthorizerChain _authorizerChain;

  @Autowired private DatahubOpenlineageProperties _properties;

  @Autowired
  @Qualifier("systemOperationContext")
  OperationContext systemOperationContext;

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.of(OBJECT_MAPPER);
  }

  @Autowired private HttpServletRequest request;

  /**
   * OpenLineage 2.0 defines three top-level events. {@code RunEvent} reports an execution; {@code
   * JobEvent} and {@code DatasetEvent} carry metadata with no run attached, which is how producers
   * describe a job or a table they did not just execute or write.
   */
  private enum EventKind {
    RUN,
    JOB,
    DATASET
  }

  @Override
  public ResponseEntity<Void> postRunEventRaw(String body) {
    // Event payloads carry table names, column names and SQL text, so they stay at DEBUG rather
    // than being written to shipped logs on every request.
    log.debug("Received lineage event: {}", body);
    return ingest(conversionFor(body, parseObject(body)));
  }

  /**
   * The batch counterpart of {@link #postRunEventRaw(String)}. Conversion runs per event so that
   * one unconvertible event does not reject the rest of the array — which is the whole point of a
   * batch for a streaming producer — while the resulting proposals are still ingested as a single
   * write.
   *
   * <p>Authorization is not per event: it is a property of the actor, so a batch touching anything
   * the caller may not write is refused whole, exactly as a single event would be.
   */
  @Override
  public ResponseEntity<LineageBatchResult> postEventBatchRaw(String body) {
    log.debug("Received lineage batch: {}", body);

    JsonNode parsed = parse(body, "batch");
    if (!parsed.isArray()) {
      throw new IllegalArgumentException("Malformed OpenLineage batch: expected a JSON array");
    }
    int received = parsed.size();
    if (received > _properties.getMaxBatchSize()) {
      throw new IllegalArgumentException(
          "OpenLineage batch of "
              + received
              + " events exceeds the configured maximum of "
              + _properties.getMaxBatchSize());
    }

    List<MetadataChangeProposal> proposals = new ArrayList<>();
    List<LineageBatchResult.FailedEvent> failures = new ArrayList<>();
    for (int i = 0; i < received; i++) {
      JsonNode event = parsed.get(i);
      try {
        if (!event.isObject()) {
          throw new IllegalArgumentException("Malformed OpenLineage event: expected a JSON object");
        }
        proposals.addAll(convert(conversionFor(event.toString(), event)));
      } catch (IllegalArgumentException | UnprocessableEntityException e) {
        // Both mean the event itself is the problem, and both are deterministic: the same bytes
        // fail the same way, so a resend cannot help.
        log.warn("Skipping event {} of OpenLineage batch: {}", i, e.getMessage());
        failures.add(new LineageBatchResult.FailedEvent(i, e.getMessage(), false));
      }
    }

    if (!proposals.isEmpty()) {
      // A failure here is a server fault affecting the whole write, not an event a producer could
      // fix, so it propagates as a 500 rather than being reported per event.
      ingestProposals(newOperationContext("postEventBatchRaw"), proposals);
    }
    return ResponseEntity.ok(LineageBatchResult.of(received, failures));
  }

  private static JsonNode parse(String body, String what) {
    JsonNode parsed;
    try {
      parsed = OBJECT_MAPPER.readTree(body);
    } catch (Exception e) {
      log.warn("Rejecting malformed OpenLineage {} payload: {}", what, e.getMessage());
      throw new IllegalArgumentException("Malformed OpenLineage " + what + ": " + e.getMessage());
    }
    // An empty or whitespace-only body parses to null rather than throwing.
    if (parsed == null || parsed.isNull()) {
      log.warn("Rejecting empty OpenLineage {} payload", what);
      throw new IllegalArgumentException("Malformed OpenLineage " + what + ": body is empty");
    }
    return parsed;
  }

  private static JsonNode parseObject(String body) {
    JsonNode parsed = parse(body, "event");
    if (!parsed.isObject()) {
      log.warn("Rejecting OpenLineage payload that is not a JSON object");
      throw new IllegalArgumentException("Malformed OpenLineage event: expected a JSON object");
    }
    return parsed;
  }

  /**
   * Resolves one event to the mapping that converts it, deferring the conversion itself so the
   * caller decides how a failure is reported — a status code on the single path, an entry in the
   * response body on the batch path.
   *
   * <p>Deserialization is the only step here whose failure means "the caller sent us nonsense". It
   * is kept out of {@link #convert} on purpose: wrapping the ingest call too would report a GMS
   * write failure, including a transaction conflict, as a 400.
   */
  private Function<RunEventMapper, Stream<MetadataChangeProposal>> conversionFor(
      String body, JsonNode parsed) {
    EventKind kind = classify(parsed);
    switch (kind) {
      case DATASET:
        OpenLineage.DatasetEvent datasetEvent =
            deserialize(body, kind, new TypeReference<OpenLineage.DatasetEvent>() {});
        return mapper -> mapper.map(datasetEvent, this._mappingConfig);
      case JOB:
        OpenLineage.JobEvent jobEvent =
            deserialize(body, kind, new TypeReference<OpenLineage.JobEvent>() {});
        return mapper -> mapper.map(jobEvent, this._mappingConfig);
      case RUN:
      default:
        OpenLineage.RunEvent runEvent;
        try {
          runEvent = OpenLineageClientUtils.runEventFromJson(body);
        } catch (Exception e) {
          log.warn("Rejecting malformed OpenLineage {} payload: {}", kind, e.getMessage());
          throw new IllegalArgumentException("Malformed OpenLineage event: " + e.getMessage());
        }
        return mapper -> mapper.map(runEvent, this._mappingConfig);
    }
  }

  private static <T> T deserialize(String body, EventKind kind, TypeReference<T> type) {
    try {
      return OpenLineageClientUtils.fromJson(body, type);
    } catch (Exception e) {
      log.warn("Rejecting malformed OpenLineage {} payload: {}", kind, e.getMessage());
      throw new IllegalArgumentException("Malformed OpenLineage event: " + e.getMessage());
    }
  }

  /**
   * The {@code schemaURL} is the spec's own discriminator, so it decides when present. Producers do
   * omit it, though, so fall back to the event's shape: only a RunEvent has a {@code run}, and only
   * a DatasetEvent has a top-level {@code dataset}.
   */
  private static EventKind classify(JsonNode event) {
    JsonNode schemaUrl = event.get("schemaURL");
    if (schemaUrl != null && schemaUrl.isTextual()) {
      String url = schemaUrl.asText();
      if (url.endsWith("DatasetEvent")) {
        return EventKind.DATASET;
      }
      if (url.endsWith("JobEvent")) {
        return EventKind.JOB;
      }
      if (url.endsWith("RunEvent")) {
        return EventKind.RUN;
      }
    }
    // isObject rather than has: a JSON null is still "present", and clients that serialize
    // optional fields emit "run": null on a JobEvent. Treating that as a RunEvent reproduces
    // exactly the null-run failure this dispatch exists to prevent.
    if (event.path("run").isObject()) {
      return EventKind.RUN;
    }
    if (event.path("dataset").isObject()) {
      return EventKind.DATASET;
    }
    if (event.path("job").isObject()) {
      return EventKind.JOB;
    }
    // Unrecognised shape: let the RunEvent path produce the error, as it did before dispatch.
    return EventKind.RUN;
  }

  public ResponseEntity<Void> postRunEventRaw(OpenLineage.RunEvent openlineageRunEvent) {
    return ingest(mapper -> mapper.map(openlineageRunEvent, this._mappingConfig));
  }

  /** Single-event tail: convert, authorize, then ingest as one batch. */
  private ResponseEntity<Void> ingest(
      Function<RunEventMapper, Stream<MetadataChangeProposal>> conversion) {
    ingestProposals(newOperationContext("postRunEventRaw"), convert(conversion));
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  private OperationContext newOperationContext(String operationName) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    return OperationContext.asSession(
        systemOperationContext,
        RequestContext.builder()
            .buildOpenapi(authentication.getActor().toUrnStr(), request, operationName, List.of())
            .withUsageOperation(UsageOperation.METADATA_INGEST),
        _authorizerChain,
        authentication,
        true);
  }

  /**
   * A conversion failure means the caller sent something we understood but cannot store, which is a
   * 422 rather than a server fault.
   */
  private static List<MetadataChangeProposal> convert(
      Function<RunEventMapper, Stream<MetadataChangeProposal>> conversion) {
    try {
      return conversion.apply(new RunEventMapper()).collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("OpenLineage event could not be converted: {}", e.getMessage());
      throw new UnprocessableEntityException(
          "OpenLineage event could not be converted: " + e.getMessage());
    }
  }

  private void ingestProposals(OperationContext opContext, List<MetadataChangeProposal> proposals) {
    authorize(opContext, proposals);

    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(opContext.getActorContext().getActorUrn())
            .setTime(System.currentTimeMillis());

    // One batch rather than a proposal-at-a-time loop: a partial failure midway through the loop
    // left some aspects committed with nothing recording which.
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(proposals, auditStamp, opContext.getRetrieverContext())
            .build(opContext);
    _entityService.ingestProposal(opContext, batch, true);
  }

  /**
   * Authorization has to happen here rather than being left to the entity service: this endpoint
   * writes through EntityServiceImpl, which is the storage layer and performs no privilege check.
   */
  private static void authorize(
      OperationContext opContext, List<MetadataChangeProposal> proposals) {
    List<Pair<MetadataChangeProposal, Integer>> denied =
        EntityAuthorizationUtils.isAPIAuthorizedIngest(
                opContext, opContext.getEntityRegistry(), proposals)
            .stream()
            .filter(p -> p.getSecond() != com.linkedin.restli.common.HttpStatus.S_200_OK.getCode())
            .collect(Collectors.toList());
    if (!denied.isEmpty()) {
      throw new UnauthorizedException(
          opContext.getActorContext().getActorUrn()
              + " is unauthorized to ingest OpenLineage lineage for: "
              + denied.stream()
                  .map(
                      p ->
                          p.getFirst().getEntityUrn() == null
                              ? p.getFirst().getEntityType()
                              : p.getFirst().getEntityUrn().toString())
                  .distinct()
                  .collect(Collectors.joining(", ")));
    }
  }
}
