package io.datahubproject.openapi.openlineage.controller;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.openlineage.exception.InvalidOpenLineageEventException;
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openapi.openlineage.validation.OpenLineageRequestValidator;
import io.datahubproject.openapi.openlineage.validation.OpenLineageValidationError;
import io.openlineage.client.OpenLineage;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/openlineage/api/v1")
@Slf4j
public class LineageApiImpl {
  private final OpenLineageRequestValidator requestValidator;
  private final OpenLineageEventDeserializer eventDeserializer;
  private final RunEventMapper runEventMapper;

  public LineageApiImpl(
      OpenLineageRequestValidator requestValidator,
      OpenLineageEventDeserializer eventDeserializer,
      RunEventMapper runEventMapper) {
    this.requestValidator = requestValidator;
    this.eventDeserializer = eventDeserializer;
    this.runEventMapper = runEventMapper;
  }

  @Autowired private RunEventMapper.MappingConfig _mappingConfig;

  @Autowired private EntityService<?> _entityService;

  @Autowired private AuthorizerChain _authorizerChain;

  @Autowired
  @Qualifier("systemOperationContext")
  OperationContext systemOperationContext;

  @Autowired private HttpServletRequest request;

  @PostMapping(
      value = "/lineage",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Void> postRunEventRaw(@RequestBody byte[] body) {
    JsonNode root = requestValidator.validate(body);
    List<MetadataChangeProposal> mcps;
    try {
      if (root.path("run").isObject() && root.path("job").isObject()) {
        OpenLineage.RunEvent event =
            eventDeserializer.deserialize(root, OpenLineage.RunEvent.class);
        log.debug("Mapping OpenLineage RunEvent from producer {}", event.getProducer());
        mcps = runEventMapper.map(event, _mappingConfig).collect(Collectors.toList());
      } else if (root.path("dataset").isObject()) {
        OpenLineage.DatasetEvent event =
            eventDeserializer.deserialize(root, OpenLineage.DatasetEvent.class);
        log.debug("Mapping OpenLineage DatasetEvent from producer {}", event.getProducer());
        mcps = runEventMapper.map(event, _mappingConfig).collect(Collectors.toList());
      } else {
        OpenLineage.JobEvent event =
            eventDeserializer.deserialize(root, OpenLineage.JobEvent.class);
        log.debug("Mapping OpenLineage JobEvent from producer {}", event.getProducer());
        mcps = runEventMapper.map(event, _mappingConfig).collect(Collectors.toList());
      }
    } catch (JsonProcessingException exception) {
      throw new InvalidOpenLineageEventException(
          List.of(new OpenLineageValidationError("$", "deserialization", null, null)));
    }
    return ingestMcps(mcps, AuthenticationContext.getAuthentication());
  }

  @PostMapping(value = "/lineage", produces = MediaType.APPLICATION_JSON_VALUE)
  public void rejectUnsupportedMediaType() throws HttpMediaTypeNotSupportedException {
    throw new HttpMediaTypeNotSupportedException(
        "OpenLineage requests must use application/json", List.of(MediaType.APPLICATION_JSON));
  }

  private ResponseEntity<Void> ingestMcps(
      List<MetadataChangeProposal> mcps, Authentication authentication) {
    if (mcps.isEmpty()) {
      throw new IllegalStateException(
          "OpenLineage event mapping did not produce any metadata proposals");
    }

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "postRunEventRaw", List.of())
                .withUsageOperation(UsageOperation.METADATA_INGEST),
            _authorizerChain,
            authentication,
            true);

    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
            .setTime(System.currentTimeMillis());
    AspectsBatch aspectsBatch =
        AspectsBatchImpl.builder()
            .mcps(
                mcps,
                auditStamp,
                opContext.getRetrieverContext(),
                opContext.getValidationContext().isAlternateValidation())
            .build(opContext);
    boolean authorized =
        EntityAuthorizationUtils.isAPIAuthorizedBatchItems(opContext, aspectsBatch.getItems())
            .stream()
            .allMatch(result -> result.getSecond() == org.apache.http.HttpStatus.SC_OK);
    if (!authorized) {
      throw new UnauthorizedException("Not authorized to ingest OpenLineage metadata");
    }
    log.info("Submitting OpenLineage batch with {} proposals", aspectsBatch.getItems().size());
    _entityService.ingestProposal(opContext, aspectsBatch, true);
    return new ResponseEntity<>(HttpStatus.ACCEPTED);
  }
}
