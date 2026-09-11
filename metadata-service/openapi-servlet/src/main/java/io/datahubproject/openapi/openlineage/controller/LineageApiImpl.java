package io.datahubproject.openapi.openlineage.controller;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
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
import io.datahubproject.openapi.openlineage.mapping.RunEventMapper;
import io.datahubproject.openlineage.generated.controller.LineageApi;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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

  @Autowired
  @Qualifier("systemOperationContext")
  OperationContext systemOperationContext;

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.of(OBJECT_MAPPER);
  }

  @Autowired private HttpServletRequest request;

  @Override
  public ResponseEntity<Void> postRunEventRaw(String body) {
    // Event payloads carry table names, column names and SQL text, so they stay at DEBUG rather
    // than being written to shipped logs on every request.
    log.debug("Received lineage event: {}", body);
    OpenLineage.RunEvent openlineageRunEvent;
    try {
      openlineageRunEvent = OpenLineageClientUtils.runEventFromJson(body);
    } catch (Exception e) {
      log.warn("Rejecting malformed OpenLineage payload: {}", e.getMessage());
      throw new IllegalArgumentException("Malformed OpenLineage event: " + e.getMessage());
    }
    log.debug("Deserialized to lineage event: {}", openlineageRunEvent);
    return postRunEventRaw(openlineageRunEvent);
  }

  public ResponseEntity<Void> postRunEventRaw(OpenLineage.RunEvent openlineageRunEvent) {
    Authentication authentication = AuthenticationContext.getAuthentication();
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

    // A conversion failure means the caller sent something we understood but cannot store, which
    // is a 422 rather than a server fault. Only the mapping is wrapped, so a genuine GMS failure
    // during ingest still surfaces as a 500.
    List<MetadataChangeProposal> proposals;
    try {
      proposals =
          new RunEventMapper()
              .map(openlineageRunEvent, this._mappingConfig)
              .collect(Collectors.toList());
    } catch (Exception e) {
      log.warn("OpenLineage event could not be converted: {}", e.getMessage());
      throw new UnprocessableEntityException(
          "OpenLineage event could not be converted: " + e.getMessage());
    }

    // Authorization has to happen here rather than being left to the entity service: this endpoint
    // writes through EntityServiceImpl, which is the storage layer and performs no privilege check.
    List<Pair<MetadataChangeProposal, Integer>> denied =
        EntityAuthorizationUtils.isAPIAuthorizedIngest(
                opContext, opContext.getEntityRegistry(), proposals)
            .stream()
            .filter(p -> p.getSecond() != com.linkedin.restli.common.HttpStatus.S_200_OK.getCode())
            .collect(Collectors.toList());
    if (!denied.isEmpty()) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
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

    // One batch rather than a proposal-at-a-time loop: a partial failure midway through the loop
    // left some aspects committed with nothing recording which.
    AspectsBatch batch =
        AspectsBatchImpl.builder()
            .mcps(proposals, auditStamp, opContext.getRetrieverContext())
            .build(opContext);
    _entityService.ingestProposal(opContext, batch, true);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }
}
