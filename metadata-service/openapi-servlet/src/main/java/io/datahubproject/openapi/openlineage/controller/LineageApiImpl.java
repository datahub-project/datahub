package io.datahubproject.openapi.openlineage.controller;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
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

  // @Autowired
  // @Qualifier("javaEntityClient")
  // private EntityClient _entityClient;

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
    try {
      log.info("Received lineage event: {}", body);
      OpenLineage.RunEvent openlineageRunEvent = OpenLineageClientUtils.runEventFromJson(body);
      log.info("Deserialized to lineage event: {}", openlineageRunEvent);
      return postRunEventRaw(openlineageRunEvent);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public ResponseEntity<Void> postRunEventRaw(OpenLineage.RunEvent openlineageRunEvent) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    authentication.getActor().toUrnStr(), request, "postRunEventRaw", List.of()),
            _authorizerChain,
            authentication,
            true);

    log.info("PostRun received lineage event: {}", openlineageRunEvent);

    RunEventMapper runEventMapper = new RunEventMapper();
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(authentication.getActor().toUrnStr()))
            .setTime(System.currentTimeMillis());
    try {
      for (MetadataChangeProposal mcp :
          runEventMapper
              .map(openlineageRunEvent, this._mappingConfig)
              .collect(Collectors.toList())) {
        log.info("Ingesting MCP: {}", mcp);
        _entityService.ingestProposal(opContext, mcp, auditStamp, true);
      }
      return new ResponseEntity<>(HttpStatus.OK);
    } catch (Exception e) {
      // log.error(e.getMessage(), e);
      throw new RuntimeException(e);
      // return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
