package io.datahubproject.openapi.v2.controller;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.generated.MetadataChangeProposal;
import io.datahubproject.openapi.util.MappingUtil;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/openapi/v2/platform/entities/v1")
@Slf4j
@Tag(
    name = "Platform Entities",
    description = "Platform level APIs intended for lower level access to entities")
public class PlatformEntitiesController {

  private final OperationContext systemOperationContext;
  private final EntityService<ChangeItemImpl> _entityService;
  private final CachingEntitySearchService _cachingEntitySearchService;
  private final ObjectMapper _objectMapper;
  private final AuthorizerChain _authorizerChain;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @PostMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<String>> postEntities(
      HttpServletRequest request,
      @RequestBody @Nonnull List<MetadataChangeProposal> metadataChangeProposals,
      @RequestParam(required = false, name = "async") Boolean async) {
    log.info("INGEST PROPOSAL proposal: {}", metadataChangeProposals);

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "postEntities",
                    metadataChangeProposals.stream()
                        .map(MetadataChangeProposal::getEntityType)
                        .distinct()
                        .collect(Collectors.toList())),
            _authorizerChain,
            authentication,
            true);

    List<com.linkedin.mxe.MetadataChangeProposal> proposals =
        metadataChangeProposals.stream()
            .map(proposal -> MappingUtil.mapToServiceProposal(proposal, _objectMapper))
            .collect(Collectors.toList());

    /*
      Ingest Authorization Checks
    */
    List<Pair<com.linkedin.mxe.MetadataChangeProposal, Integer>> exceptions =
        isAPIAuthorized(opContext, ENTITY, opContext.getEntityRegistry(), proposals).stream()
            .filter(p -> p.getSecond() != com.linkedin.restli.common.HttpStatus.S_200_OK.getCode())
            .collect(Collectors.toList());
    if (!exceptions.isEmpty()) {
      throw new UnauthorizedException(
          actorUrnStr
              + " is unauthorized to edit entities. "
              + exceptions.stream()
                  .map(
                      ex ->
                          String.format(
                              "HttpStatus: %s Urn: %s",
                              ex.getSecond(), ex.getFirst().getEntityUrn()))
                  .collect(Collectors.toList()));
    }

    boolean asyncBool =
        Objects.requireNonNullElseGet(
            async, () -> Boolean.parseBoolean(System.getenv("ASYNC_INGEST_DEFAULT")));
    List<Pair<String, Boolean>> responses =
        proposals.stream()
            .map(
                proposal ->
                    MappingUtil.ingestProposal(
                        opContext, proposal, actorUrnStr, _entityService, asyncBool))
            .collect(Collectors.toList());
    if (responses.stream().anyMatch(Pair::getSecond)) {
      return ResponseEntity.status(HttpStatus.CREATED)
          .body(
              responses.stream()
                  .filter(Pair::getSecond)
                  .map(Pair::getFirst)
                  .collect(Collectors.toList()));
    } else {
      return ResponseEntity.ok(Collections.emptyList());
    }
  }
}
