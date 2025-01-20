package io.datahubproject.openapi.v1.entities;

import static com.datahub.authorization.AuthUtil.isAPIAuthorized;
import static com.linkedin.metadata.authorization.ApiGroup.ENTITY;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.dto.RollbackRunResultDto;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.generated.AspectRowSummary;
import io.datahubproject.openapi.util.MappingUtil;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/*
 Use v2 or v3 controllers instead
*/
@Deprecated
@RestController
@RequestMapping("/openapi/entities/v1")
@Slf4j
@Tag(
    name = "Entities",
    description = "APIs for ingesting and accessing entities and their constituent aspects")
public class EntitiesController {

  private final OperationContext systemOperationContext;
  private final EntityService<ChangeItemImpl> _entityService;
  private final ObjectMapper _objectMapper;
  private final AuthorizerChain _authorizerChain;

  public EntitiesController(
      OperationContext systemOperationContext,
      EntityService<ChangeItemImpl> _entityService,
      ObjectMapper _objectMapper,
      AuthorizerChain _authorizerChain) {
    this.systemOperationContext = systemOperationContext;
    this._entityService = _entityService;
    this._objectMapper = _objectMapper;
    this._authorizerChain = _authorizerChain;
  }

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @GetMapping(value = "/latest", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<UrnResponseMap> getEntities(
      HttpServletRequest request,
      @Parameter(
              name = "urns",
              required = true,
              description =
                  "A list of raw urn strings, only supports a single entity type per request.")
          @RequestParam("urns")
          @Nonnull
          String[] urns,
      @Parameter(name = "aspectNames", description = "The list of aspect names to retrieve")
          @RequestParam(name = "aspectNames", required = false)
          @Nullable
          String[] aspectNames) {
    Timer.Context context = MetricUtils.timer("getEntities").time();
    final Set<Urn> entityUrns =
        Arrays.stream(urns)
            // Have to decode here because of frontend routing, does No-op for already unencoded
            // through direct API access
            .map(URLDecoder::decode)
            .map(UrnUtils::getUrn)
            .collect(Collectors.toSet());
    log.debug("GET ENTITIES {}", entityUrns);
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "getEntities",
                    entityUrns.stream()
                        .map(Urn::getEntityType)
                        .distinct()
                        .collect(Collectors.toList())),
            _authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, entityUrns)) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to get entities.");
    }

    if (entityUrns.size() <= 0) {
      return ResponseEntity.ok(UrnResponseMap.builder().responses(Collections.emptyMap()).build());
    }
    // TODO: Only supports one entity type at a time, may cause confusion
    final String entityName = urnToEntityName(entityUrns.iterator().next());
    final Set<String> projectedAspects =
        aspectNames == null
            ? opContext.getEntityAspectNames(entityName)
            : new HashSet<>(Arrays.asList(aspectNames));
    Throwable exceptionally = null;
    try {
      return ResponseEntity.ok(
          UrnResponseMap.builder()
              .responses(
                  MappingUtil.mapServiceResponse(
                      _entityService.getEntitiesV2(
                          opContext, entityName, entityUrns, projectedAspects),
                      _objectMapper))
              .build());
    } catch (Exception e) {
      exceptionally = e;
      throw new RuntimeException(
          String.format(
              "Failed to batch get entities with urns: %s, projectedAspects: %s",
              entityUrns, projectedAspects),
          e);
    } finally {
      if (exceptionally != null) {
        MetricUtils.counter(MetricRegistry.name("getEntities", "failed")).inc();
      } else {
        MetricUtils.counter(MetricRegistry.name("getEntities", "success")).inc();
      }
      context.stop();
    }
  }

  @PostMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<String>> postEntities(
      HttpServletRequest request,
      @RequestBody @Nonnull List<UpsertAspectRequest> aspectRequests,
      @RequestParam(required = false, name = "async") Boolean async,
      @RequestParam(required = false, name = "createIfNotExists") Boolean createIfNotExists,
      @RequestParam(required = false, name = "createEntityIfNotExists")
          Boolean createEntityIfNotExists) {

    log.info("INGEST PROPOSAL proposal: {}", aspectRequests);

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    List<com.linkedin.mxe.MetadataChangeProposal> proposals =
        aspectRequests.stream()
            .map(req -> MappingUtil.mapToProposal(req, createIfNotExists, createEntityIfNotExists))
            .map(proposal -> MappingUtil.mapToServiceProposal(proposal, _objectMapper))
            .collect(Collectors.toList());

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(
                    actorUrnStr,
                    request,
                    "postEntities",
                    proposals.stream()
                        .map(MetadataChangeProposal::getEntityType)
                        .collect(Collectors.toSet())),
            _authorizerChain,
            authentication,
            true);
    /*
     Ingest Authorization Checks
    */
    List<Pair<MetadataChangeProposal, Integer>> exceptions =
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
        MappingUtil.ingestBatchProposal(
            opContext, proposals, actorUrnStr, _entityService, asyncBool);

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

  @DeleteMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<RollbackRunResultDto>> deleteEntities(
      HttpServletRequest request,
      @Parameter(
              name = "urns",
              required = true,
              description =
                  "A list of raw urn strings, only supports a single entity type per request.")
          @RequestParam("urns")
          @Nonnull
          String[] urns,
      @Parameter(
              name = "soft",
              description =
                  "Determines whether the delete will be soft or hard, defaults to true for soft delete")
          @RequestParam(value = "soft", defaultValue = "true")
          boolean soft,
      @RequestParam(required = false, name = "async") Boolean async) {
    Throwable exceptionally = null;
    try (Timer.Context context = MetricUtils.timer("deleteEntities").time()) {
      Authentication authentication = AuthenticationContext.getAuthentication();
      String actorUrnStr = authentication.getActor().toUrnStr();

      final Set<Urn> entityUrns =
          Arrays.stream(urns)
              // Have to decode here because of frontend routing, does No-op for already unencoded
              // through direct API access
              .map(URLDecoder::decode)
              .map(UrnUtils::getUrn)
              .collect(Collectors.toSet());

      OperationContext opContext =
          OperationContext.asSession(
              systemOperationContext,
              RequestContext.builder()
                  .buildOpenapi(
                      actorUrnStr,
                      request,
                      "deleteEntities",
                      entityUrns.stream().map(Urn::getEntityType).collect(Collectors.toSet())),
              _authorizerChain,
              authentication,
              true);

      if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, DELETE, entityUrns)) {
        throw new UnauthorizedException(actorUrnStr + " is unauthorized to delete entities.");
      }

      if (!soft) {
        return ResponseEntity.ok(
            entityUrns.stream()
                .map(urn -> _entityService.deleteUrn(opContext, urn))
                .map(
                    rollbackRunResult ->
                        MappingUtil.mapRollbackRunResult(rollbackRunResult, _objectMapper))
                .collect(Collectors.toList()));
      } else {
        List<UpsertAspectRequest> deleteRequests =
            entityUrns.stream()
                .map(entityUrn -> MappingUtil.createStatusRemoval(opContext, entityUrn))
                .collect(Collectors.toList());

        boolean asyncBool =
            Objects.requireNonNullElseGet(
                async, () -> Boolean.parseBoolean(System.getenv("ASYNC_INGEST_DEFAULT")));
        return ResponseEntity.ok(
            Collections.singletonList(
                RollbackRunResultDto.builder()
                    .rowsRolledBack(
                        deleteRequests.stream()
                            .map(req -> MappingUtil.mapToProposal(req, null, null))
                            .map(
                                proposal ->
                                    MappingUtil.mapToServiceProposal(proposal, _objectMapper))
                            .map(
                                proposal ->
                                    MappingUtil.ingestProposal(
                                        opContext,
                                        proposal,
                                        actorUrnStr,
                                        _entityService,
                                        asyncBool))
                            .filter(Pair::getSecond)
                            .map(Pair::getFirst)
                            .map(urnString -> AspectRowSummary.builder().urn(urnString).build())
                            .collect(Collectors.toList()))
                    .rowsDeletedFromEntityDeletion(deleteRequests.size())
                    .build()));
      }
    } catch (Exception e) {
      exceptionally = e;
      throw new RuntimeException(
          String.format("Failed to batch delete entities with urns: %s", Arrays.asList(urns)), e);
    } finally {
      if (exceptionally != null) {
        MetricUtils.counter(MetricRegistry.name("getEntities", "failed")).inc();
      } else {
        MetricUtils.counter(MetricRegistry.name("getEntities", "success")).inc();
      }
    }
  }
}
