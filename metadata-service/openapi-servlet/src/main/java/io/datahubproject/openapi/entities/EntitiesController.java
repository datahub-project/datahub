package io.datahubproject.openapi.entities;

import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.dto.RollbackRunResultDto;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.generated.AspectRowSummary;
import io.datahubproject.openapi.util.MappingUtil;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
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

@RestController
@RequiredArgsConstructor
@RequestMapping("/entities/v1")
@Slf4j
@Tag(
    name = "Entities",
    description = "APIs for ingesting and accessing entities and their constituent aspects")
public class EntitiesController {

  private final EntityService _entityService;
  private final ObjectMapper _objectMapper;
  private final AuthorizerChain _authorizerChain;

  @Value("${authorization.restApiAuthorization:false}")
  private boolean restApiAuthorizationEnabled;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @GetMapping(value = "/latest", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<UrnResponseMap> getEntities(
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
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.GET_ENTITY_PRIVILEGE.getType()))));

    List<Optional<EntitySpec>> resourceSpecs =
        entityUrns.stream()
            .map(urn -> Optional.of(new EntitySpec(urn.getEntityType(), urn.toString())))
            .collect(Collectors.toList());
    if (restApiAuthorizationEnabled
        && !AuthUtil.isAuthorizedForResources(
            _authorizerChain, actorUrnStr, resourceSpecs, orGroup)) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to get entities.");
    }
    if (entityUrns.size() <= 0) {
      return ResponseEntity.ok(UrnResponseMap.builder().responses(Collections.emptyMap()).build());
    }
    // TODO: Only supports one entity type at a time, may cause confusion
    final String entityName = urnToEntityName(entityUrns.iterator().next());
    final Set<String> projectedAspects =
        aspectNames == null
            ? _entityService.getEntityAspectNames(entityName)
            : new HashSet<>(Arrays.asList(aspectNames));
    Throwable exceptionally = null;
    try {
      return ResponseEntity.ok(
          UrnResponseMap.builder()
              .responses(
                  MappingUtil.mapServiceResponse(
                      _entityService.getEntitiesV2(entityName, entityUrns, projectedAspects),
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
      @RequestBody @Nonnull List<UpsertAspectRequest> aspectRequests) {
    log.info("INGEST PROPOSAL proposal: {}", aspectRequests);

    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType()))));
    List<com.linkedin.mxe.MetadataChangeProposal> proposals =
        aspectRequests.stream()
            .map(MappingUtil::mapToProposal)
            .map(proposal -> MappingUtil.mapToServiceProposal(proposal, _objectMapper))
            .collect(Collectors.toList());

    if (restApiAuthorizationEnabled
        && !MappingUtil.authorizeProposals(
            proposals, _entityService, _authorizerChain, actorUrnStr, orGroup)) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to edit entities.");
    }

    List<Pair<String, Boolean>> responses =
        proposals.stream()
            .map(proposal -> MappingUtil.ingestProposal(proposal, actorUrnStr, _entityService))
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

  @DeleteMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<RollbackRunResultDto>> deleteEntities(
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
          boolean soft) {
    Throwable exceptionally = null;
    try (Timer.Context context = MetricUtils.timer("deleteEntities").time()) {
      Authentication authentication = AuthenticationContext.getAuthentication();
      String actorUrnStr = authentication.getActor().toUrnStr();
      DisjunctivePrivilegeGroup orGroup =
          new DisjunctivePrivilegeGroup(
              ImmutableList.of(
                  new ConjunctivePrivilegeGroup(
                      ImmutableList.of(PoliciesConfig.DELETE_ENTITY_PRIVILEGE.getType()))));
      final Set<Urn> entityUrns =
          Arrays.stream(urns)
              // Have to decode here because of frontend routing, does No-op for already unencoded
              // through direct API access
              .map(URLDecoder::decode)
              .map(UrnUtils::getUrn)
              .collect(Collectors.toSet());

      List<Optional<EntitySpec>> resourceSpecs =
          entityUrns.stream()
              .map(urn -> Optional.of(new EntitySpec(urn.getEntityType(), urn.toString())))
              .collect(Collectors.toList());
      if (restApiAuthorizationEnabled
          && !AuthUtil.isAuthorizedForResources(
              _authorizerChain, actorUrnStr, resourceSpecs, orGroup)) {
        UnauthorizedException unauthorizedException =
            new UnauthorizedException(actorUrnStr + " is unauthorized to delete entities.");
        exceptionally = unauthorizedException;
        throw unauthorizedException;
      }

      if (!soft) {
        return ResponseEntity.ok(
            entityUrns.stream()
                .map(_entityService::deleteUrn)
                .map(
                    rollbackRunResult ->
                        MappingUtil.mapRollbackRunResult(rollbackRunResult, _objectMapper))
                .collect(Collectors.toList()));
      } else {
        List<UpsertAspectRequest> deleteRequests =
            entityUrns.stream()
                .map(entityUrn -> MappingUtil.createStatusRemoval(entityUrn, _entityService))
                .collect(Collectors.toList());

        return ResponseEntity.ok(
            Collections.singletonList(
                RollbackRunResultDto.builder()
                    .rowsRolledBack(
                        deleteRequests.stream()
                            .map(MappingUtil::mapToProposal)
                            .map(
                                proposal ->
                                    MappingUtil.mapToServiceProposal(proposal, _objectMapper))
                            .map(
                                proposal ->
                                    MappingUtil.ingestProposal(
                                        proposal, actorUrnStr, _entityService))
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
