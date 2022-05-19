package io.datahubproject.openapi.entities;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.util.Pair;
import io.datahubproject.openapi.dto.RollbackRunResultDto;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.dto.UrnResponseMap;
import io.datahubproject.openapi.generated.AspectRowSummary;
import io.datahubproject.openapi.util.MappingUtil;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
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

import static com.linkedin.metadata.utils.PegasusUtils.*;


@RestController
@AllArgsConstructor
@RequestMapping("/entities/v1")
@Slf4j
@Tag(name = "Entities", description = "APIs for ingesting and accessing entities and their constituent aspects")
public class EntitiesController {

  private final EntityService _entityService;
  private final ObjectMapper _objectMapper;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  @GetMapping(value = "/latest", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<UrnResponseMap> getEntities(
      @Parameter(name = "urns", required = true, description = "A list of raw urn strings, only supports a single entity type per request.")
      @RequestParam("urns") @Nonnull String[] urns,
      @Parameter(name = "aspectNames", description = "The list of aspect names to retrieve")
      @RequestParam(name = "aspectNames", required = false) @Nullable String[] aspectNames) {
    Timer.Context context = MetricUtils.timer("getEntities").time();
    final Set<Urn> entityUrns =
        Arrays.stream(urns)
            // Have to decode here because of frontend routing, does No-op for already unencoded through direct API access
            .map(URLDecoder::decode)
            .map(UrnUtils::getUrn).collect(Collectors.toSet());
    log.debug("GET ENTITIES {}", entityUrns);
    if (entityUrns.size() <= 0) {
      return ResponseEntity.ok(UrnResponseMap.builder().responses(Collections.emptyMap()).build());
    }
    // TODO: Only supports one entity type at a time, may cause confusion
    final String entityName = urnToEntityName(entityUrns.iterator().next());
    final Set<String> projectedAspects = aspectNames == null ? _entityService.getEntityAspectNames(entityName)
        : new HashSet<>(Arrays.asList(aspectNames));
    Throwable exceptionally = null;
    try {
      return ResponseEntity.ok(UrnResponseMap.builder()
          .responses(MappingUtil.mapServiceResponse(_entityService
              .getEntitiesV2(entityName, entityUrns, projectedAspects), _objectMapper))
          .build());
    } catch (Exception e) {
      exceptionally = e;
      throw new RuntimeException(
          String.format("Failed to batch get entities with urns: %s, projectedAspects: %s", entityUrns,
              projectedAspects), e);
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

    List<Pair<String, Boolean>> responses = aspectRequests.stream()
        .map(MappingUtil::mapToProposal)
        .map(proposal -> MappingUtil.ingestProposal(proposal, _entityService, _objectMapper))
        .collect(Collectors.toList());
    if (responses.stream().anyMatch(Pair::getSecond)) {
      return ResponseEntity.status(HttpStatus.CREATED)
          .body(responses.stream().filter(Pair::getSecond).map(Pair::getFirst).collect(Collectors.toList()));
    } else {
      return ResponseEntity.ok(Collections.emptyList());
    }
  }

  @DeleteMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<RollbackRunResultDto>> deleteEntities(
      @Parameter(name = "urns", required = true, description = "A list of raw urn strings, only supports a single entity type per request.")
      @RequestParam("urns") @Nonnull String[] urns,
      @Parameter(name = "soft", description = "Determines whether the delete will be soft or hard, defaults to true for soft delete")
      @RequestParam(value = "soft", defaultValue = "true") boolean soft) {
    Timer.Context context = MetricUtils.timer("deleteEntities").time();
    final Set<Urn> entityUrns =
        Arrays.stream(urns)
            // Have to decode here because of frontend routing, does No-op for already unencoded through direct API access
            .map(URLDecoder::decode)
            .map(UrnUtils::getUrn).collect(Collectors.toSet());
    Throwable exceptionally = null;
    try {
      if (!soft) {

        return ResponseEntity.ok(entityUrns.stream()
            .map(_entityService::deleteUrn)
            .map(rollbackRunResult -> MappingUtil.mapRollbackRunResult(rollbackRunResult, _objectMapper))
            .collect(Collectors.toList()));
      } else {
        List<UpsertAspectRequest> deleteRequests = entityUrns.stream()
            .map(entityUrn -> MappingUtil.createStatusRemoval(entityUrn, _entityService))
            .collect(Collectors.toList());
        return ResponseEntity.ok(Collections.singletonList(RollbackRunResultDto.builder()
            .rowsRolledBack(deleteRequests.stream()
                .map(MappingUtil::mapToProposal)
                .map(proposal -> MappingUtil.ingestProposal(proposal, _entityService, _objectMapper))
                .filter(Pair::getSecond)
                .map(Pair::getFirst)
                .map(urnString -> new AspectRowSummary().urn(urnString))
                .collect(Collectors.toList()))
            .rowsDeletedFromEntityDeletion(deleteRequests.size())
            .build()));
      }
    } catch (Exception e) {
      exceptionally = e;
      throw new RuntimeException(
          String.format("Failed to batch delete entities with urns: %s", entityUrns), e);
    } finally {
      if (exceptionally != null) {
        MetricUtils.counter(MetricRegistry.name("getEntities", "failed")).inc();
      } else {
        MetricUtils.counter(MetricRegistry.name("getEntities", "success")).inc();
      }
      context.stop();
    }
  }
}
