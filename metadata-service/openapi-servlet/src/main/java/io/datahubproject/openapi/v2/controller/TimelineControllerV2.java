package io.datahubproject.openapi.v2.controller;

import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/openapi/v2/timeline/v1")
@Tag(
    name = "Timeline",
    description =
        "An API for retrieving historical updates to entities and their related documentation.")
public class TimelineControllerV2 {

  private static final int MAX_VERSION_WALK = 50;
  private static final String VERSION_SET_SEARCH_FIELD = "versionSet";

  private final OperationContext systemOperationContext;
  private final TimelineService _timelineService;
  private final AuthorizerChain _authorizerChain;
  private final EntityClient _entityClient;

  @Value("${authorization.restApiAuthorization:false}")
  private Boolean restApiAuthorizationEnabled;

  public TimelineControllerV2(
      OperationContext systemOperationContext,
      TimelineService timelineService,
      AuthorizerChain authorizerChain,
      @Qualifier("entityClient") EntityClient entityClient) {
    this.systemOperationContext = systemOperationContext;
    this._timelineService = timelineService;
    this._authorizerChain = authorizerChain;
    this._entityClient = entityClient;
  }

  @GetMapping(path = "/{urn}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<ChangeTransaction>> getTimeline(
      HttpServletRequest request,
      @PathVariable("urn") String rawUrn,
      @RequestParam(name = "startTime", defaultValue = "-1") long startTime,
      @RequestParam(name = "endTime", defaultValue = "0") long endTime,
      @RequestParam(name = "raw", defaultValue = "false") boolean raw,
      @RequestParam(name = "categories") Set<ChangeCategory> categories,
      @Parameter(
              description =
                  "When true and the entity belongs to a VersionSet, fetches and merges the "
                      + "timelines of ALL versions in the set into a unified chronological view.")
          @RequestParam(name = "includeVersionSet", defaultValue = "false")
          boolean includeVersionSet)
      throws URISyntaxException, JsonProcessingException {

    String startVersionStamp = null;
    String endVersionStamp = null;
    Urn urn = Urn.createFromString(rawUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getTimeline", urn.getEntityType())
                .withUsageOperation(UsageOperation.METADATA_QUERY),
            _authorizerChain,
            authentication,
            true);

    EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), rawUrn);
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.GET_TIMELINE_PRIVILEGE.getType()))));
    if (restApiAuthorizationEnabled && !AuthUtil.isAuthorized(opContext, orGroup, resourceSpec)) {
      throw new UnauthorizedException(
          actorUrnStr + " is unauthorized to get the timeline for entity " + urn);
    }
    if (!EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(opContext, READ, List.of(urn))) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to view entity " + urn);
    }

    if (includeVersionSet) {
      List<Urn> allUrns = resolveVersionSetUrns(urn, opContext);
      // Authorize every version before merging histories. REST has no partial-result signal, so
      // deny the whole request if any sibling is unauthorized under REST API authorization.
      if (!EntityAuthorizationUtils.isAPIAuthorizedEntityUrns(opContext, READ, allUrns)) {
        throw new UnauthorizedException(
            actorUrnStr + " is unauthorized to view one or more versions for entity " + urn);
      }
      // REST clients today only consume the merged transaction stream; skipped-URN count is
      // surfaced on the GraphQL surface (GetTimelineResult.skippedVersionCount). If REST
      // consumers later need the same signal, return a wrapper object here instead.
      return ResponseEntity.ok(
          _timelineService
              .getTimelineForUrns(opContext, allUrns, categories, raw)
              .getTransactions());
    }

    return ResponseEntity.ok(
        _timelineService.getTimeline(
            opContext,
            urn,
            categories,
            startTime,
            endTime,
            startVersionStamp,
            endVersionStamp,
            raw));
  }

  /**
   * Resolves all version URNs in the same VersionSet as {@code urn}. Falls back to a singleton list
   * if the entity is not versioned or discovery fails.
   */
  private List<Urn> resolveVersionSetUrns(Urn urn, OperationContext opContext) {
    try {
      EntityResponse entityResponse =
          _entityClient.getV2(
              opContext,
              urn.getEntityType(),
              urn,
              Collections.singleton(VERSION_PROPERTIES_ASPECT_NAME));

      if (entityResponse == null
          || !entityResponse.getAspects().containsKey(VERSION_PROPERTIES_ASPECT_NAME)) {
        return Collections.singletonList(urn);
      }

      VersionProperties vp =
          RecordUtils.toRecordTemplate(
              VersionProperties.class,
              entityResponse.getAspects().get(VERSION_PROPERTIES_ASPECT_NAME).getValue().data());
      Urn versionSetUrn = vp.getVersionSet();

      SearchResult searchResult =
          _entityClient.search(
              opContext,
              urn.getEntityType(),
              "*",
              QueryUtils.newFilter(
                  CriterionUtils.buildCriterion(
                      VERSION_SET_SEARCH_FIELD, Condition.EQUAL, versionSetUrn.toString())),
              null,
              0,
              MAX_VERSION_WALK);

      if (searchResult == null || searchResult.getEntities() == null) {
        return Collections.singletonList(urn);
      }

      List<Urn> urns =
          searchResult.getEntities().stream()
              .map(SearchEntity::getEntity)
              .collect(Collectors.toCollection(ArrayList::new));

      if (!urns.contains(urn)) {
        urns.add(urn);
      }
      return urns;

    } catch (Exception e) {
      log.warn(
          "Failed to resolve version set URNs for {}, falling back to single-entity: {}",
          urn,
          e.getMessage());
      return Collections.singletonList(urn);
    }
  }
}
