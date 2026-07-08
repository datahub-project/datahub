package io.datahubproject.openapi.v1.timeline;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.datahub.authorization.ConjunctivePrivilegeGroup;
import com.datahub.authorization.DisjunctivePrivilegeGroup;
import com.datahub.authorization.EntitySpec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
@Tag(
    name = "Timeline",
    description =
        "An API for retrieving historical updates to entities and their related documentation.")
public class TimelineControllerV1 {

  private final OperationContext systemOperationContext;
  private final TimelineService _timelineService;
  private final AuthorizerChain _authorizerChain;

  @Value("${authorization.restApiAuthorization:false}")
  private Boolean restApiAuthorizationEnabled;

  /** Preferred OpenAPI timeline path (aligned with other `/openapi/v1/...` ops APIs). */
  @GetMapping(path = "/openapi/v1/timeline/{urn}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get timeline change history for an entity.")
  public ResponseEntity<List<ChangeTransaction>> getTimeline(
      HttpServletRequest request,
      @PathVariable("urn") String rawUrn,
      @RequestParam(name = "startTime", defaultValue = "-1") long startTime,
      @RequestParam(name = "endTime", defaultValue = "0") long endTime,
      @RequestParam(name = "raw", defaultValue = "false") boolean raw,
      @RequestParam(name = "categories") Set<ChangeCategory> categories)
      throws URISyntaxException, JsonProcessingException {
    return doGetTimeline(request, rawUrn, startTime, endTime, raw, categories);
  }

  /**
   * Legacy path retained for compatibility. Prefer {@code GET /openapi/v1/timeline/{urn}}.
   *
   * @deprecated use {@link #getTimeline}
   */
  @Deprecated
  @GetMapping(path = "/openapi/timeline/v1/{urn}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(deprecated = true, summary = "Deprecated; use GET /openapi/v1/timeline/{urn}")
  public ResponseEntity<List<ChangeTransaction>> getTimelineLegacy(
      HttpServletRequest request,
      @PathVariable("urn") String rawUrn,
      @RequestParam(name = "startTime", defaultValue = "-1") long startTime,
      @RequestParam(name = "endTime", defaultValue = "0") long endTime,
      @RequestParam(name = "raw", defaultValue = "false") boolean raw,
      @RequestParam(name = "categories") Set<ChangeCategory> categories)
      throws URISyntaxException, JsonProcessingException {
    return doGetTimeline(request, rawUrn, startTime, endTime, raw, categories);
  }

  private ResponseEntity<List<ChangeTransaction>> doGetTimeline(
      HttpServletRequest request,
      String rawUrn,
      long startTime,
      long endTime,
      boolean raw,
      Set<ChangeCategory> categories)
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
    // Entity-level view authorization (independent of restApiAuthorization flag):
    // a caller without view privileges on the target URN must not read its history.
    if (!AuthUtil.canViewEntity(opContext, urn)) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to view entity " + urn);
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
}
