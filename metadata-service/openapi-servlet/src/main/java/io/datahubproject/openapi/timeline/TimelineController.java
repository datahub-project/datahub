package io.datahubproject.openapi.timeline;

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
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
@RequestMapping("/timeline/v1")
@Tag(
    name = "Timeline",
    description =
        "An API for retrieving historical updates to entities and their related documentation.")
public class TimelineController {

  private final TimelineService _timelineService;
  private final AuthorizerChain _authorizerChain;

  @Value("${authorization.restApiAuthorization:false}")
  private Boolean restApiAuthorizationEnabled;

  /**
   * @param rawUrn
   * @param startTime
   * @param endTime
   * @param raw
   * @param categories
   * @return
   * @throws URISyntaxException
   * @throws JsonProcessingException
   */
  @GetMapping(path = "/{urn}", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<List<ChangeTransaction>> getTimeline(
      @PathVariable("urn") String rawUrn,
      @RequestParam(defaultValue = "-1") long startTime,
      @RequestParam(defaultValue = "0") long endTime,
      @RequestParam(defaultValue = "false") boolean raw,
      @RequestParam Set<ChangeCategory> categories)
      throws URISyntaxException, JsonProcessingException {
    // Make request params when implemented
    String startVersionStamp = null;
    String endVersionStamp = null;
    Urn urn = Urn.createFromString(rawUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();
    EntitySpec resourceSpec = new EntitySpec(urn.getEntityType(), rawUrn);
    DisjunctivePrivilegeGroup orGroup =
        new DisjunctivePrivilegeGroup(
            ImmutableList.of(
                new ConjunctivePrivilegeGroup(
                    ImmutableList.of(PoliciesConfig.GET_TIMELINE_PRIVILEGE.getType()))));
    if (restApiAuthorizationEnabled
        && !AuthUtil.isAuthorized(
            _authorizerChain, actorUrnStr, Optional.of(resourceSpec), orGroup)) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to edit entities.");
    }
    return ResponseEntity.ok(
        _timelineService.getTimeline(
            urn, categories, startTime, endTime, startVersionStamp, endVersionStamp, raw));
  }
}
