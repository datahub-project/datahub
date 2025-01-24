package io.datahubproject.openapi.v1.relationships;

import static com.linkedin.metadata.authorization.ApiGroup.RELATIONSHIP;
import static com.linkedin.metadata.authorization.ApiOperation.READ;
import static com.linkedin.metadata.search.utils.QueryUtils.*;

import com.codahale.metrics.MetricRegistry;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.propertyeditors.StringArrayPropertyEditor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/*
 Use v2 or v3 controllers instead
*/
@Deprecated
@RestController
@RequiredArgsConstructor
@RequestMapping("/openapi/relationships/v1")
@Slf4j
@Tag(name = "Relationships", description = "APIs for accessing relationships of entities")
public class RelationshipsController {

  public enum RelationshipDirection {
    INCOMING,
    OUTGOING
  }

  private static final int MAX_DOWNSTREAM_CNT = 200;
  private final OperationContext systemOperationContext;
  private final GraphService _graphService;
  private final AuthorizerChain _authorizerChain;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    binder.registerCustomEditor(String[].class, new StringArrayPropertyEditor(null));
  }

  private RelatedEntitiesResult getRelatedEntities(
      @Nonnull final OperationContext opContext,
      String rawUrn,
      List<String> relationshipTypes,
      RelationshipDirection direction,
      @Nullable Integer start,
      @Nullable Integer count) {

    start = start == null ? 0 : start;
    count = count == null ? MAX_DOWNSTREAM_CNT : count;
    com.linkedin.metadata.query.filter.RelationshipDirection restLiDirection;

    switch (direction) {
      case INCOMING:
        {
          restLiDirection = com.linkedin.metadata.query.filter.RelationshipDirection.INCOMING;
          break;
        }
      case OUTGOING:
        {
          restLiDirection = com.linkedin.metadata.query.filter.RelationshipDirection.OUTGOING;
          break;
        }
      default:
        {
          throw new RuntimeException("Unexpected relationship direction " + direction);
        }
    }

    return _graphService.findRelatedEntities(
        opContext,
        null,
        newFilter("urn", rawUrn),
        null,
        QueryUtils.EMPTY_FILTER,
        relationshipTypes,
        newRelationshipFilter(QueryUtils.EMPTY_FILTER, restLiDirection),
        start,
        count);
  }

  @GetMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      responses = {
        @ApiResponse(
            responseCode = "0",
            description = "",
            content = @Content(schema = @Schema(implementation = RelatedEntitiesResult.class)))
      })
  public ResponseEntity<RelatedEntitiesResult> getRelationships(
      HttpServletRequest request,
      @Parameter(
              name = "urn",
              required = true,
              description = "The urn for the entity whose relationships are being queried")
          @RequestParam("urn")
          @Nonnull
          String urn,
      @Parameter(
              name = "relationshipTypes",
              required = true,
              description = "The list of relationship types to traverse")
          @RequestParam(name = "relationshipTypes")
          @Nonnull
          String[] relationshipTypes,
      @Parameter(
              name = "direction",
              required = true,
              description = "The directionality of the relationship")
          @RequestParam(name = "direction")
          @Nonnull
          RelationshipsController.RelationshipDirection direction,
      @Parameter(
              name = "start",
              description =
                  "An offset for the relationships to return from. " + "Useful for pagination.")
          @RequestParam(name = "start", defaultValue = "0")
          @Nullable
          Integer start,
      @Parameter(
              name = "count",
              description =
                  "A count of relationships that will be returned "
                      + "starting from the offset. Useful for pagination.")
          @RequestParam(name = "count", defaultValue = "200")
          @Nullable
          Integer count) {

    // Have to decode here because of frontend routing, does No-op for already unencoded through
    // direct API access
    final Urn entityUrn = UrnUtils.getUrn(URLDecoder.decode(urn, Charset.forName("UTF-8")));
    log.debug("GET Relationships {}", entityUrn);
    Authentication authentication = AuthenticationContext.getAuthentication();
    String actorUrnStr = authentication.getActor().toUrnStr();

    OperationContext opContext =
        OperationContext.asSession(
            systemOperationContext,
            RequestContext.builder()
                .buildOpenapi(actorUrnStr, request, "getRelationships", entityUrn.getEntityType()),
            _authorizerChain,
            authentication,
            true);

    if (!AuthUtil.isAPIAuthorizedUrns(opContext, RELATIONSHIP, READ, List.of(entityUrn))) {
      throw new UnauthorizedException(actorUrnStr + " is unauthorized to get relationships.");
    }

    Throwable exceptionally = null;
    try {
      return ResponseEntity.ok(
          getRelatedEntities(
              opContext,
              entityUrn.toString(),
              Arrays.asList(relationshipTypes),
              direction,
              start,
              count));
    } catch (Exception e) {
      exceptionally = e;
      throw new RuntimeException(
          String.format(
              "Failed to batch get relationships with urn: %s, relationshipTypes: %s",
              urn, Arrays.toString(relationshipTypes)),
          e);
    } finally {
      if (exceptionally != null) {
        MetricUtils.counter(MetricRegistry.name("getRelationships", "failed")).inc();
      } else {
        MetricUtils.counter(MetricRegistry.name("getRelationships", "success")).inc();
      }
    }
  }
}
