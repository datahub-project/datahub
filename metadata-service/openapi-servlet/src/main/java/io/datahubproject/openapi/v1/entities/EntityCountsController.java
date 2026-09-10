package io.datahubproject.openapi.v1.entities;

import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.metadata.authorization.ApiGroup;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountResult;
import com.linkedin.metadata.systemmetadata.KeyAspectEntityCountService;
import com.linkedin.metadata.systemmetadata.PlatformEntityCountResult;
import com.linkedin.metadata.systemmetadata.PlatformEntityCounts;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.datahubproject.openapi.v1.models.entities.EntityCountsResponseDto;
import io.datahubproject.openapi.v1.models.entities.EntityTypeCountResponseDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/v1/entities")
@Tag(
    name = "Entity Counts",
    description = "Entity counts from the system metadata index by key aspect.")
public class EntityCountsController {

  private static final String GROUP_BY_PLATFORM = "platform";

  private final AuthorizerChain authorizerChain;
  private final OperationContext systemOperationContext;
  private final KeyAspectEntityCountService keyAspectEntityCountService;
  private final PlatformEntityCounts platformEntityCounts;

  public EntityCountsController(
      AuthorizerChain authorizerChain,
      @Qualifier("systemOperationContext") OperationContext systemOperationContext,
      @Qualifier("keyAspectEntityCountService")
          KeyAspectEntityCountService keyAspectEntityCountService,
      @Qualifier("platformEntityCounts") PlatformEntityCounts platformEntityCounts) {
    this.authorizerChain = authorizerChain;
    this.systemOperationContext = systemOperationContext;
    this.keyAspectEntityCountService = keyAspectEntityCountService;
    this.platformEntityCounts = platformEntityCounts;
  }

  @GetMapping(path = "/counts", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get entity counts by type",
      description =
          "Returns active and soft-deleted entity counts per registry key aspect. "
              + "Omit types for all entity types. Pass groupBy=platform for a "
              + "(entityType, platform) breakdown from the entity search index "
              + "(live aggregation; skipCache does not apply). With groupBy=platform, "
              + "includeTotal sums only returned (entityType, platform) rows; types "
              + "without a searchable platform field are omitted.")
  public ResponseEntity<EntityCountsResponseDto> getEntityCounts(
      HttpServletRequest request,
      @Parameter(description = "Entity types to include. Omit for all registry entity types.")
          @RequestParam(name = "types", required = false)
          List<String> types,
      @Parameter(
              description =
                  "Optional breakdown. Supported: platform (search-index terms agg with "
                      + "NO_PLATFORM for missing values).")
          @RequestParam(name = "groupBy", required = false)
          String groupBy,
      @Parameter(
              description =
                  "When true, include totalCount rollups in the response. With "
                      + "groupBy=platform, totals sum only returned (entityType, platform) "
                      + "rows; types without a searchable platform field are omitted.")
          @RequestParam(name = "includeTotal", required = false, defaultValue = "false")
          boolean includeTotal,
      @Parameter(
              description =
                  "When true, bypass the distributed entity count cache. Ignored when "
                      + "groupBy=platform (platform counts are always computed live).")
          @RequestParam(name = "skipCache", required = false, defaultValue = "false")
          boolean skipCache) {
    OperationContext opContext = buildSession(request, "getEntityCounts", types);
    authorizeCountsRead(opContext);

    if (groupBy != null && !groupBy.isBlank()) {
      if (!GROUP_BY_PLATFORM.equalsIgnoreCase(groupBy.trim())) {
        throw new IllegalArgumentException(
            "Unsupported groupBy='" + groupBy + "'. Supported values: platform");
      }
      PlatformEntityCountResult platformResult =
          platformEntityCounts.getCountsByPlatform(opContext, types);
      return ResponseEntity.ok(
          EntityCountsResponseMapper.toPlatformBatchResponse(platformResult, includeTotal));
    }

    KeyAspectEntityCountResult result =
        keyAspectEntityCountService.getCounts(opContext, types, skipCache);
    return ResponseEntity.ok(EntityCountsResponseMapper.toBatchResponse(result, includeTotal));
  }

  @GetMapping(path = "/{entityType}/count", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Get entity count for one type",
      description = "Returns active and soft-deleted counts for a single entity type.")
  public ResponseEntity<EntityTypeCountResponseDto> getEntityTypeCount(
      HttpServletRequest request,
      @PathVariable("entityType") String entityType,
      @Parameter(description = "When true, include totalCount in the response.")
          @RequestParam(name = "includeTotal", required = false, defaultValue = "false")
          boolean includeTotal,
      @Parameter(description = "When true, bypass the distributed entity count cache.")
          @RequestParam(name = "skipCache", required = false, defaultValue = "false")
          boolean skipCache) {
    OperationContext opContext = buildSession(request, "getEntityTypeCount", List.of(entityType));
    authorizeCountsRead(opContext);

    KeyAspectEntityCountResult result =
        keyAspectEntityCountService.getCountForEntityType(
            opContext, entityType.toLowerCase(), skipCache);
    return ResponseEntity.ok(EntityCountsResponseMapper.toSingleResponse(result, includeTotal));
  }

  @Nonnull
  private OperationContext buildSession(
      @Nonnull HttpServletRequest request,
      @Nonnull String operationName,
      @Nonnull List<String> entityNames) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    return systemOperationContext.asSession(
        RequestContext.builder()
            .buildOpenapi(
                authentication.getActor().toUrnStr(), request, operationName, entityNames),
        authorizerChain,
        authentication);
  }

  private void authorizeCountsRead(@Nonnull OperationContext opContext) {
    if (!AuthUtil.isAPIAuthorized(opContext, ApiGroup.COUNTS, READ)) {
      throw new UnauthorizedException("User is unauthorized to get entity counts.");
    }
  }
}
