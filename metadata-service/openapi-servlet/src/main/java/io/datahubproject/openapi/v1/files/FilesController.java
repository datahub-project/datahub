package io.datahubproject.openapi.v1.files;

import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.file.DataHubFileInfo;
import com.linkedin.file.FileUploadScenario;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.aws.S3Util;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/openapi/v1/files")
@Tag(name = "DataHub Files API", description = "An API to expose DataHub files")
@Slf4j
public class FilesController {

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  @Autowired
  @Qualifier("configurationProvider")
  private ConfigurationProvider configProvider;

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Qualifier("systemOperationContext")
  @Autowired
  protected OperationContext systemOperationContext;

  @Qualifier("authorizerChain")
  @Autowired
  protected AuthorizerChain authorizationChain;

  private static final int MAX_EXPIRATION_SECONDS = 604800; // 7 days

  /**
   * Endpoint to serve files by generating presigned S3 URLs and redirecting to them.
   *
   * @param fileId UUID of the file to serve
   * @param expirationSeconds Optional expiration time for the presigned URL (default: 3600 seconds)
   * @param request HTTP servlet request
   * @return Redirect response to the presigned S3 URL
   */
  @GetMapping("/{folder}/{fileId}")
  public ResponseEntity<Void> getFile(
      @PathVariable("folder") String folder,
      @PathVariable("fileId") String fileId,
      @RequestParam(value = "expiration", required = false) Integer expirationSeconds,
      HttpServletRequest request) {

    OperationContext opContext =
        buildOperationContext(
            "getFile", Collections.singleton(Constants.DATAHUB_FILE_ENTITY_NAME), request);

    // Validate user has permission to access this file
    validateFilePermissions(fileId, opContext);

    // Validate and set expiration time
    final int defaultExpirationSeconds =
        configProvider.getDatahub().getS3().getPresignedDownloadUrlExpirationSeconds();
    int expiration = expirationSeconds != null ? expirationSeconds : defaultExpirationSeconds;
    if (expiration <= 0 || expiration > MAX_EXPIRATION_SECONDS) {
      log.warn(
          "Invalid expiration time: {}. Must be between 1 and {} seconds",
          expiration,
          MAX_EXPIRATION_SECONDS);
      return ResponseEntity.badRequest().build();
    }

    try {
      String bucket = configProvider.getDatahub().getS3().getBucketName();
      if (bucket == null) {
        log.error("S3 bucket name not configured");
        return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
      }

      // Prefix file ID with bucket name
      String key = String.format("%s/%s", folder, fileId);

      // Generate presigned URL using the existing S3Util
      String presignedUrl = s3Util.generatePresignedDownloadUrl(bucket, key, expiration);
      log.info(
          "Generated presigned URL for folder: {}, file ID: {}, expires in: {}s",
          folder,
          fileId,
          expiration);

      // Return redirect response
      HttpHeaders headers = new HttpHeaders();
      headers.setLocation(URI.create(presignedUrl));

      return new ResponseEntity<>(headers, HttpStatus.FOUND);

    } catch (Exception e) {
      log.error("Failed to generate presigned URL for file ID: {}", fileId, e);
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * Helper method to build an OperationContext for the current authenticated user.
   *
   * @param action The action being performed (e.g., "getFile")
   * @param entityNames The set of entity names involved in the operation
   * @param request HTTP servlet request
   * @return OperationContext for the current user and action
   */
  private OperationContext buildOperationContext(
      String action, java.util.Set<String> entityNames, HttpServletRequest request) {
    Authentication authentication = AuthenticationContext.getAuthentication();
    return OperationContext.asSession(
        systemOperationContext,
        RequestContext.builder()
            .buildOpenapi(authentication.getActor().toUrnStr(), request, action, entityNames),
        authorizationChain,
        authentication,
        true);
  }

  /**
   * Validates that the user has permission to access the file based on the file's scenario.
   *
   * @param fileId The file ID containing the UUID
   * @param opContext The operation context for the current user
   * @throws UnauthorizedException if the user doesn't have permission to access the file
   */
  private void validateFilePermissions(String fileId, OperationContext opContext) {
    Authentication authentication = AuthenticationContext.getAuthentication();

    // Extract UUID from file ID
    String fileUUID = fileId.split(Constants.S3_FILE_ID_NAME_SEPARATOR)[0];
    Urn fileUrn = UrnUtils.getUrn("urn:li:dataHubFile:" + fileUUID);

    // Retrieve file entity and info
    EntityResponse response;
    try {
      response =
          entityService.getEntityV2(
              systemOperationContext,
              Constants.DATAHUB_FILE_ENTITY_NAME,
              fileUrn,
              new HashSet<>(Collections.singleton(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)),
              true);
    } catch (Exception e) {
      log.error("Failed to retrieve file entity: {}", fileUrn, e);
      throw new RuntimeException("Issue when retrieving file entity to check permissions", e);
    }

    if (response == null
        || !response.getAspects().containsKey(Constants.DATAHUB_FILE_INFO_ASPECT_NAME)) {
      throw new UnauthorizedException(
          "No dataHubFile entity associated with request to determine permissions");
    }

    DataHubFileInfo fileInfo =
        new DataHubFileInfo(
            response.getAspects().get(Constants.DATAHUB_FILE_INFO_ASPECT_NAME).getValue().data());

    // Check permissions based on file upload scenario
    FileUploadScenario scenario = fileInfo.getScenario();
    switch (scenario) {
      case ASSET_DOCUMENTATION:
        validateAssetDocumentationPermissions(fileInfo, opContext, authentication);
        break;
        // Add additional scenarios here as needed
      default:
        log.warn("Unknown file upload scenario: {}", scenario);
        throw new UnauthorizedException(
            "Unable to determine permissions for file upload scenario: " + scenario);
    }
  }

  /**
   * Validates permissions for files in the ASSET_DOCUMENTATION scenario.
   *
   * @param fileInfo The file info containing the referenced asset
   * @param opContext The operation context for the current user
   * @param authentication The current user's authentication
   * @throws UnauthorizedException if the user doesn't have READ permission on the referenced asset
   */
  private void validateAssetDocumentationPermissions(
      DataHubFileInfo fileInfo, OperationContext opContext, Authentication authentication) {
    Urn relatedUrn = fileInfo.getReferencedByAsset();
    if (!AuthUtil.isAPIAuthorizedEntityUrns(opContext, READ, Collections.singleton(relatedUrn))) {
      throw new UnauthorizedException(
          authentication.getActor().toUrnStr()
              + " is unauthorized to "
              + READ
              + " related entity to file with urn "
              + relatedUrn);
    }
  }
}
