package io.datahubproject.openapi.v1.files;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.aws.S3Util;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
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
    // TODO: Add permission checks

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
      String key = String.format("%s/%s/%s", bucket, folder, fileId);

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
}
