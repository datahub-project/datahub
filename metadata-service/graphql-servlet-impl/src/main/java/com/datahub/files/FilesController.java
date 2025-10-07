package com.datahub.files;

import com.linkedin.datahub.graphql.util.S3Util;
import jakarta.servlet.http.HttpServletRequest;
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

import java.net.URI;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api")
public class FilesController {

  @Autowired
  @Qualifier("s3Util")
  private S3Util s3Util;

  private static final int DEFAULT_EXPIRATION_SECONDS = 3600; // 1 hour
  private static final int MAX_EXPIRATION_SECONDS = 604800; // 7 days

  /**
   * Endpoint to serve files by generating presigned S3 URLs and redirecting to them.
   * 
   * @param fileId UUID of the file to serve
   * @param expirationSeconds Optional expiration time for the presigned URL (default: 3600 seconds)
   * @param request HTTP servlet request
   * @return Redirect response to the presigned S3 URL
   */
  @GetMapping("/files/{fileId}")
  public ResponseEntity<Void> getFile(
      @PathVariable("fileId") String fileId,
      @RequestParam(value = "expiration", required = false) Integer expirationSeconds,
      HttpServletRequest request) {
    
    try {
      // Validate fileId is a valid UUID
      UUID.fromString(fileId);
    } catch (IllegalArgumentException e) {
      log.warn("Invalid file ID format: {}", fileId);
      return ResponseEntity.badRequest().build();
    }

    // Validate and set expiration time
    int expiration = expirationSeconds != null ? expirationSeconds : DEFAULT_EXPIRATION_SECONDS;
    if (expiration <= 0 || expiration > MAX_EXPIRATION_SECONDS) {
      log.warn("Invalid expiration time: {}. Must be between 1 and {} seconds", expiration, MAX_EXPIRATION_SECONDS);
      return ResponseEntity.badRequest().build();
    }

    try {
      // TODO: This is a placeholder implementation. You'll need to:
      // 1. Map the fileId to the actual S3 bucket and key
      // 2. Implement proper authorization checks
      // 3. Handle file metadata lookup
      
      // For now, this is a basic structure that assumes:
      // - You have a way to resolve fileId to S3 location
      // - You have proper S3 configuration in place
      
      // Example mapping (replace with your actual logic):
      String bucket = "your-datahub-files-bucket";
      String key = "files/" + fileId;
      
      // Generate presigned URL using the existing S3Util
      String presignedUrl = s3Util.generatePresignedDownloadUrl(bucket, key, expiration);
      
      log.info("Generated presigned URL for file ID: {}, expires in: {}s", fileId, expiration);
      
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
