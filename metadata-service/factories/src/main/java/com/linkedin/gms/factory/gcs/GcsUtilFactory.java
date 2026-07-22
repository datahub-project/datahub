package com.linkedin.gms.factory.gcs;

import com.google.cloud.storage.StorageOptions;
import com.linkedin.metadata.utils.gcs.GcsUtil;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the {@code gcsUtil} bean using Application Default Credentials — on GKE this resolves via
 * Workload Identity Federation with no static key file needed. Construction is cheap and lazy (the
 * client library only resolves credentials and makes network calls on first use), so unlike {@code
 * S3UtilFactory} there is no upfront "is this environment configured for GCP" signal to gate on:
 * any failure (e.g. no ambient credentials on a non-GCP deployment) is caught here and degrades to
 * a {@code null} bean rather than failing GMS startup.
 */
@Slf4j
@Configuration
public class GcsUtilFactory {

  @Bean(name = "gcsUtil")
  @Nullable
  protected GcsUtil getInstance() {
    try {
      return new GcsUtil(StorageOptions.getDefaultInstance().getService());
    } catch (Exception e) {
      log.debug("Skipping GcsUtil creation (no ambient GCP credentials available)", e);
      return null;
    }
  }
}
