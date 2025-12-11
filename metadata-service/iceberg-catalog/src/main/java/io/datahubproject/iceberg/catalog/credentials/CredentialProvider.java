/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog.credentials;

import com.linkedin.metadata.authorization.PoliciesConfig;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public interface CredentialProvider {

  @EqualsAndHashCode
  @AllArgsConstructor
  class CredentialsCacheKey {
    public final String platformInstance;
    public final PoliciesConfig.Privilege privilege;
    public final Set<String> locations;
  }

  @AllArgsConstructor
  class StorageProviderCredentials {
    public final String clientId;
    public final String clientSecret;
    public final String role;
    public final String region;
    public final Integer tempCredentialExpirationSeconds;
  }

  Map<String, String> getCredentials(
      CredentialsCacheKey key, StorageProviderCredentials storageProviderCredentials);
}
