/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.metadata.services;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import javax.annotation.Nonnull;

public class RestrictedService {
  public static final String RESTRICTED_ENTITY_TYPE = "restricted";

  private final SecretService secretService;

  public RestrictedService(@Nonnull SecretService secretService) {
    this.secretService = secretService;
  }

  public Urn encryptRestrictedUrn(@Nonnull final Urn entityUrn) {
    final String encryptedEntityUrn = this.secretService.encrypt(entityUrn.toString());
    try {
      return new Urn(RESTRICTED_ENTITY_TYPE, encryptedEntityUrn);
    } catch (Exception e) {
      throw new RuntimeException("Error when creating restricted entity urn", e);
    }
  }

  public Urn decryptRestrictedUrn(@Nonnull final Urn restrictedUrn) {
    final String encryptedUrn = restrictedUrn.getId();
    return UrnUtils.getUrn(this.secretService.decrypt(encryptedUrn));
  }
}
