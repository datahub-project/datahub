package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.secret.SecretService;
import javax.annotation.Nonnull;

public class RestrictedService {

  private final SecretService _secretService;

  public RestrictedService(@Nonnull SecretService secretService) {
    this._secretService = secretService;
  }

  public Urn encryptRestrictedUrn(@Nonnull final Urn entityUrn) {
    final String encryptedEntityUrn = this._secretService.encrypt(entityUrn.toString());
    try {
      return new Urn("restricted", encryptedEntityUrn);
    } catch (Exception e) {
      throw new RuntimeException("Error when creating restricted entity urn", e);
    }
  }

  public Urn decryptRestrictedUrn(@Nonnull final Urn restrictedUrn) {
    final String encryptedUrn = restrictedUrn.getId();
    return UrnUtils.getUrn(this._secretService.decrypt(encryptedUrn));
  }
}
