package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.net.URISyntaxException;
import java.util.Set;

public class AiAssistantConfigPlatformService {

  private final SystemEntityClient entityClient;
  private final SecretService secretService;
  private final SettingsService settingsService;

  private final OperationContext systemOperationContext;

  public AiAssistantConfigPlatformService(
      SystemEntityClient entityClient,
      SecretService secretService,
      SettingsService settingsService,
      OperationContext systemOperationContext) {
    this.entityClient = entityClient;
    this.secretService = secretService;
    this.settingsService = settingsService;
    this.systemOperationContext = systemOperationContext;
  }

  boolean exists(Urn urn) throws Exception {
    return entityClient.exists(systemOperationContext, urn);
  }

  EntityResponse get(Urn urn, Set<String> aspectNames) throws Exception {
    return entityClient.getV2(systemOperationContext, urn.getEntityType(), urn, aspectNames);
  }

  void ingestProposal(MetadataChangeProposal proposal) throws Exception {
    entityClient.ingestProposal(systemOperationContext, proposal, false);
  }

  String encrypt(String value) {
    return secretService.encrypt(systemOperationContext, value);
  }

  Urn getActorUrn() {
    try {
      return Urn.createFromString(systemOperationContext.getAuthentication().getActor().toUrnStr());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("System operation context actor is not a valid URN", e);
    }
  }

  GlobalSettingsInfo getGlobalSettings() {
    return settingsService.getGlobalSettings(systemOperationContext);
  }

  void updateGlobalSettings(GlobalSettingsInfo globalSettings) {
    settingsService.updateGlobalSettings(systemOperationContext, globalSettings);
  }
}
