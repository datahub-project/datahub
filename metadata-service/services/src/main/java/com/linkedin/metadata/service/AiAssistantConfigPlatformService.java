package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.mxe.MetadataChangeProposal;
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

  boolean exists(OperationContext opContext, Urn urn) throws Exception {
    return entityClient.exists(opContext, urn);
  }

  EntityResponse get(OperationContext opContext, Urn urn, Set<String> aspectNames)
      throws Exception {
    return entityClient.getV2(opContext, urn.getEntityType(), urn, aspectNames);
  }

  void ingestProposal(OperationContext opContext, MetadataChangeProposal proposal)
      throws Exception {
    entityClient.ingestProposal(opContext, proposal, false);
  }

  String encrypt(OperationContext opContext, String value) {
    return secretService.encrypt(opContext, value);
  }

  Urn getActorUrn(OperationContext opContext) {
    try {
      return Urn.createFromString(opContext.getAuthentication().getActor().toUrnStr());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Session actor is not a valid URN", e);
    }
  }

  CorpUserSettings getCorpUserSettings(OperationContext opContext, Urn userUrn) {
    return settingsService.getCorpUserSettings(opContext, userUrn);
  }

  void updateCorpUserSettings(OperationContext opContext, Urn userUrn, CorpUserSettings settings) {
    settingsService.updateCorpUserSettings(opContext, userUrn, settings);
  }
}
