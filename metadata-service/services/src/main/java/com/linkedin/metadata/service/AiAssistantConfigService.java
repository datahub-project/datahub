package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.SECRETS_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SECRET_VALUE_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.identity.CorpUserAIAssistantSettings;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

public class AiAssistantConfigService {

  public enum Provider {
    CLAUDE,
    OPENAI
  }

  public enum Model {
    SONNET,
    OPUS,
    GPT_5_5
  }

  private static final Set<String> SUPPORTED_PROVIDERS = Set.of("claude", "openai");
  private final AiAssistantConfigPersistenceService persistenceService;

  public AiAssistantConfigService(AiAssistantConfigPersistenceService persistenceService) {
    this.persistenceService = persistenceService;
  }

  public ProviderKeyResult upsertProviderKey(
      @Nonnull OperationContext opContext, @Nonnull String provider, @Nullable String apiKey) {
    final String normalizedProvider = normalizeProvider(provider);
    final String secretName = getSecretName(normalizedProvider);
    final Urn secretUrn = getSecretUrn(secretName);

    try {
      if (apiKey == null) {
        if (persistenceService.exists(opContext, secretUrn)) {
          persistenceService.deleteUrn(opContext, secretUrn);
        }

        return ProviderKeyResult.builder()
            .provider(normalizedProvider)
            .hasKey(false)
            .updated(true)
            .build();
      }

      final String trimmedApiKey = requireNonEmpty(apiKey, "apiKey");
      if (persistenceService.exists(opContext, secretUrn)) {
        final EntityResponse existingSecret =
            persistenceService.get(opContext, secretUrn, Set.of(SECRET_VALUE_ASPECT_NAME));
        persistenceService.ingestProposal(
            opContext,
            buildSecretProposal(
                secretUrn,
                buildSecretValue(
                    opContext,
                    existingSecret,
                    secretName,
                    persistenceService.encrypt(opContext, trimmedApiKey))));
      } else {
        final DataHubSecretKey key = new DataHubSecretKey();
        key.setId(secretName);
        persistenceService.ingestProposal(
            opContext,
            buildSecretProposal(
                key,
                buildSecretValue(
                    opContext,
                    null,
                    secretName,
                    persistenceService.encrypt(opContext, trimmedApiKey))));
      }

      return ProviderKeyResult.builder()
          .provider(normalizedProvider)
          .hasKey(true)
          .updated(true)
          .build();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert provider API key for provider %s", normalizedProvider),
          e);
    }
  }

  public ProviderKeyResult getProviderKey(
      @Nonnull OperationContext opContext, @Nonnull String provider) {
    final String normalizedProvider = normalizeProvider(provider);

    return ProviderKeyResult.builder()
        .provider(normalizedProvider)
        .hasKey(hasSecret(opContext, normalizedProvider))
        .updated(false)
        .keyPreview(null)
        .build();
  }

  public ProvidersResult getProviders() {
    return ProvidersResult.builder().providers(List.of(Provider.values())).build();
  }

  public ModelsResult getModels() {
    return ModelsResult.builder().models(List.of(Model.values())).build();
  }

  public PreferredModelResult getPreferredModel(@Nonnull OperationContext opContext) {
    final Urn actorUrn = persistenceService.getActorUrn(opContext);
    final CorpUserSettings userSettings =
        persistenceService.getCorpUserSettings(opContext, actorUrn);
    final String preferredModel =
        Optional.ofNullable(userSettings)
            .filter(CorpUserSettings::hasAiAssistant)
            .map(CorpUserSettings::getAiAssistant)
            .filter(CorpUserAIAssistantSettings::hasPreferredModel)
            .map(CorpUserAIAssistantSettings::getPreferredModel)
            .orElse(null);

    return PreferredModelResult.builder()
        .model(preferredModel)
        .hasKey(preferredModel != null && hasSecret(opContext, resolveProvider(preferredModel)))
        .keyPreview(null)
        .build();
  }

  public UpdatePreferredModelResult updatePreferredModel(
      @Nonnull OperationContext opContext, @Nonnull String model) {
    final String normalizedModel = normalizeModel(model);
    resolveProvider(normalizedModel);

    final Urn actorUrn = persistenceService.getActorUrn(opContext);
    final CorpUserSettings userSettings =
        Optional.ofNullable(persistenceService.getCorpUserSettings(opContext, actorUrn))
            .orElseGet(CorpUserSettings::new);
    if (!userSettings.hasAppearance()) {
      userSettings.setAppearance(new CorpUserAppearanceSettings());
    }
    final CorpUserAIAssistantSettings aiAssistantSettings =
        userSettings.hasAiAssistant()
            ? userSettings.getAiAssistant()
            : new CorpUserAIAssistantSettings();
    aiAssistantSettings.setPreferredModel(normalizedModel);
    userSettings.setAiAssistant(aiAssistantSettings);
    persistenceService.updateCorpUserSettings(opContext, actorUrn, userSettings);

    return UpdatePreferredModelResult.builder().model(normalizedModel).updated(true).build();
  }

  private boolean hasSecret(@Nonnull OperationContext opContext, @Nonnull String provider) {
    try {
      return persistenceService.exists(opContext, getSecretUrn(getSecretName(provider)));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to determine whether provider %s has a configured key", provider),
          e);
    }
  }

  private String resolveProvider(@Nonnull String model) {
    final String normalizedModel = normalizeModel(model);
    if (normalizedModel.startsWith("claude-")) {
      return "claude";
    }
    if (normalizedModel.startsWith("gpt-")) {
      return "openai";
    }
    throw new IllegalArgumentException(String.format("Unsupported model '%s'.", model));
  }

  private String normalizeProvider(@Nonnull String provider) {
    final String normalizedProvider =
        requireNonEmpty(provider, "provider").toLowerCase(Locale.ROOT);
    if (!SUPPORTED_PROVIDERS.contains(normalizedProvider)) {
      throw new IllegalArgumentException(String.format("Unsupported provider '%s'.", provider));
    }
    return normalizedProvider;
  }

  private String normalizeModel(@Nonnull String model) {
    return requireNonEmpty(model, "model").toLowerCase(Locale.ROOT);
  }

  private static String requireNonEmpty(@Nonnull String input, @Nonnull String fieldName) {
    final String trimmed = Objects.requireNonNull(input, fieldName + " must not be null").trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException(fieldName + " must not be empty.");
    }
    return trimmed;
  }

  private static String getSecretName(@Nonnull String provider) {
    return "AI_PROVIDER__" + provider.toUpperCase(Locale.ROOT) + "__API_KEY";
  }

  private static Urn getSecretUrn(@Nonnull String secretName) {
    final DataHubSecretKey key = new DataHubSecretKey();
    key.setId(secretName);
    return EntityKeyUtils.convertEntityKeyToUrn(key, SECRETS_ENTITY_NAME);
  }

  private DataHubSecretValue buildSecretValue(
      OperationContext opContext,
      EntityResponse existingSecret,
      String name,
      String encryptedValue) {
    final DataHubSecretValue value =
        existingSecret != null
            ? new DataHubSecretValue(
                existingSecret.getAspects().get(SECRET_VALUE_ASPECT_NAME).getValue().data())
            : new DataHubSecretValue();
    value.setName(name);
    value.setValue(encryptedValue);
    value.setDescription("AI assistant provider API key", SetMode.REMOVE_IF_NULL);
    if (existingSecret == null) {
      value.setCreated(
          new AuditStamp()
              .setActor(persistenceService.getActorUrn(opContext))
              .setTime(System.currentTimeMillis()));
    }
    return value;
  }

  private MetadataChangeProposal buildSecretProposal(Urn urn, DataHubSecretValue value) {
    return AspectUtils.buildMetadataChangeProposal(urn, SECRET_VALUE_ASPECT_NAME, value);
  }

  private MetadataChangeProposal buildSecretProposal(
      DataHubSecretKey key, DataHubSecretValue value) {
    return AspectUtils.buildMetadataChangeProposal(
        SECRETS_ENTITY_NAME, key, SECRET_VALUE_ASPECT_NAME, value);
  }

  @Data
  @Builder
  @AllArgsConstructor
  public static class ProviderKeyResult {
    private String provider;
    private boolean hasKey;
    private boolean updated;
    private String keyPreview;
  }

  @Data
  @Builder
  public static class PreferredModelResult {
    private String model;
    private boolean hasKey;
    private String keyPreview;
  }

  @Data
  @Builder
  public static class UpdatePreferredModelResult {
    private String model;
    private boolean updated;
  }

  @Data
  @Builder
  public static class ProvidersResult {
    private List<Provider> providers;
  }

  @Data
  @Builder
  public static class ModelsResult {
    private List<Model> models;
  }
}
