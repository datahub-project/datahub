package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.SECRETS_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SECRET_VALUE_ASPECT_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import com.linkedin.settings.global.FeatureSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

public class AiAssistantConfigService {

  private static final String AI_ASSISTANT_CONFIG_VERSION = "1";

  public enum Provider {
    CLAUDE,
    OPENAI
  }

  public enum Model {
    SONNET,
    OPUS,
    GPT_5_5
  }

  private static final Map<String, String> MODEL_PROVIDER_MAP =
      Map.of(
          "claude-sonnet-5", "claude",
          "claude-sonnet-4-5", "claude",
          "claude-opus-4", "claude",
          "claude-haiku-4-5", "claude",
          "gpt-5", "openai");

  private static final Set<String> SUPPORTED_PROVIDERS = Set.of("claude", "openai");

  private final AiAssistantConfigPlatformService platformService;
  private final ObjectMapper objectMapper;

  public AiAssistantConfigService(
      AiAssistantConfigPlatformService platformService, ObjectMapper objectMapper) {
    this.platformService = platformService;
    this.objectMapper = objectMapper;
  }

  public ProviderKeyResult upsertProviderKey(@Nonnull String provider, @Nonnull String apiKey) {
    final String normalizedProvider = normalizeProvider(provider);
    final String trimmedApiKey = requireNonEmpty(apiKey, "apiKey");
    final String secretName = getSecretName(normalizedProvider);
    final Urn secretUrn = getSecretUrn(secretName);

    try {
      if (platformService.exists(secretUrn)) {
        final EntityResponse existingSecret =
            platformService.get(secretUrn, Set.of(SECRET_VALUE_ASPECT_NAME));
        final MetadataChangeProposal proposal =
            buildSecretProposal(
                secretUrn,
                buildSecretValue(
                    existingSecret, secretName, platformService.encrypt(trimmedApiKey)));
        platformService.ingestProposal(proposal);
      } else {
        final DataHubSecretKey key = new DataHubSecretKey();
        key.setId(secretName);
        final MetadataChangeProposal proposal =
            buildSecretProposal(
                key, buildSecretValue(null, secretName, platformService.encrypt(trimmedApiKey)));
        platformService.ingestProposal(proposal);
      }

      final AiAssistantSettingsConfig settingsConfig = getSettingsConfig();
      settingsConfig
          .getProviderCredentials()
          .put(
              normalizedProvider,
              ProviderCredentialMetadata.builder().keyPreview(maskApiKey(trimmedApiKey)).build());
      saveSettingsConfig(settingsConfig);

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

  public ProviderKeyResult getProviderKey(@Nonnull String provider) {
    final String normalizedProvider = normalizeProvider(provider);
    final AiAssistantSettingsConfig settingsConfig = getSettingsConfig();
    final boolean hasKey = hasSecret(normalizedProvider);
    final ProviderCredentialMetadata metadata =
        settingsConfig.getProviderCredentials().get(normalizedProvider);

    return ProviderKeyResult.builder()
        .provider(normalizedProvider)
        .hasKey(hasKey)
        .updated(false)
        .keyPreview(hasKey && metadata != null ? metadata.getKeyPreview() : null)
        .build();
  }

  public ProvidersResult getProviders() {
    return ProvidersResult.builder().providers(List.of(Provider.values())).build();
  }

  public ModelsResult getModels() {
    return ModelsResult.builder().models(List.of(Model.values())).build();
  }

  public PreferredModelResult getPreferredModel() {
    final AiAssistantSettingsConfig settingsConfig = getSettingsConfig();
    final String preferredModel = settingsConfig.getPreferredModel();
    if (preferredModel == null) {
      return PreferredModelResult.builder().model(null).hasKey(false).keyPreview(null).build();
    }

    final String provider = resolveProvider(preferredModel);
    final boolean hasKey = hasSecret(provider);
    final ProviderCredentialMetadata metadata =
        settingsConfig.getProviderCredentials().get(normalizedProvider(provider));

    return PreferredModelResult.builder()
        .model(preferredModel)
        .hasKey(hasKey)
        .keyPreview(hasKey && metadata != null ? metadata.getKeyPreview() : null)
        .build();
  }

  public UpdatePreferredModelResult updatePreferredModel(@Nonnull String model) {
    final String normalizedModel = normalizeModel(model);
    resolveProvider(normalizedModel);

    final AiAssistantSettingsConfig settingsConfig = getSettingsConfig();
    settingsConfig.setPreferredModel(normalizedModel);
    saveSettingsConfig(settingsConfig);

    return UpdatePreferredModelResult.builder().model(normalizedModel).updated(true).build();
  }

  private boolean hasSecret(@Nonnull String provider) {
    try {
      return platformService.exists(getSecretUrn(getSecretName(provider)));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to determine whether provider %s has a configured key", provider),
          e);
    }
  }

  private String resolveProvider(@Nonnull String model) {
    final String normalizedModel = normalizeModel(model);
    final String provider = MODEL_PROVIDER_MAP.get(normalizedModel);
    if (provider == null) {
      throw new IllegalArgumentException(String.format("Unsupported model '%s'.", model));
    }
    return provider;
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
    return "AI_PROVIDER__" + normalizedProvider(provider).toUpperCase(Locale.ROOT) + "__API_KEY";
  }

  private static String normalizedProvider(@Nonnull String provider) {
    return provider.toLowerCase(Locale.ROOT);
  }

  private static Urn getSecretUrn(@Nonnull String secretName) {
    final DataHubSecretKey key = new DataHubSecretKey();
    key.setId(secretName);
    return EntityKeyUtils.convertEntityKeyToUrn(key, SECRETS_ENTITY_NAME);
  }

  private DataHubSecretValue buildSecretValue(
      EntityResponse existingSecret, String name, String encryptedValue) {
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
              .setActor(platformService.getActorUrn())
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

  private AiAssistantSettingsConfig getSettingsConfig() {
    final GlobalSettingsInfo globalSettings = getOrCreateGlobalSettings();
    if (!globalSettings.hasAiAssistant() || globalSettings.getAiAssistant().getConfig() == null) {
      return new AiAssistantSettingsConfig();
    }
    try {
      return objectMapper.readValue(
          globalSettings.getAiAssistant().getConfig(), AiAssistantSettingsConfig.class);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse AI assistant settings config", e);
    }
  }

  private void saveSettingsConfig(@Nonnull AiAssistantSettingsConfig settingsConfig) {
    final GlobalSettingsInfo globalSettings = getOrCreateGlobalSettings();
    final FeatureSettings aiAssistantSettings =
        globalSettings.hasAiAssistant() ? globalSettings.getAiAssistant() : new FeatureSettings();
    aiAssistantSettings.setEnabled(true);
    aiAssistantSettings.setConfig(serializeSettingsConfig(settingsConfig));
    aiAssistantSettings.setConfigVersion(AI_ASSISTANT_CONFIG_VERSION, SetMode.REMOVE_IF_NULL);
    globalSettings.setAiAssistant(aiAssistantSettings);
    platformService.updateGlobalSettings(globalSettings);
  }

  private GlobalSettingsInfo getOrCreateGlobalSettings() {
    final GlobalSettingsInfo existing = platformService.getGlobalSettings();
    return existing != null ? existing : new GlobalSettingsInfo();
  }

  private String serializeSettingsConfig(@Nonnull AiAssistantSettingsConfig settingsConfig) {
    try {
      return objectMapper.writeValueAsString(settingsConfig);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize AI assistant settings config", e);
    }
  }

  private String maskApiKey(@Nonnull String apiKey) {
    final String trimmedApiKey = apiKey.trim();
    if (trimmedApiKey.length() <= 4) {
      return "****";
    }
    final int prefixLength = Math.min(7, Math.max(1, trimmedApiKey.length() - 4));
    return trimmedApiKey.substring(0, prefixLength)
        + "..."
        + trimmedApiKey.substring(trimmedApiKey.length() - 4);
  }

  @Data
  public static class AiAssistantSettingsConfig {
    private String preferredModel;
    private Map<String, ProviderCredentialMetadata> providerCredentials = new HashMap<>();
  }

  @Data
  @Builder
  @AllArgsConstructor
  public static class ProviderCredentialMetadata {
    private String keyPreview;

    public ProviderCredentialMetadata() {}
  }

  @Data
  @Builder
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
