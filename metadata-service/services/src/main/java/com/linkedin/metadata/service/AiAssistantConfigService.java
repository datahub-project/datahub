package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.SECRETS_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SECRET_VALUE_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
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
  private final AiAssistantConfigPlatformService platformService;

  public AiAssistantConfigService(AiAssistantConfigPlatformService platformService) {
    this.platformService = platformService;
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
        platformService.ingestProposal(
            buildSecretProposal(
                secretUrn,
                buildSecretValue(
                    existingSecret, secretName, platformService.encrypt(trimmedApiKey))));
      } else {
        final DataHubSecretKey key = new DataHubSecretKey();
        key.setId(secretName);
        platformService.ingestProposal(
            buildSecretProposal(
                key, buildSecretValue(null, secretName, platformService.encrypt(trimmedApiKey))));
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

  public ProviderKeyResult getProviderKey(@Nonnull String provider) {
    final String normalizedProvider = normalizeProvider(provider);

    return ProviderKeyResult.builder()
        .provider(normalizedProvider)
        .hasKey(hasSecret(normalizedProvider))
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

  private boolean hasSecret(@Nonnull String provider) {
    try {
      return platformService.exists(getSecretUrn(getSecretName(provider)));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to determine whether provider %s has a configured key", provider),
          e);
    }
  }

  private String normalizeProvider(@Nonnull String provider) {
    final String normalizedProvider =
        requireNonEmpty(provider, "provider").toLowerCase(Locale.ROOT);
    if (!SUPPORTED_PROVIDERS.contains(normalizedProvider)) {
      throw new IllegalArgumentException(String.format("Unsupported provider '%s'.", provider));
    }
    return normalizedProvider;
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
  public static class ProvidersResult {
    private List<Provider> providers;
  }

  @Data
  @Builder
  public static class ModelsResult {
    private List<Model> models;
  }
}
