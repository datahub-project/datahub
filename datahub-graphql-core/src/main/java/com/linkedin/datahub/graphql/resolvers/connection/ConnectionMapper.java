package com.linkedin.datahub.graphql.resolvers.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.DataHubConnectionDetails;
import com.linkedin.datahub.graphql.generated.DataHubJsonConnection;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.services.SecretService;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectionMapper {
  /**
   * Maps a GMS encrypted connection details object into the decrypted form returned by the GraphQL
   * API.
   *
   * <p>Returns null if the Entity does not have the required aspects: dataHubConnectionDetails or
   * dataPlatformInstance.
   */
  @Nullable
  public static DataHubConnection map(
      @Nonnull final QueryContext context,
      @Nonnull final EntityResponse entityResponse,
      @Nonnull final SecretService secretService,
      @Nonnull final FeatureFlags featureFlags) {
    // If the connection does not exist, simply return null
    if (!hasAspects(entityResponse)) {
      return null;
    }

    final DataHubConnection result = new DataHubConnection();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DATAHUB_CONNECTION);

    final EnvelopedAspect envelopedAssertionInfo =
        aspects.get(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME);
    if (envelopedAssertionInfo != null) {
      result.setDetails(
          mapConnectionDetails(
              context,
              new com.linkedin.connection.DataHubConnectionDetails(
                  envelopedAssertionInfo.getValue().data()),
              secretService,
              featureFlags));
    }
    final EnvelopedAspect envelopedPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      result.setPlatform(mapPlatform(new DataPlatformInstance(data)));
    }
    return result;
  }

  private static DataHubConnectionDetails mapConnectionDetails(
      @Nonnull final QueryContext context,
      @Nonnull final com.linkedin.connection.DataHubConnectionDetails gmsDetails,
      @Nonnull final SecretService secretService,
      @Nonnull final FeatureFlags featureFlags) {
    final DataHubConnectionDetails result = new DataHubConnectionDetails();
    result.setType(
        com.linkedin.datahub.graphql.generated.DataHubConnectionDetailsType.valueOf(
            gmsDetails.getType().toString()));
    if (gmsDetails.hasJson() && ConnectionUtils.canManageConnections(context)) {
      result.setJson(
          mapJsonConnectionDetails(
              gmsDetails.getJson(),
              secretService,
              featureFlags,
              context.getActorUrn().equals(Constants.SYSTEM_ACTOR)));
    }
    if (gmsDetails.hasName()) {
      result.setName(gmsDetails.getName());
    }
    return result;
  }

  private static DataHubJsonConnection mapJsonConnectionDetails(
      @Nonnull final com.linkedin.connection.DataHubJsonConnection gmsJsonConnection,
      @Nonnull final SecretService secretService,
      @Nonnull final FeatureFlags featureFlags,
      final boolean isSystemCaller) {
    final DataHubJsonConnection result = new DataHubJsonConnection();
    // Decrypt the BLOB!
    result.setBlob(secretService.decrypt(gmsJsonConnection.getEncryptedBlob()));

    // Obfuscate slack tokens if needed
    if (featureFlags.isSlackBotTokensObfuscationEnabled() && !isSystemCaller) {
      return tryObfuscateSlackBotTokens(result);
    }
    return result;
  }

  @VisibleForTesting
  public static DataHubJsonConnection tryObfuscateSlackBotTokens(DataHubJsonConnection result) {
    // Check for presence of...
    // a) bot_token
    final boolean isJsonContainsBotToken = result.getBlob().contains("\"bot_token\"");
    // b) app_details tokens
    final boolean isJsonContainsAppDetailTokens = result.getBlob().contains("\"app_details\"");
    // c) app_config_tokens
    final boolean isJsonContainsAppConfigTokens =
        result.getBlob().contains("\"app_config_tokens\"");

    // If none of the above exist, skip obfuscation
    final boolean shouldObfuscate =
        isJsonContainsBotToken || isJsonContainsAppDetailTokens || isJsonContainsAppConfigTokens;
    if (!shouldObfuscate) {
      return result;
    }

    // Else continue with obfuscation
    try {
      final Map<String, Object> parsedJson =
          new ObjectMapper().readValue(result.getBlob(), HashMap.class);

      // 1. Obfuscate bot_token
      if (isJsonContainsBotToken) {
        obfuscateBotToken(parsedJson);
      }

      // 2. Obfuscate app_details tokens
      if (isJsonContainsAppDetailTokens) {
        obfuscateAppDetailTokens(parsedJson);
      }

      // 3. Obfuscate app_config_tokens
      if (isJsonContainsAppConfigTokens) {
        obfuscateAppConfigTokens(parsedJson);
      }

      // 4. Write to blob
      final ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      result.setBlob(ow.writeValueAsString(parsedJson));
      return result;
    } catch (Exception e) {
      log.error("Failed to parse JSON content for obfuscation. Returning an empty string.", e);
      result.setBlob("");
      return result;
    }
  }

  private static void obfuscateAppConfigTokens(Map<String, Object> parsedJson) {
    final Map<String, String> appConfigTokensMap =
        (Map<String, String>) parsedJson.get("app_config_tokens");
    if (appConfigTokensMap != null) {
      final String accessToken = appConfigTokensMap.get("access_token");
      if (accessToken != null) {
        appConfigTokensMap.put("access_token", obfuscateToken(accessToken));
      }
      final String refreshToken = appConfigTokensMap.get("refresh_token");
      if (refreshToken != null) {
        appConfigTokensMap.put("refresh_token", obfuscateToken(refreshToken));
      }
      parsedJson.put("app_config_tokens", appConfigTokensMap);
    }
  }

  private static void obfuscateAppDetailTokens(Map<String, Object> parsedJson) {
    final Map<String, String> appDetailsMap = (Map<String, String>) parsedJson.get("app_details");
    if (appDetailsMap != null) {
      final String clientSecret = appDetailsMap.get("client_secret");
      if (clientSecret != null) {
        appDetailsMap.put("client_secret", obfuscateToken(clientSecret));
      }
      final String signingSecret = appDetailsMap.get("signing_secret");
      if (signingSecret != null) {
        appDetailsMap.put("signing_secret", obfuscateToken(signingSecret));
      }
      final String verificationToken = appDetailsMap.get("verification_token");
      if (verificationToken != null) {
        appDetailsMap.put("verification_token", obfuscateToken(verificationToken));
      }
      parsedJson.put("app_details", appDetailsMap);
    }
  }

  private static void obfuscateBotToken(Map<String, Object> parsedJson) {
    final String botTokenStr = (String) parsedJson.get("bot_token");
    if (botTokenStr != null) {
      parsedJson.put("bot_token", obfuscateToken(botTokenStr));
    }
  }

  private static String obfuscateToken(String token) {
    if (token.isEmpty()) {
      return token;
    }
    final int tokenLength = token.length();
    if (tokenLength == 1) {
      return "*";
    }
    if (tokenLength < 4) {
      return "****" + token.charAt(tokenLength - 1);
    } else if (tokenLength < 6) {
      return token.charAt(0) + "****" + token.charAt(tokenLength - 1);
    }
    return token.substring(0, 2) + "****" + token.substring(tokenLength - 3);
  }

  private static DataPlatform mapPlatform(final DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved.
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private static boolean hasAspects(@Nonnull final EntityResponse response) {
    return response.hasAspects()
        && response.getAspects().containsKey(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)
        && response.getAspects().containsKey(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
  }

  private ConnectionMapper() {}
}
