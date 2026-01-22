package com.linkedin.datahub.graphql.types.auth.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuthLocation;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServer;
import com.linkedin.datahub.graphql.generated.OAuthAuthorizationServerProperties;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.generated.TokenAuthMethod;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps a GMS OAuthAuthorizationServer entity response to the GraphQL type. */
public class OAuthAuthorizationServerMapper {

  /**
   * Maps a GMS entity response to a GraphQL OAuthAuthorizationServer.
   *
   * @param context The query context
   * @param entityResponse The GMS entity response
   * @return The mapped OAuthAuthorizationServer, or null if required aspects are missing
   */
  @Nullable
  public static OAuthAuthorizationServer map(
      @Nonnull final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final OAuthAuthorizationServer result = new OAuthAuthorizationServer();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.OAUTH_AUTHORIZATION_SERVER);

    // Map authorization server properties
    final EnvelopedAspect envelopedProperties =
        aspects.get(Constants.OAUTH_AUTHORIZATION_SERVER_PROPERTIES_ASPECT_NAME);
    if (envelopedProperties != null) {
      result.setProperties(
          mapProperties(
              new com.linkedin.oauth.OAuthAuthorizationServerProperties(
                  envelopedProperties.getValue().data())));
    }

    // Map ownership
    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    // Map status
    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      result.setStatus(StatusMapper.map(context, new Status(envelopedStatus.getValue().data())));
    }

    return result;
  }

  private static OAuthAuthorizationServerProperties mapProperties(
      @Nonnull final com.linkedin.oauth.OAuthAuthorizationServerProperties gmsProperties) {
    final OAuthAuthorizationServerProperties result = new OAuthAuthorizationServerProperties();

    // Basic properties
    result.setDisplayName(gmsProperties.getDisplayName());
    if (gmsProperties.hasDescription()) {
      result.setDescription(gmsProperties.getDescription());
    }

    // Secret presence flag and URN (the URN is safe to expose - actual value is protected)
    result.setHasClientSecret(gmsProperties.hasClientSecretUrn());
    if (gmsProperties.hasClientSecretUrn()) {
      result.setClientSecretUrn(gmsProperties.getClientSecretUrn().toString());
    }

    // OAuth config (public fields only)
    if (gmsProperties.hasClientId()) {
      result.setClientId(gmsProperties.getClientId());
    }
    if (gmsProperties.hasAuthorizationUrl()) {
      result.setAuthorizationUrl(gmsProperties.getAuthorizationUrl());
    }
    if (gmsProperties.hasTokenUrl()) {
      result.setTokenUrl(gmsProperties.getTokenUrl());
    }
    if (gmsProperties.hasScopes()) {
      result.setScopes(new ArrayList<>(gmsProperties.getScopes()));
    }

    // Token auth method
    result.setTokenAuthMethod(
        TokenAuthMethod.valueOf(gmsProperties.getTokenAuthMethod().toString()));

    // Additional parameters
    if (gmsProperties.hasAdditionalTokenParams()) {
      result.setAdditionalTokenParams(mapStringMap(gmsProperties.getAdditionalTokenParams()));
    }
    if (gmsProperties.hasAdditionalAuthParams()) {
      result.setAdditionalAuthParams(mapStringMap(gmsProperties.getAdditionalAuthParams()));
    }

    // Auth injection config
    result.setAuthLocation(AuthLocation.valueOf(gmsProperties.getAuthLocation().toString()));
    result.setAuthHeaderName(gmsProperties.getAuthHeaderName());
    if (gmsProperties.hasAuthScheme()) {
      result.setAuthScheme(gmsProperties.getAuthScheme());
    }
    if (gmsProperties.hasAuthQueryParam()) {
      result.setAuthQueryParam(gmsProperties.getAuthQueryParam());
    }

    return result;
  }

  private static List<StringMapEntry> mapStringMap(@Nonnull final Map<String, String> map) {
    final List<StringMapEntry> result = new ArrayList<>();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      final StringMapEntry mapEntry = new StringMapEntry();
      mapEntry.setKey(entry.getKey());
      mapEntry.setValue(entry.getValue());
      result.add(mapEntry);
    }
    return result;
  }

  private OAuthAuthorizationServerMapper() {}
}
