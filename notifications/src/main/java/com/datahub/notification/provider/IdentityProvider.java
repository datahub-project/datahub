package com.datahub.notification.provider;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


/**
 * Provider of basic identity (user + group) information.
 */
@Slf4j
public class IdentityProvider {

  protected final EntityClient _entityClient;
  protected final Authentication _systemAuthentication;

  public IdentityProvider(
      @Nonnull final EntityClient entityClient,
      @Nonnull final Authentication systemAuthentication) {
    _entityClient = entityClient;
    _systemAuthentication = systemAuthentication;
  }

  /**
   * Returns a single user
   */
  public User getUser(final Urn userUrn) {
    return batchGetUsers(ImmutableSet.of(userUrn)).get(userUrn);
  }

  /**
   * Returns a list of User objects by URN.
   */
  public Map<Urn, User> batchGetUsers(final Set<Urn> userUrns) {
    try {
      final Map<Urn, EntityResponse> response =
          _entityClient.batchGetV2(
              CORP_USER_ENTITY_NAME,
              userUrns,
              ImmutableSet.of(CORP_USER_INFO_ASPECT_NAME, CORP_USER_EDITABLE_INFO_NAME, CORP_USER_STATUS_ASPECT_NAME),
              _systemAuthentication);
      final Map<Urn, User> result = new HashMap<>();
      for (Urn urn : userUrns) {
        if (response.containsKey(urn)) {
          result.put(urn, mapToUser(response.get(urn)));
        }
      }
      return result;
    } catch (RemoteInvocationException | URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to batch get users: %s", userUrns), e);
    }
  }

  private User mapToUser(final EntityResponse response) {
    final User user = new User();
    user.setUrn(response.getUrn());

    if (response.getAspects().containsKey(CORP_USER_INFO_ASPECT_NAME)) {
      final CorpUserInfo info = new CorpUserInfo(response.getAspects().get(CORP_USER_INFO_ASPECT_NAME).getValue().data());
      user.setTitle(info.getTitle(GetMode.NULL));
      user.setEmail(info.getEmail(GetMode.NULL));
      user.setDisplayName(info.getDisplayName(GetMode.NULL));
      user.setFirstName(info.getFirstName(GetMode.NULL));
      user.setLastName(info.getLastName(GetMode.NULL));
    }

    if (response.getAspects().containsKey(CORP_USER_EDITABLE_INFO_NAME)) {
      final CorpUserEditableInfo editableInfo = new CorpUserEditableInfo(response.getAspects().get(CORP_USER_EDITABLE_INFO_NAME).getValue().data());
      if (editableInfo.hasDisplayName()) {
        user.setDisplayName(editableInfo.getDisplayName());
      }
      if (editableInfo.hasEmail()) {
        user.setEmail(editableInfo.getEmail());
      }
      if (editableInfo.hasPhone()) {
        user.setPhone(editableInfo.getPhone());
      }
      if (editableInfo.hasTitle()) {
        user.setTitle(editableInfo.getTitle());
      }
      if (editableInfo.hasSlack()) {
        user.setSlack(editableInfo.getSlack());
      }
    }

    if (response.getAspects().containsKey(CORP_USER_STATUS_ASPECT_NAME)) {
      final CorpUserStatus status = new CorpUserStatus(response.getAspects().get(CORP_USER_STATUS_ASPECT_NAME).getValue().data());
      user.setActive(CORP_USER_STATUS_ACTIVE.equals(status.getStatus(GetMode.NULL)));
    }

    return user;
  }

  /**
   * A basic representation of a user.
   */
  @Data
  @Getter
  @Setter
  @NoArgsConstructor
  public static class User {
    private Urn urn;
    private String displayName;
    private String firstName;
    private String lastName;
    private String email;
    private String phone;
    private String slack;
    private String title;
    private boolean isActive;
    // TODO: User Preferences

    /**
     * Returns the appropriate display name to use from the set of user fields available.
     */
    public String getResolvedDisplayName() {
      if (displayName != null) {
        return displayName;
      }
      if (firstName != null && lastName != null) {
        return String.format("%s %s", firstName, lastName);
      }
      return urn.getId();
    }
  }
}