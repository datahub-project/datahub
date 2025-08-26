package io.datahubproject.openapi.scim.repositories;

import static com.linkedin.metadata.Constants.*;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataHubRoleUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.identity.CorpUserCredentials;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.key.CorpUserKey;
import io.datahubproject.metadata.services.SecretService;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.scim.server.exception.UnableToResolveIdResourceException;
import org.apache.directory.scim.spec.exception.ResourceException;
import org.apache.directory.scim.spec.phonenumber.PhoneNumberParseException;
import org.apache.directory.scim.spec.resources.Email;
import org.apache.directory.scim.spec.resources.Name;
import org.apache.directory.scim.spec.resources.PhoneNumber;
import org.apache.directory.scim.spec.resources.Photo;
import org.apache.directory.scim.spec.resources.Role;
import org.apache.directory.scim.spec.resources.ScimUser;
import org.apache.directory.scim.spec.resources.UserGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ScimUserRepository extends AbstractScimRepository<ScimUser, CorpuserUrn, CorpUserKey> {

  static final String USER_ACTIVE = "ACTIVE";

  static final String USER_INACTIVE = "SUSPENDED";

  @Autowired
  @Qualifier("dataHubSecretService")
  private SecretService _secretService;

  @Override
  public Class<ScimUser> getResourceClass() {
    return ScimUser.class;
  }

  /*
   Write-only functions, ingest<aspectName> ()
  */
  private void ingestUserCreds(ScimUser scimUser, CorpuserUrn urn, AuditStamp auditStamp) {
    if (scimUser.getPassword() != null) {
      CorpUserCredentials userCreds = new CorpUserCredentials();
      final byte[] salt = _secretService.generateSalt(SALT_TOKEN_LENGTH);
      String encryptedSalt = _secretService.encrypt(Base64.getEncoder().encodeToString(salt));
      userCreds.setSalt(encryptedSalt);
      try {
        userCreds.setHashedPassword(_secretService.getHashedPassword(salt, scimUser.getPassword()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ingest(urn, CORP_USER_CREDENTIALS_ASPECT_NAME, userCreds, auditStamp);
    }
  }

  private CorpUserKey ingestUserKey(ScimUser scimUser, CorpuserUrn urn, AuditStamp auditStamp) {
    CorpUserKey userKey = new CorpUserKey();
    userKey.setUsername(scimUser.getUserName());
    ingest(urn, CORP_USER_KEY_ASPECT_NAME, userKey, auditStamp);
    return userKey;
  }

  /*
   Read-write functions ingest<aspectName> ()
  */
  private CorpUserStatus ingestUserStatus(
      ScimUser scimUser, AuditStamp auditStamp, CorpuserUrn urn, CorpUserStatus existing) {
    if (scimUser.getActive() != null) {
      CorpUserStatus userStatus = new CorpUserStatus();
      // NOTE: not setting as "PROVISIONED"
      userStatus.setStatus(scimUser.getActive() ? USER_ACTIVE : USER_INACTIVE);
      userStatus.setLastModified(auditStamp);

      if (existing == null || !userStatus.getStatus().equals(existing.getStatus())) {
        log.debug(
            String.format("UserStatus changed for user %s; %s ==> %s", urn, existing, userStatus));
        ingest(urn, CORP_USER_STATUS_ASPECT_NAME, userStatus, auditStamp);
        return userStatus;
      }
    }
    return existing;
  }

  private boolean isUserActive(ScimUser scimUser) {
    // defaulting to "true" as per SCIMple's default value for ScimUser.active
    // note that corpUserInfo.active is deprecated, yet mandatory; hence this is required.
    return scimUser.getActive() == null || scimUser.getActive();
  }

  /*
  If an array-field in SCIM is not specified in the resource for PUT, the field is cleared in the system.
   */
  private CorpUserInfo ingestUserInfo(
      ScimUser scimUser, CorpuserUrn urn, AuditStamp auditStamp, CorpUserInfo existing) {
    CorpUserInfo userInfo = null;
    try {
      userInfo = existing == null ? new CorpUserInfo() : new CorpUserInfo(existing.data().clone());
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    boolean ingest = false;

    if (scimUser.getDisplayName() != null) {
      userInfo.setDisplayName(scimUser.getDisplayName(), SetMode.IGNORE_NULL);
      ingest = true;
    }

    if (scimUser.getTitle() != null) {
      userInfo.setTitle(scimUser.getTitle(), SetMode.IGNORE_NULL);
      ingest = true;
    }

    if (scimUser.getName() != null) {
      userInfo.setFullName(scimUser.getName().getFormatted(), SetMode.IGNORE_NULL);
      userInfo.setLastName(scimUser.getName().getFamilyName(), SetMode.IGNORE_NULL);
      userInfo.setFirstName(scimUser.getName().getGivenName(), SetMode.IGNORE_NULL);
      userInfo.setTitle(scimUser.getTitle(), SetMode.IGNORE_NULL);
      ingest = true;
    }

    if (scimUser.getPrimaryEmailAddress().isPresent()) {
      userInfo.setEmail(scimUser.getPrimaryEmailAddress().get().getValue());
      ingest = true;
    } else if (existing != null && !Strings.isNullOrEmpty(existing.getEmail(GetMode.NULL))) {
      userInfo.removeEmail();
      ingest = true;
    }

    if (ingest) {
      if (existing == null
          || !Objects.equals(
              userInfo.getDisplayName(GetMode.NULL), existing.getDisplayName(GetMode.NULL))
          || !Objects.equals(userInfo.getTitle(GetMode.NULL), existing.getTitle(GetMode.NULL))
          || !Objects.equals(userInfo.getFullName(GetMode.NULL), existing.getFullName(GetMode.NULL))
          || !Objects.equals(userInfo.getLastName(GetMode.NULL), existing.getLastName(GetMode.NULL))
          || !Objects.equals(
              userInfo.getFirstName(GetMode.NULL), existing.getFirstName(GetMode.NULL))
          || !Objects.equals(userInfo.getTitle(GetMode.NULL), existing.getTitle(GetMode.NULL))) {
        userInfo.setActive(isUserActive(scimUser));
        log.debug(
            String.format("UserInfo changed for user %s; %s ==> %s ", urn, existing, userInfo));
        ingest(urn, CORP_USER_INFO_ASPECT_NAME, userInfo, auditStamp);
        return userInfo;
      }
    }
    return existing;
  }

  /*
   If an array-field in SCIM is not specified in the resource for PUT, the field is cleared in the system.
  */
  private CorpUserEditableInfo ingestUserEditableInfo(
      ScimUser scimUser, CorpuserUrn urn, AuditStamp auditStamp, CorpUserEditableInfo existing) {
    CorpUserEditableInfo userEditableInfo = null;
    try {
      userEditableInfo =
          existing == null
              ? new CorpUserEditableInfo()
              : new CorpUserEditableInfo(existing.data().clone());
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }

    boolean ingest = false;

    if (scimUser.getPrimaryEmailAddress().isPresent()) {
      userEditableInfo.setEmail(scimUser.getPrimaryEmailAddress().get().getValue());
      ingest = true;
    } else if (existing != null && !Strings.isNullOrEmpty(existing.getEmail(GetMode.NULL))) {
      userEditableInfo.removeEmail();
      ingest = true;
    }

    if (scimUser.getPrimaryPhoneNumber().isPresent()) {
      userEditableInfo.setPhone(scimUser.getPrimaryPhoneNumber().get().getValue());
      ingest = true;
    } else if (existing != null && !Strings.isNullOrEmpty(existing.getPhone(GetMode.NULL))) {
      userEditableInfo.removePhone();
      ingest = true;
    }

    if (scimUser.getPhotos() != null && !scimUser.getPhotos().isEmpty()) {
      Photo photo = scimUser.getPhotos().get(0);
      userEditableInfo.setPictureLink(new Url(photo.getValue()));
      ingest = true;
    } else if (existing != null && existing.getPictureLink(GetMode.NULL) != null) {
      userEditableInfo.removePictureLink();
      ingest = true;
    }

    if (ingest) {
      if (existing == null
          || !Objects.equals(
              userEditableInfo.getEmail(GetMode.NULL), existing.getEmail(GetMode.NULL))
          || !Objects.equals(
              userEditableInfo.getPhone(GetMode.NULL), existing.getPhone(GetMode.NULL))
          || !Objects.equals(
              userEditableInfo.getPictureLink(GetMode.NULL),
              existing.getPictureLink(GetMode.NULL))) {
        log.debug(
            String.format(
                "UserEditableInfo changed for user %s; %s ==> %s ",
                urn, existing, userEditableInfo));
        ingest(urn, CORP_USER_EDITABLE_INFO_ASPECT_NAME, userEditableInfo, auditStamp);
        return userEditableInfo;
      }
    }
    return existing;
  }

  private RoleMembership ingestRoleMembership(
      ScimUser scimUser, CorpuserUrn urn, AuditStamp auditStamp, RoleMembership existing) {

    /*
     If an array-field in SCIM is not specified in the resource for PUT, the field is cleared in the system.
     We follow the same thumb-rule for roleMembership as well below.
    */

    AtomicReference<Urn> primaryRole = new AtomicReference<>();

    List<Urn> expected =
        scimUser.getRoles() == null
            ? Collections.emptyList()
            : scimUser.getRoles().stream()
                .map(
                    role -> {
                      Urn roleUrn = new DataHubRoleUrn(role.getValue());
                      if (_entityService.exists(systemOperationContext, roleUrn, false)) {
                        if (primaryRole.getPlain() == null
                            && role.getPrimary() != null
                            && role.getPrimary()) {
                          primaryRole.setPlain(roleUrn);
                        }
                        return Optional.of(roleUrn);
                      } else {
                        log.warn(
                            String.format(
                                "Skipping unknown role %s assigned to user with id %s, urn %s",
                                role, scimUser.getId(), urn));
                        return Optional.<Urn>empty();
                      }
                    })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

    List<Urn> actual =
        existing == null || existing.getRoles(GetMode.NULL) == null
            ? Collections.emptyList()
            : existing.getRoles().stream().toList();

    List<Urn> rolesToAdd = listDiff(expected, actual);
    List<Urn> rolesToRemove = listDiff(actual, expected);

    RoleMembership roleMembership;

    if (!rolesToAdd.isEmpty() || !rolesToRemove.isEmpty()) {
      List<Urn> modifiedRoles = new LinkedList<>(actual);
      modifiedRoles.addAll(rolesToAdd);
      modifiedRoles.removeAll(rolesToRemove);

      /* Since DataHub does not have a notion of "primary" role, and that's required for MS Entra ID, we maintain
      the notion that the first role is "primary".
      Accordingly, if a primary role has been specified, move it to the start of the list.

      This approach can lead to an inconsistency when an IAM has no "primary" role - in that case, we
      would still end up maintaining a notion of "primary" role. This inconsistency would likely be benign, though.
       */
      if (primaryRole.getPlain() != null && modifiedRoles.remove(primaryRole.getPlain())) {
        modifiedRoles.add(0, primaryRole.getPlain());
      }

      roleMembership = new RoleMembership();
      roleMembership.setRoles(new UrnArray(modifiedRoles));

      log.debug(
          String.format(
              "RoleMembership changed for user %s; %s ==> %s ", urn, existing, roleMembership));
      ingest(urn, ROLE_MEMBERSHIP_ASPECT_NAME, roleMembership, auditStamp);
    } else {
      roleMembership = existing;
    }

    return roleMembership;
  }

  private static Map<Class<? extends RecordTemplate>, RecordTemplate> createAspectsMap(
      CorpUserStatus userStatus,
      CorpUserInfo userInfo,
      CorpUserEditableInfo userEditableInfo,
      RoleMembership roleMembership,
      GroupMembership groupMembership) {
    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects = new HashMap<>();
    aspects.put(CorpUserStatus.class, userStatus);
    aspects.put(CorpUserInfo.class, userInfo);
    aspects.put(CorpUserEditableInfo.class, userEditableInfo);
    aspects.put(RoleMembership.class, roleMembership);
    aspects.put(GroupMembership.class, groupMembership);
    return aspects;
  }

  /*
  Note that as per SCIM, groupMembership is specified with the SCIM-group; so we don't ingest groupMembership aspect information here.
   */
  @Override
  ScimUser ingestUpdates(
      ScimUser scimUser, ScimReadableCorpEntity corpUser, AuditStamp auditStamp) {
    CorpUserStatus existingUserStatus = corpUser.getAspect(CorpUserStatus.class);
    CorpUserInfo existingUserInfo = corpUser.getAspect(CorpUserInfo.class);
    CorpUserEditableInfo existingUserEditableInfo = corpUser.getAspect(CorpUserEditableInfo.class);
    RoleMembership existingRoleMembership = corpUser.getAspect(RoleMembership.class);

    CorpUserStatus userStatus =
        ingestUserStatus(scimUser, auditStamp, corpUser.getUrn(), existingUserStatus);
    CorpUserInfo userInfo =
        ingestUserInfo(scimUser, corpUser.getUrn(), auditStamp, existingUserInfo);
    CorpUserEditableInfo userEditableInfo =
        ingestUserEditableInfo(scimUser, corpUser.getUrn(), auditStamp, existingUserEditableInfo);
    RoleMembership roleMembership =
        ingestRoleMembership(scimUser, corpUser.getUrn(), auditStamp, existingRoleMembership);

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects =
        createAspectsMap(
            userStatus,
            userInfo,
            userEditableInfo,
            roleMembership,
            corpUser.getAspect(GroupMembership.class));

    boolean changed =
        userStatus != existingUserStatus
            || userInfo != existingUserInfo
            || userEditableInfo != existingUserEditableInfo
            || roleMembership != existingRoleMembership;

    if (scimUser.getPassword() != null) {
      // assume creds have changed
      ingestUserCreds(scimUser, corpUser.getUrn(), auditStamp);
      changed = true;
    }

    ScimReadableCorpEntity result =
        new ScimReadableCorpEntity(
            corpUser.getUrn(),
            corpUser.getKey(),
            corpUser.getCreated(),
            changed ? auditStamp.getTime() : corpUser.getLastModified(),
            aspects);

    return result.asScimResource();
  }

  @Override
  String getResourceType() {
    return ScimUser.RESOURCE_NAME;
  }

  @Override
  String attributeNameToFindBy() {
    return "userName";
  }

  @Override
  CorpuserUrn urnFromStr(String urnStr) throws UnableToResolveIdResourceException {
    try {
      return CorpuserUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw invalidResourceException(urnStr);
    }
  }

  @Override
  CorpuserUrn urnFromName(String name) {
    return new CorpuserUrn(name);
  }

  @Override
  String nameFromResource(ScimUser resource) {
    return resource.getUserName();
  }

  @Override
  String invalidResourceMsg(String id) {
    return "User " + id + " is invalid.";
  }

  @Override
  String resourceNotFoundMsg(String id) {
    return "User " + id + " not found.";
  }

  @Override
  String entityName() {
    return CORP_USER_ENTITY_NAME;
  }

  @Override
  protected String keyAspectName() {
    return CORP_USER_KEY_ASPECT_NAME;
  }

  @Override
  Map<String, Class<? extends RecordTemplate>> aspectNamesToClasses() {
    Map<String, Class<? extends RecordTemplate>> result = new HashMap<>();

    result.put(CORP_USER_KEY_ASPECT_NAME, CorpUserKey.class);
    result.put(CORP_USER_STATUS_ASPECT_NAME, CorpUserStatus.class);
    result.put(CORP_USER_INFO_ASPECT_NAME, CorpUserInfo.class);
    result.put(CORP_USER_EDITABLE_INFO_ASPECT_NAME, CorpUserEditableInfo.class);
    result.put(GROUP_MEMBERSHIP_ASPECT_NAME, GroupMembership.class);
    result.put(ROLE_MEMBERSHIP_ASPECT_NAME, RoleMembership.class);

    return result;
  }

  @Override
  ScimUser newScimResource() {
    return new ScimUser();
  }

  @Override
  void sinkKeyToResource(CorpUserKey key, ScimUser resource) {
    resource.setUserName(key.getUsername());
  }

  @Override
  void sinkAspectsToResource(
      CorpuserUrn urn,
      Map<Class<? extends RecordTemplate>, RecordTemplate> aspects,
      ScimUser resource) {
    userStatusToScim(resource, getAspect(aspects, CorpUserStatus.class));
    userInfoToScim(resource, getAspect(aspects, CorpUserInfo.class));
    userEditableInfoToScim(resource, getAspect(aspects, CorpUserEditableInfo.class));
    groupMembershipToScim(resource, getAspect(aspects, GroupMembership.class));
    roleMembershipToScim(resource, getAspect(aspects, RoleMembership.class));
  }

  @Override
  String endpointName() {
    return USERS_ENDPOINT;
  }

  /*
  Note that as per SCIM, groupMembership is specified with the SCIM-group; so we don't ingest groupMembership aspect information here.
   */
  @Override
  Map<Class<? extends RecordTemplate>, RecordTemplate> ingestCreate(
      ScimUser scimUser, CorpuserUrn urn, AuditStamp auditStamp) {
    ingestUserKey(scimUser, urn, auditStamp);
    ingestUserCreds(scimUser, urn, auditStamp);

    CorpUserStatus userStatus = ingestUserStatus(scimUser, auditStamp, urn, null);
    CorpUserInfo userInfo = ingestUserInfo(scimUser, urn, auditStamp, null);
    CorpUserEditableInfo userEditableInfo = ingestUserEditableInfo(scimUser, urn, auditStamp, null);
    RoleMembership roleMembership = ingestRoleMembership(scimUser, urn, auditStamp, null);

    Map<Class<? extends RecordTemplate>, RecordTemplate> aspects =
        createAspectsMap(userStatus, userInfo, userEditableInfo, roleMembership, null);

    return aspects;
  }

  @Override
  void checkKeyMutation(ScimUser scimUser, ScimReadableCorpEntity corpUser)
      throws ResourceException {
    if (scimUser.getUserName() != null
        && !scimUser.getUserName().equals(corpUser.getKey().getUsername())) {
      throw new ResourceException(
          HttpStatus.BAD_REQUEST.value(),
          "userName cannot be modified. "
              + corpUser.getKey().getUsername()
              + " => "
              + scimUser.getUserName());
    }
  }

  private void userStatusToScim(ScimUser scimUser, CorpUserStatus userStatus) {
    if (userStatus == null) {
      return;
    }
    if (userStatus.getStatus().equals(ScimUserRepository.USER_ACTIVE)) {
      scimUser.setActive(true);
    } else if (userStatus.getStatus().equals(ScimUserRepository.USER_INACTIVE)) {
      scimUser.setActive(false);
    }
  }

  private void userInfoToScim(ScimUser scimUser, CorpUserInfo userInfo) {
    if (userInfo == null) {
      return;
    }
    scimUser.setDisplayName(userInfo.getDisplayName(GetMode.NULL));
    scimUser.setTitle(userInfo.getTitle(GetMode.NULL));

    if (userInfo.getEmail(GetMode.NULL) != null) {
      Email email = new Email();
      email.setValue(userInfo.getEmail());
      email.setPrimary(true);
      scimUser.setEmails(ImmutableList.of(email));
    }

    if (userInfo.getFullName(GetMode.NULL) != null
        || userInfo.getLastName(GetMode.NULL) != null
        || userInfo.getFirstName(GetMode.NULL) != null) {
      Name name = new Name();
      scimUser.setName(name);
      name.setFormatted(userInfo.getFullName(GetMode.NULL));
      name.setFamilyName(userInfo.getLastName(GetMode.NULL));
      name.setGivenName(userInfo.getFirstName(GetMode.NULL));
    }
  }

  private void userEditableInfoToScim(ScimUser scimUser, CorpUserEditableInfo userEditableInfo) {
    if (userEditableInfo == null) {
      return;
    }
    if (userEditableInfo.getPhone(GetMode.NULL) != null) {
      PhoneNumber phone = new PhoneNumber();
      try {
        phone.setValue(userEditableInfo.getPhone(GetMode.NULL));
      } catch (PhoneNumberParseException e) {
        // shouldn't occur since this was ingested from Scim to begin with
        throw new RuntimeException(e);
      }
      phone.setPrimary(true);
      scimUser.setPhoneNumbers(ImmutableList.of(phone));
    }

    Url pictureLink = userEditableInfo.getPictureLink(GetMode.NULL);
    if (pictureLink != null && pictureLink.toString() != null) {
      Photo photo = new Photo();
      photo.setValue(pictureLink.toString());
      scimUser.setPhotos(ImmutableList.of(photo));
    }
    // Note: email is obtained from userInfo, so omitting here.
  }

  private void groupMembershipToScim(ScimUser scimUser, GroupMembership groupMembership) {
    if (groupMembership != null) {
      scimUser.setGroups(
          groupMembership.getGroups().stream().map(this::scimGroup).collect(Collectors.toList()));
    }
  }

  private UserGroup scimGroup(Urn urn) {
    String groupId = urnToId(urn);

    UserGroup userGroup = new UserGroup();
    userGroup.setValue(groupId);

    String location = location(GROUPS_ENDPOINT, groupId);
    userGroup.setRef(location);
    return userGroup;
  }

  private void roleMembershipToScim(ScimUser scimUser, RoleMembership roleMembership) {
    if (roleMembership != null) {
      scimUser.setRoles(
          roleMembership.getRoles().stream().map(this::scimRole).collect(Collectors.toList()));

      // set primary=true for the first role. See comments in ingestRoleMembership().
      if (!scimUser.getRoles().isEmpty()) {
        scimUser.getRoles().get(0).setPrimary(true);
      }
    }
  }

  private Role scimRole(Urn urn) {
    String roleName = null;
    try {
      DataHubRoleUrn roleUrn = DataHubRoleUrn.createFromUrn(urn);
      roleName = roleUrn.getRoleName();
    } catch (URISyntaxException e) {
      // likely indicates server-side bug
      throw new RuntimeException(e);
    }

    Role role = new Role();
    role.setValue(roleName);
    return role;
  }

  @Override
  Set<String> ignoreAspectsModificationTime() {
    return Collections.singleton(GROUP_MEMBERSHIP_ASPECT_NAME);
  }
}
