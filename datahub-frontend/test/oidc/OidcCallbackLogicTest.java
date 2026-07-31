package oidc;

import static auth.sso.oidc.OidcCallbackLogic.checkRequiredGroups;
import static auth.sso.oidc.OidcCallbackLogic.getGroupNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import auth.sso.oidc.OidcConfigs;
import auth.sso.oidc.RequiredGroupsException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pac4j.core.profile.CommonProfile;

public class OidcCallbackLogicTest {

  @Test
  public void testCheckRequiredGroups_UserHasRequiredGroup() {
    CommonProfile profile =
        createMockProfileWithAttribute("[\"group1\", \"group2\"]", "groupsClaimName");
    when(profile.containsAttribute("groupsClaimName")).thenReturn(true);
    String userName = "testuser";

    OidcConfigs oidcConfigs = mock(OidcConfigs.class);
    Set<String> requiredGroups = new HashSet<>(Arrays.asList("group1", "group3"));
    when(oidcConfigs.getRequiredGroups()).thenReturn(requiredGroups);
    when(oidcConfigs.getGroupsClaimName()).thenReturn("groupsClaimName");

    org.junit.jupiter.api.Assertions.assertDoesNotThrow(
        () -> checkRequiredGroups(profile, userName, oidcConfigs));
  }

  @Test
  public void testCheckRequiredGroups_UserHasNoRequiredGroup_Throws() {
    CommonProfile profile =
        createMockProfileWithAttribute("[\"group3\", \"group4\"]", "groupsClaimName");
    when(profile.containsAttribute("groupsClaimName")).thenReturn(true);
    String userName = "testuser";
    OidcConfigs oidcConfigs = mock(OidcConfigs.class);
    Set<String> requiredGroups = new HashSet<>(Arrays.asList("group1", "group2"));
    when(oidcConfigs.getRequiredGroups()).thenReturn(requiredGroups);
    when(oidcConfigs.getGroupsClaimName()).thenReturn("groupsClaimName");

    org.junit.jupiter.api.Assertions.assertThrows(
        RequiredGroupsException.class, () -> checkRequiredGroups(profile, userName, oidcConfigs));
  }

  @Test
  public void testGetGroupsClaimNamesJsonArray() {
    CommonProfile profile =
        createMockProfileWithAttribute("[\"group1\", \"group2\"]", "groupsClaimName");
    Collection<String> result =
        getGroupNames(profile, "[\"group1\", \"group2\"]", "groupsClaimName");
    assertEquals(Arrays.asList("group1", "group2"), result);
  }

  @Test
  public void testGetGroupNamesWithSingleGroup() {
    CommonProfile profile = createMockProfileWithAttribute("group1", "groupsClaimName");
    Collection<String> result = getGroupNames(profile, "group1", "groupsClaimName");
    assertEquals(Arrays.asList("group1"), result);
  }

  @Test
  public void testGetGroupNamesWithCommaSeparated() {
    CommonProfile profile = createMockProfileWithAttribute("group1,group2", "groupsClaimName");
    Collection<String> result = getGroupNames(profile, "group1,group2", "groupsClaimName");
    assertEquals(Arrays.asList("group1", "group2"), result);
  }

  @Test
  public void testGetGroupNamesWithCollection() {
    CommonProfile profile =
        createMockProfileWithAttribute(Arrays.asList("group1", "group2"), "groupsClaimName");
    Collection<String> result =
        getGroupNames(profile, Arrays.asList("group1", "group2"), "groupsClaimName");
    assertEquals(Arrays.asList("group1", "group2"), result);
  }

  // Helper method to create a mock CommonProfile with given attribute
  private CommonProfile createMockProfileWithAttribute(Object attribute, String attributeName) {
    CommonProfile profile = mock(CommonProfile.class);

    // Mock for getAttribute(String)
    when(profile.getAttribute(attributeName)).thenReturn(attribute);

    // Mock for getAttribute(String, Class<T>)
    if (attribute instanceof Collection<?>) {
      when(profile.getAttribute(attributeName, Collection.class))
          .thenReturn((Collection<?>) attribute);
    } else if (attribute instanceof String) {
      when(profile.getAttribute(attributeName, String.class)).thenReturn((String) attribute);
    }
    // Add more conditions here if needed for other types

    return profile;
  }
}
