package oidc;

import auth.sso.oidc.OidcConfigs;
import java.util.List;

import static auth.sso.oidc.OidcCallbackLogic.getGroupsClaimNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
public class OidcCallbackLogicTest {

    @Test
    public void testGetGroupsClaimNamesJson1() {

        OidcConfigs mockConfigs = Mockito.mock(OidcConfigs.class);
        Mockito.when(mockConfigs.getGroupsClaimName()).thenReturn("[\"group1\"]");
        List<String> result = getGroupsClaimNames(mockConfigs);
        assertEquals(List.of("group1"), result);
    }
    @Test
    public void testGetGroupsClaimNamesJson2() {

        OidcConfigs mockConfigs = Mockito.mock(OidcConfigs.class);
        Mockito.when(mockConfigs.getGroupsClaimName()).thenReturn("[\"group1\",\"group2\"]");
        List<String> result = getGroupsClaimNames(mockConfigs);
        assertEquals(List.of("group1", "group2"), result);
    }
    @Test
    public void testGetGroupsClaimNamesCommaSeparated1() {

        OidcConfigs mockConfigs = Mockito.mock(OidcConfigs.class);
        Mockito.when(mockConfigs.getGroupsClaimName()).thenReturn("group1");
        List<String> result = getGroupsClaimNames(mockConfigs);
        assertEquals(List.of("group1"), result);
    }
    @Test
    public void testGetGroupsClaimNamesCommaSeparated2() {

        OidcConfigs mockConfigs = Mockito.mock(OidcConfigs.class);
        Mockito.when(mockConfigs.getGroupsClaimName()).thenReturn("group1, group2");
        List<String> result = getGroupsClaimNames(mockConfigs);
        assertEquals(List.of("group1", "group2"), result);
    }
}
