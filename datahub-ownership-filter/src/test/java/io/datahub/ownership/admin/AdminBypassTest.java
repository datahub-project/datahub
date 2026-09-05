package io.datahub.ownership.admin;

import com.linkedin.common.urn.Urn;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class AdminBypassTest {

    private final Urn datahubUser = urn("urn:li:corpuser:datahub");
    private final Urn alice = urn("urn:li:corpuser:alice");
    private final Urn admins = urn("urn:li:corpGroup:admins");
    private final Urn engineering = urn("urn:li:corpGroup:engineering");

    static Urn urn(String s) { try { return Urn.createFromString(s); } catch (Exception e) { throw new RuntimeException(e); } }

    @Test
    void recognizesBuiltInAdmin() {
        AdminBypass b = new AdminBypass(Set.of(datahubUser), Set.of(admins));
        assertThat(b.isAdmin(datahubUser, List.of())).isTrue();
    }

    @Test
    void recognizesAdminGroupMembership() {
        AdminBypass b = new AdminBypass(Set.of(datahubUser), Set.of(admins));
        assertThat(b.isAdmin(alice, List.of(admins, engineering))).isTrue();
    }

    @Test
    void rejectsRegularUser() {
        AdminBypass b = new AdminBypass(Set.of(datahubUser), Set.of(admins));
        assertThat(b.isAdmin(alice, List.of(engineering))).isFalse();
    }
}
