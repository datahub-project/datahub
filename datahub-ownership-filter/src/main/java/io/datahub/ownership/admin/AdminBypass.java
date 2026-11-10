package io.datahub.ownership.admin;

import com.linkedin.common.urn.Urn;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

public class AdminBypass {

    private final Set<Urn> adminUserUrns;
    private final Set<Urn> adminGroupUrns;

    public AdminBypass(@Nonnull Set<Urn> adminUserUrns, @Nonnull Set<Urn> adminGroupUrns) {
        this.adminUserUrns = adminUserUrns;
        this.adminGroupUrns = adminGroupUrns;
    }

    public boolean isAdmin(@Nonnull Urn actor, @Nonnull Collection<Urn> actorGroups) {
        if (adminUserUrns.contains(actor)) return true;
        for (Urn g : actorGroups) {
            if (adminGroupUrns.contains(g)) return true;
        }
        return false;
    }
}
