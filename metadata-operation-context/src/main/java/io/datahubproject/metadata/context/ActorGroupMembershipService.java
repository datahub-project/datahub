package io.datahubproject.metadata.context;

import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.urn.Urn;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/** Canonical read API for corp user group membership and role inheritance. */
public interface ActorGroupMembershipService {

  @Nonnull
  SessionActorIdentity fetchUserIdentity(@Nonnull OperationContext opContext, @Nonnull Urn userUrn);

  @Nonnull
  List<Urn> getGroupsForUser(@Nonnull OperationContext opContext, @Nonnull Urn userUrn);

  @Nonnull
  Set<Urn> fetchRolesViaGroups(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> groupUrns);
}
