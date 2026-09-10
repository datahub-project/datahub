package io.datahubproject.metadata.context;

import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ServicesRegistryContext implements ContextInterface {

  @Nonnull private final RestrictedService restrictedService;

  @Nullable private final ActorGroupMembershipService actorGroupMembershipService;

  @Nonnull
  public SessionActorIdentity fetchUserIdentity(
      @Nonnull OperationContext opContext, @Nonnull Urn userUrn) {
    if (actorGroupMembershipService == null) {
      return SessionActorIdentity.empty(userUrn);
    }
    return actorGroupMembershipService.fetchUserIdentity(opContext, userUrn);
  }

  @Nonnull
  public List<Urn> getGroupsForUser(@Nonnull OperationContext opContext, @Nonnull Urn userUrn) {
    if (actorGroupMembershipService == null) {
      return Collections.emptyList();
    }
    return actorGroupMembershipService.getGroupsForUser(opContext, userUrn);
  }

  @Nonnull
  public Set<Urn> fetchRolesViaGroups(
      @Nonnull OperationContext opContext, @Nonnull Collection<Urn> groupUrns) {
    if (actorGroupMembershipService == null) {
      return Collections.emptySet();
    }
    return actorGroupMembershipService.fetchRolesViaGroups(opContext, groupUrns);
  }

  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
