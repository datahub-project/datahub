package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * A configurable chain of {@link Authorizer}s executed in series to attempt to authenticate an inbound request.
 *
 * Individual {@link Authorizer}s are registered with the chain using {@link #register(Authorizer)}.
 * The chain can be executed by invoking {@link #authorize(AuthorizationRequest)}.
 */
@Slf4j
public class AuthorizerChain implements Authorizer {

  private final List<Authorizer> authorizers;

  public AuthorizerChain(final List<Authorizer> authorizers) {
    this.authorizers = Objects.requireNonNull(authorizers);
  }

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfig) {
    // pass.
  }

  /**
   * Executes a set of {@link Authorizer}s and returns the first successful authentication result.
   *
   * Returns an instance of {@link AuthorizationResult}.
   */
  @Nullable
  public AuthorizationResult authorize(@Nonnull final AuthorizationRequest request) {
    Objects.requireNonNull(request);
    for (final Authorizer authorizer : this.authorizers) {
      try {
        log.debug("Executing Authorizer with class name {}", authorizer.getClass().getCanonicalName());
        AuthorizationResult result = authorizer.authorize(request);
        if (AuthorizationResult.Type.ALLOW.equals(result.type)) {
          // Authorization was successful - Short circuit
          return result;
        } else {
          log.debug("Received DENY result from Authorizer with class name {}. message: {}",
              authorizer.getClass().getCanonicalName(), result.getMessage());
        }
      } catch (Exception e) {
        log.error("Caught exception while attempting to authorize request using Authorizer {}. Skipping authorizer.",
            authorizer.getClass().getCanonicalName(), e);
      }
    }
    // Return failed Authorization result.
    return new AuthorizationResult(request, AuthorizationResult.Type.DENY, null);
  }

  @Override
  public AuthorizedActors authorizedActors(String privilege, Optional<ResourceSpec> resourceSpec) {
    if (this.authorizers.isEmpty()) {
      return null;
    }

    AuthorizedActors finalAuthorizedActors = this.authorizers.get(0).authorizedActors(privilege, resourceSpec);
    for (int i = 1; i < this.authorizers.size(); i++) {
      finalAuthorizedActors = mergeAuthorizedActors(finalAuthorizedActors,
          this.authorizers.get(i).authorizedActors(privilege, resourceSpec));
    }
    return finalAuthorizedActors;
  }

  private AuthorizedActors mergeAuthorizedActors(@Nullable AuthorizedActors original,
      @Nullable AuthorizedActors other) {
    if (original == null) {
      return other;
    }
    if (other == null) {
      return original;
    }

    List<Urn> mergedUsers;
    if (original.isAllUsers()) {
      // If original enabled for all users, take the other's users
      mergedUsers = other.getUsers();
    } else if (other.isAllUsers()) {
      mergedUsers = original.getUsers();
    } else {
      Set<Urn> otherUsers = new HashSet<>(other.getUsers());
      mergedUsers = original.getUsers().stream().filter(otherUsers::contains).collect(Collectors.toList());
    }

    List<Urn> mergedGroups;
    if (original.isAllGroups()) {
      // If original enabled for all users, take the other's users
      mergedGroups = other.getGroups();
    } else if (other.isAllUsers()) {
      mergedGroups = original.getGroups();
    } else {
      Set<Urn> otherGroups = new HashSet<>(other.getGroups());
      mergedGroups = original.getGroups().stream().filter(otherGroups::contains).collect(Collectors.toList());
    }

    return AuthorizedActors.builder()
        .allUsers(original.isAllUsers() || other.isAllUsers())
        .allGroups(original.isAllGroups() || other.isAllGroups())
        .users(mergedUsers)
        .groups(mergedGroups)
        .build();
  }

  /**
   * Returns an instance of {@link DataHubAuthorizer} if it is present in the Authentication chain,
   * or null if it cannot be found.
   */
  public DataHubAuthorizer getDefaultAuthorizer() {
    return (DataHubAuthorizer) this.authorizers.stream()
        .filter(authorizer -> authorizer instanceof DataHubAuthorizer)
        .findFirst()
        .orElse(null);
  }
}