package com.datahub.authorization;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
 * A configurable chain of {@link Authorizer}s executed in series to attempt to authenticate an
 * inbound request.
 *
 * <p>Individual {@link Authorizer}s are registered at the instance creation time. The chain can be
 * executed by invoking either {@link #authorize(AuthorizationRequest)} or {@link
 * #authorizeBatch(BatchAuthorizationRequest)}
 */
@Slf4j
public class AuthorizerChain implements Authorizer {

  private final List<Authorizer> authorizers;

  private final Authorizer defaultAuthorizer;

  public AuthorizerChain(final List<Authorizer> authorizers, Authorizer defaultAuthorizer) {
    this.authorizers = Objects.requireNonNull(authorizers);
    this.defaultAuthorizer = defaultAuthorizer;
  }

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfig, @Nonnull AuthorizerContext ctx) {
    // pass.
  }

  /**
   * Should never be invoked as it's superseded by {@link
   * #authorizeBatch(BatchAuthorizationRequest)}
   */
  @Nullable
  public AuthorizationResult authorize(@Nonnull final AuthorizationRequest request) {
    throw new UnsupportedOperationException(
        "This method should never be invoked with DataHub itself. Use authorizeBatch method");
  }

  /**
   * Executes a set of {@link Authorizer}s and returns the composition of the authentication
   * results.
   *
   * <p>Each {@link Authorizer}'s result per specific privilege is accessed:
   *
   * <ol>
   *   <li>in order of the {@link #authorizers} is defined
   *   <li>only if all previous {@link Authorizer}s denied that privilege
   * </ol>
   */
  @Nullable
  public BatchAuthorizationResult authorizeBatch(@Nonnull final BatchAuthorizationRequest request) {
    Objects.requireNonNull(request);
    // Save contextClassLoader
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

    var authorizersResults = new ArrayList<BatchAuthorizationResult>();

    for (final Authorizer authorizer : this.authorizers) {
      try {
        log.debug(
            "Executing authorizeBatch on Authorizer with class name {}",
            authorizer.getClass().getCanonicalName());
        log.debug("Batch Authorization Request: {}", request);
        // The library came with plugin can use the contextClassLoader to load the classes. For
        // example apache-ranger library does this.
        // Here we need to set our IsolatedClassLoader as contextClassLoader to resolve such class
        // loading request from plugin's home directory,
        // otherwise plugin's internal library wouldn't be able to find their dependent classes
        Thread.currentThread().setContextClassLoader(authorizer.getClass().getClassLoader());
        BatchAuthorizationResult result = authorizer.authorizeBatch(request);
        // reset
        Thread.currentThread().setContextClassLoader(contextClassLoader);

        log.debug("Batch authorization is successful");
        authorizersResults.add(result);
      } catch (Exception e) {
        log.error(
            "Caught exception while attempting to authorize request using Authorizer {}. Skipping authorizer.",
            authorizer.getClass().getCanonicalName(),
            e);
      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    }

    return new BatchAuthorizationResult(
        request, composeAuthorizersResults(request, authorizersResults));
  }

  private static LazyAuthorizationResultMap composeAuthorizersResults(
      BatchAuthorizationRequest request, ArrayList<BatchAuthorizationResult> authorizersResults) {
    return new LazyAuthorizationResultMap(
        privilege -> {
          for (BatchAuthorizationResult authorizerResult : authorizersResults) {
            var authorizationResult = authorizerResult.getResults().get(privilege);
            if (AuthorizationResult.Type.ALLOW == authorizationResult.getType()) {
              return authorizationResult;
            }

            log.debug(
                "Received DENY from Authorizer. message: {}", authorizationResult.getMessage());
          }

          return new AuthorizationResult(
              new AuthorizationRequest(
                  request.getActorUrn(),
                  privilege,
                  request.getResourceSpec(),
                  request.getSubResources()),
              AuthorizationResult.Type.DENY,
              "No Authorizer has approved the request");
        });
  }

  @Override
  public AuthorizedActors authorizedActors(String privilege, Optional<EntitySpec> resourceSpec) {
    if (this.authorizers.isEmpty()) {
      return null;
    }

    AuthorizedActors finalAuthorizedActors =
        this.authorizers.get(0).authorizedActors(privilege, resourceSpec);
    for (int i = 1; i < this.authorizers.size(); i++) {
      finalAuthorizedActors =
          mergeAuthorizedActors(
              finalAuthorizedActors,
              this.authorizers.get(i).authorizedActors(privilege, resourceSpec));
    }
    return finalAuthorizedActors;
  }

  private AuthorizedActors mergeAuthorizedActors(
      @Nullable AuthorizedActors original, @Nullable AuthorizedActors other) {
    if (original == null) {
      return other;
    }
    if (other == null) {
      return original;
    }

    boolean isAllUsers = original.isAllUsers() || other.isAllUsers();
    List<Urn> mergedUsers;
    if (isAllUsers) {
      // If enabled for all users, no need to check users
      mergedUsers = Collections.emptyList();
    } else {
      Set<Urn> users = new HashSet<>(original.getUsers());
      users.addAll(other.getUsers());
      mergedUsers = new ArrayList<>(users);
    }

    boolean isAllGroups = original.isAllGroups() || other.isAllGroups();
    List<Urn> mergedGroups;
    if (isAllGroups) {
      // If enabled for all users, no need to check users
      mergedGroups = Collections.emptyList();
    } else {
      Set<Urn> groups = new HashSet<>(original.getGroups());
      groups.addAll(other.getGroups());
      mergedGroups = new ArrayList<>(groups);
    }

    Set<Urn> roles = new HashSet<>(original.getRoles());
    roles.addAll(other.getRoles());
    List<Urn> mergedRoles = new ArrayList<>(roles);

    return AuthorizedActors.builder()
        .allUsers(original.isAllUsers() || other.isAllUsers())
        .allGroups(original.isAllGroups() || other.isAllGroups())
        .users(mergedUsers)
        .groups(mergedGroups)
        .roles(mergedRoles)
        .build();
  }

  /** Returns an instance of default {@link DataHubAuthorizer} */
  public DataHubAuthorizer getDefaultAuthorizer() {
    return (DataHubAuthorizer) defaultAuthorizer;
  }

  @Override
  public Set<DataHubPolicyInfo> getActorPolicies(@Nonnull Urn actorUrn) {
    return authorizers.stream()
        .flatMap(authorizer -> authorizer.getActorPolicies(actorUrn).stream())
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<Urn> getActorGroups(@Nonnull Urn actorUrn) {
    return authorizers.stream()
        .flatMap(authorizer -> authorizer.getActorGroups(actorUrn).stream())
        .collect(Collectors.toList());
  }

  @Override
  public Collection<Urn> getActorPeers(@Nonnull Urn actorUrn) {
    return authorizers.stream()
        .flatMap(authorizer -> authorizer.getActorPeers(actorUrn).stream())
        .distinct()
        .collect(Collectors.toList());
  }
}
