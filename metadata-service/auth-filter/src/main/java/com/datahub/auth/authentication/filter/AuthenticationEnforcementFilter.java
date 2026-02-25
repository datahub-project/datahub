package com.datahub.auth.authentication.filter;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Phase 2 Authentication Filter - Authentication Enforcement
 *
 * <p>This filter enforces authentication requirements based on configuration. It relies on the
 * AuthenticationContext already set by AuthenticationExtractionFilter.
 *
 * <p>Behavior: - For excluded paths (like /health, /config): Always allows access - For protected
 * paths: Requires valid authentication, returns 401 if anonymous
 *
 * <p>This filter is applied to the GraphQL Servlet, the Rest.li Servlet, and the Auth (token)
 * Servlet. It works in conjunction with AuthenticationExtractionFilter which handles authentication
 * extraction.
 */
@Component
@Slf4j
public class AuthenticationEnforcementFilter extends OncePerRequestFilter {
  @Autowired private ConfigurationProvider configurationProvider;

  private Set<String> excludedPathPatterns;

  @PostConstruct
  public void init() {
    initializeExcludedPaths();
    log.info("AuthenticationEnforcementFilter initialized - enforcement only mode.");
  }

  private void initializeExcludedPaths() {
    excludedPathPatterns = new HashSet<>();
    String excludedPaths = configurationProvider.getAuthentication().getExcludedPaths();
    if (StringUtils.hasText(excludedPaths)) {
      excludedPathPatterns.addAll(
          Arrays.stream(excludedPaths.split(","))
              .map(String::trim)
              .filter(path -> !path.isBlank())
              .toList());
    }
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {

    try {
      // Get authentication context set by AuthenticationExtractionFilter
      Authentication authentication = AuthenticationContext.getAuthentication();

      // Check if this path requires authentication
      if (!shouldNotFilter(request)) {
        // This path requires authentication
        if (authentication == null || isAnonymousUser(authentication)) {
          log.debug(
              "Authentication required for path: {}, but user is not authenticated",
              request.getServletPath());
          response.sendError(
              HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized to perform this action.");
          return;
        }

        log.debug(
            "Authentication check passed for Actor with type: {}, id: {}",
            authentication.getActor().getType(),
            authentication.getActor().getId());
      } else {
        // Path is excluded from authentication requirements
        log.debug(
            "Skipping authentication enforcement for excluded path: {}", request.getServletPath());
      }

      // Proceed with the request
      chain.doFilter(request, response);

    } finally {
      // Always clean up authentication context after request processing
      // Note: AuthenticationExtractionFilter also cleans up, but this ensures cleanup
      // in case there are any edge cases or ordering issues
      AuthenticationContext.remove();
    }
  }

  /**
   * Checks if the given authentication represents an anonymous/unauthenticated user.
   *
   * @param authentication the authentication to check
   * @return true if the user is anonymous/unauthenticated
   */
  private boolean isAnonymousUser(Authentication authentication) {
    if (authentication == null) {
      return true;
    }

    // Check if this is the anonymous user set by AuthenticationExtractionFilter
    return ANONYMOUS_ACTOR_ID.equals(authentication.getActor().getId());
  }

  @VisibleForTesting
  @Override
  public boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getServletPath();
    if (path == null) {
      return false;
    }

    // Check if the path matches any of the excluded patterns
    boolean shouldExclude =
        excludedPathPatterns.stream()
            .anyMatch(
                pattern -> {
                  if (pattern.endsWith("/*")) {
                    // Handle wildcard patterns
                    String basePattern = pattern.substring(0, pattern.length() - 2);
                    return path.startsWith(basePattern);
                  }
                  return path.equals(pattern);
                });

    if (shouldExclude) {
      log.debug("Skipping authentication for excluded path: {}", path);
    }

    return shouldExclude;
  }

  @Override
  public void destroy() {
    // Nothing
  }
}
