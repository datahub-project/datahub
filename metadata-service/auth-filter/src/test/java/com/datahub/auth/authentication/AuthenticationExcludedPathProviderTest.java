package com.datahub.auth.authentication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticationExcludedPathProvider;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

/** Paths contributed in code by an {@link AuthenticationExcludedPathProvider}. */
public class AuthenticationExcludedPathProviderTest {

  private static AuthenticationEnforcementFilter filter(
      final List<AuthenticationExcludedPathProvider> providers) {
    final AuthenticationConfiguration authentication = new AuthenticationConfiguration();
    authentication.setEnabled(true);
    authentication.setExcludedPaths("/health");
    final ConfigurationProvider configurationProvider = new ConfigurationProvider();
    configurationProvider.setAuthentication(authentication);
    configurationProvider.setFeatureFlags(new FeatureFlags());

    final AuthenticationEnforcementFilter filter = new AuthenticationEnforcementFilter();
    ReflectionTestUtils.setField(filter, "configurationProvider", configurationProvider);
    // As Spring resolves them: the provider beans in the context, none when the feature is off.
    final StaticListableBeanFactory beans = new StaticListableBeanFactory();
    for (int i = 0; i < providers.size(); i++) {
      beans.addBean("provider" + i, providers.get(i));
    }
    ReflectionTestUtils.setField(
        filter,
        "excludedPathProviderBeans",
        beans.getBeanProvider(AuthenticationExcludedPathProvider.class));
    filter.init();
    return filter;
  }

  private static HttpServletRequest request(final String servletPath) {
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getServletPath()).thenReturn(servletPath);
    return request;
  }

  // Catches: a provider's path still enforced, so the component that authenticates it never runs.
  @Test
  public void providerPathIsExcludedAlongsideConfiguredOnes() {
    final AuthenticationEnforcementFilter filter =
        filter(List.of(() -> Set.of("/openapi/example/excluded")));

    assertTrue(filter.shouldNotFilter(request("/openapi/example/excluded")));
    assertTrue(filter.shouldNotFilter(request("/health")));
    assertFalse(filter.shouldNotFilter(request("/openapi/v2/entity/dataset")));
  }

  // Catches: a provider path read as a prefix or wildcard, opening paths its guard never checks.
  @Test
  public void providerPathsMatchExactlyOnly() {
    final AuthenticationEnforcementFilter filter =
        filter(List.of(() -> Set.of("/openapi/example/excluded", "/openapi/example/*")));

    assertFalse(filter.shouldNotFilter(request("/openapi/example/excluded/more")));
    assertFalse(filter.shouldNotFilter(request("/openapi/example/other")));
  }

  // Catches: provider paths copied once at startup, so a path a provider names only after the
  // filter is initialised stays enforced, or one it stops naming stays open.
  @Test
  public void providerPathsAreReadOnEveryRequest() {
    final AtomicReference<Set<String>> paths = new AtomicReference<>(Set.of());
    final AuthenticationEnforcementFilter filter = filter(List.of(paths::get));

    assertFalse(filter.shouldNotFilter(request("/openapi/example/excluded")));
    paths.set(Set.of("/openapi/example/excluded"));
    assertTrue(filter.shouldNotFilter(request("/openapi/example/excluded")));
    paths.set(Set.of());
    assertFalse(filter.shouldNotFilter(request("/openapi/example/excluded")));
  }

  // Catches: the path excluded when no provider is registered (the guard's feature is off).
  @Test
  public void withoutProvidersThePathIsEnforced() {
    assertFalse(filter(List.of()).shouldNotFilter(request("/openapi/example/excluded")));
  }
}
