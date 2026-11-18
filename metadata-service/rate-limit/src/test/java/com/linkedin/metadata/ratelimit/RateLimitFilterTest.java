package com.linkedin.metadata.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitLease;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RateLimitFilterTest {

  private static final String ACTOR = "urn:li:corpuser:tester";
  private static final String EXCLUDED_IN_CODE = "/openapi/example/excluded";

  private RateLimitEngine engine;
  private RateLimitFilter filter;
  private HttpServletRequest request;
  private HttpServletResponse response;

  @BeforeMethod
  public void setup() {
    engine = mock(RateLimitEngine.class);
    filter = new RateLimitFilter(engine, () -> ACTOR, Set::of);
    request = mock(HttpServletRequest.class);
    response = mock(HttpServletResponse.class);
    when(request.getMethod()).thenReturn("POST");
    when(request.getRequestURI()).thenReturn("/auth/signUp");
  }

  // Catches: a path a provider excluded still drawing from the rate-limit buckets (for a caller
  // with no DataHub identity, the one anonymous bucket), or a sub-path of it being excluded too.
  @Test
  public void providerPathIsNotRateLimitedAndMatchesExactly() throws Exception {
    filter = new RateLimitFilter(engine, () -> ACTOR, () -> Set.of(EXCLUDED_IN_CODE));
    when(engine.isEnabled()).thenReturn(true);
    when(request.getServletPath()).thenReturn(EXCLUDED_IN_CODE);
    when(request.getRequestURI()).thenReturn(EXCLUDED_IN_CODE);

    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine, never()).evaluateAndAcquireRest(any(), any(), any(), any());

    stubAllowedAcquire();
    when(request.getServletPath()).thenReturn(EXCLUDED_IN_CODE + "/more");
    when(request.getRequestURI()).thenReturn(EXCLUDED_IN_CODE + "/more");
    when(request.isAsyncStarted()).thenReturn(false);
    when(response.getStatus()).thenReturn(HttpServletResponse.SC_OK);

    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine).evaluateAndAcquireRest(any(), any(), any(), any());
  }

  // Catches: the provider paths copied once when the filter is built, so a path a provider names
  // only after startup keeps drawing from the rate-limit buckets.
  @Test
  public void providerPathNamedAfterTheFilterIsBuiltIsHonoured() throws Exception {
    final AtomicReference<Set<String>> excluded = new AtomicReference<>(Set.of());
    filter = new RateLimitFilter(engine, () -> ACTOR, excluded::get);
    stubAllowedAcquire();
    when(request.getServletPath()).thenReturn(EXCLUDED_IN_CODE);
    when(request.getRequestURI()).thenReturn(EXCLUDED_IN_CODE);
    when(request.isAsyncStarted()).thenReturn(false);
    when(response.getStatus()).thenReturn(HttpServletResponse.SC_OK);

    filter.doFilter(request, response, (req, resp) -> {});
    excluded.set(Set.of(EXCLUDED_IN_CODE));
    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine, Mockito.times(1)).evaluateAndAcquireRest(any(), any(), any(), any());
  }

  @Test
  public void testSyncRequestReleasesImmediately() throws Exception {
    stubAllowedAcquire();
    when(request.isAsyncStarted()).thenReturn(false);
    when(response.getStatus()).thenReturn(HttpServletResponse.SC_OK);

    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine).release(any(RateLimitLease.class), eq(true));
  }

  @Test
  public void testAsyncRequestDefersReleaseUntilComplete() throws Exception {
    stubAllowedAcquire();
    AsyncContext asyncContext = mock(AsyncContext.class);
    AtomicReference<AsyncListener> listener = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listener.set(invocation.getArgument(0));
              return null;
            })
        .when(asyncContext)
        .addListener(any(AsyncListener.class));
    when(request.isAsyncStarted()).thenReturn(true);
    when(request.getAsyncContext()).thenReturn(asyncContext);
    when(response.getStatus()).thenReturn(HttpServletResponse.SC_OK);

    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine, never()).release(any(), anyBoolean());

    listener.get().onComplete(new AsyncEvent(asyncContext, request, response));

    verify(engine).release(any(RateLimitLease.class), eq(true));
  }

  @Test
  public void testAsyncTimeoutReleasesAsFailure() throws Exception {
    stubAllowedAcquire();
    AsyncContext asyncContext = mock(AsyncContext.class);
    AtomicReference<AsyncListener> listener = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listener.set(invocation.getArgument(0));
              return null;
            })
        .when(asyncContext)
        .addListener(any(AsyncListener.class));
    when(request.isAsyncStarted()).thenReturn(true);
    when(request.getAsyncContext()).thenReturn(asyncContext);

    filter.doFilter(request, response, (req, resp) -> {});

    listener.get().onTimeout(new AsyncEvent(asyncContext, request, response));

    verify(engine).release(any(RateLimitLease.class), eq(false));
  }

  @Test
  public void testAsyncErrorReleasesAsFailure() throws Exception {
    stubAllowedAcquire();
    AsyncContext asyncContext = mock(AsyncContext.class);
    AtomicReference<AsyncListener> listener = new AtomicReference<>();
    doAnswer(
            invocation -> {
              listener.set(invocation.getArgument(0));
              return null;
            })
        .when(asyncContext)
        .addListener(any(AsyncListener.class));
    when(request.isAsyncStarted()).thenReturn(true);
    when(request.getAsyncContext()).thenReturn(asyncContext);

    filter.doFilter(request, response, (req, resp) -> {});

    listener.get().onError(new AsyncEvent(asyncContext, request, response));

    verify(engine).release(any(RateLimitLease.class), eq(false));
  }

  @Test(expectedExceptions = ServletException.class)
  public void testServletExceptionReleasesLease() throws Exception {
    stubAllowedAcquire();
    when(request.isAsyncStarted()).thenReturn(false);

    filter.doFilter(
        request,
        response,
        (req, resp) -> {
          throw new ServletException("downstream failure");
        });

    verify(engine).release(any(RateLimitLease.class), eq(false));
  }

  @Test(expectedExceptions = IOException.class)
  public void testIOExceptionReleasesLease() throws Exception {
    stubAllowedAcquire();
    when(request.isAsyncStarted()).thenReturn(false);

    filter.doFilter(
        request,
        response,
        (req, resp) -> {
          throw new IOException("downstream failure");
        });

    verify(engine).release(any(RateLimitLease.class), eq(false));
  }

  @Test
  public void testGraphQLPostSkipsFilter() throws Exception {
    when(engine.isEnabled()).thenReturn(true);
    when(engine.isExcluded(any())).thenReturn(false);
    when(engine.isGraphQLPost("/api/graphql", "POST")).thenReturn(true);
    when(request.getRequestURI()).thenReturn("/api/graphql");

    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine, never()).evaluateAndAcquireRest(any(), any(), any(), any());
    verify(engine, never()).release(any(), anyBoolean());
  }

  @Test
  public void testPassesResolvedActorUrnToEngine() throws Exception {
    // The filter must forward the supplied actor URN so the scoped per-actor bucket applies to REST
    // (previously it always passed null, so REST skipped per-actor limiting).
    stubAllowedAcquire();
    when(request.isAsyncStarted()).thenReturn(false);
    when(response.getStatus()).thenReturn(HttpServletResponse.SC_OK);

    filter.doFilter(request, response, (req, resp) -> {});

    verify(engine).evaluateAndAcquireRest(eq("/auth/signUp"), eq("POST"), any(), eq(ACTOR));
  }

  private void stubAllowedAcquire() throws Exception {
    when(engine.isEnabled()).thenReturn(true);
    when(engine.isExcluded(any())).thenReturn(false);
    when(engine.isGraphQLPost(any(), any())).thenReturn(false);
    RateLimitDecision decision = RateLimitDecision.builder().allowed(true).build();
    when(engine.evaluateAndAcquireRest(any(), any(), any(), any())).thenReturn(decision);
    RateLimitLease lease = new RateLimitLease(null, "_default_capacity", null, 0L);
    when(engine.toLease(decision)).thenReturn(lease);
    Mockito.doNothing().when(engine).applyHeaders(any(), any());
  }
}
