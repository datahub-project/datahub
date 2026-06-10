package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitLease;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.filter.OncePerRequestFilter;

@Slf4j
@RequiredArgsConstructor
public class RateLimitFilter extends OncePerRequestFilter {

  private final RateLimitEngine engine;

  @Override
  protected void doFilterInternal(
      @Nonnull HttpServletRequest request,
      @Nonnull HttpServletResponse response,
      @Nonnull FilterChain filterChain)
      throws ServletException, IOException {
    if (!engine.isEnabled() || engine.isExcluded(request.getRequestURI())) {
      filterChain.doFilter(request, response);
      return;
    }

    String method = request.getMethod();
    if (engine.isGraphQLPost(request.getRequestURI(), method)) {
      filterChain.doFilter(request, response);
      return;
    }

    RateLimitDecision decision = engine.evaluateAndAcquireRest(request.getRequestURI(), method);
    if (!decision.isAllowed()) {
      engine.writeDeniedResponse(response, decision);
      return;
    }

    RateLimitLease lease = engine.toLease(decision);
    engine.applyHeaders(response, decision);
    AtomicBoolean released = new AtomicBoolean(false);
    boolean success = false;
    boolean deferRelease = false;
    try {
      filterChain.doFilter(request, response);
      deferRelease = request.isAsyncStarted();
      if (deferRelease) {
        request
            .getAsyncContext()
            .addListener(new AsyncRateLimitReleaseListener(engine, lease, released));
      } else {
        success = response.getStatus() < 500;
      }
    } finally {
      if (!deferRelease) {
        releaseOnce(engine, released, lease, success);
      }
    }
  }

  private static void releaseOnce(
      RateLimitEngine engine, AtomicBoolean released, RateLimitLease lease, boolean success) {
    if (released.compareAndSet(false, true)) {
      engine.release(lease, success);
    }
  }

  /** Releases a lease exactly once when async MVC processing completes. */
  static final class AsyncRateLimitReleaseListener implements AsyncListener {
    private final RateLimitEngine engine;
    private final RateLimitLease lease;
    private final AtomicBoolean released;

    AsyncRateLimitReleaseListener(
        RateLimitEngine engine, RateLimitLease lease, AtomicBoolean released) {
      this.engine = engine;
      this.lease = lease;
      this.released = released;
    }

    @Override
    public void onComplete(AsyncEvent event) {
      HttpServletResponse response = (HttpServletResponse) event.getSuppliedResponse();
      releaseOnce(response != null && response.getStatus() < 500);
    }

    @Override
    public void onTimeout(AsyncEvent event) {
      releaseOnce(false);
    }

    @Override
    public void onError(AsyncEvent event) {
      releaseOnce(false);
    }

    @Override
    public void onStartAsync(AsyncEvent event) {
      // no-op
    }

    private void releaseOnce(boolean success) {
      RateLimitFilter.releaseOnce(engine, released, lease, success);
    }
  }
}
