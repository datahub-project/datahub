package io.datahubproject.metadata.context.http;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Extension-point behavior for the outbound-HTTP enricher seam. Two guarantees matter and are the
 * reason this seam is safe to ship into OSS:
 *
 * <ol>
 *   <li>With no enrichers registered (the OSS build, and any fork build with tenancy disabled) the
 *       resolver contributes zero headers — the wired call sites add nothing, so behavior is
 *       byte-for-byte unchanged. This is the "no functional change" guardrail.
 *   <li>When enrichers are registered they are composed into a single header map, later overriding
 *       earlier on a name collision — the contract a registered enricher relies on.
 * </ol>
 */
public class HttpRequestContextResolverTest {

  private final OperationContext opContext = mock(OperationContext.class);

  @Test
  public void noEnrichers_resolvesToNoHeaders() {
    assertTrue(new HttpRequestContextResolver(List.of()).resolveHeaders(opContext).isEmpty());
  }

  @Test
  public void composesRegisteredEnrichers_laterOverridesEarlier() {
    HttpRequestContextEnricher first = ctx -> Map.of("X-A", "1", "X-Shared", "fromFirst");
    HttpRequestContextEnricher second = ctx -> Map.of("X-B", "2", "X-Shared", "fromSecond");

    Map<String, String> headers =
        new HttpRequestContextResolver(List.of(first, second)).resolveHeaders(opContext);

    assertEquals(headers.get("X-A"), "1");
    assertEquals(headers.get("X-B"), "2");
    assertEquals(headers.get("X-Shared"), "fromSecond");
  }
}
