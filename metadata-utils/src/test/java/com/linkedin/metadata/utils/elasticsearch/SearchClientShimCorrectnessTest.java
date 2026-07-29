package com.linkedin.metadata.utils.elasticsearch;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.utils.arch.OperationContextArchTestUtil;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Architectural rule: every public method declared directly on {@link SearchClientShim} must have
 * {@link OperationFingerprint} as the first parameter, unless annotated with {@link
 * com.linkedin.metadata.utils.arch.OperationContextExempt}.
 *
 * <p>One test class per checked interface. Mirrors {@code AspectDaoCorrectnessTest} in {@code
 * metadata-io}.
 *
 * <p>If a method fails this check, you have two options:
 *
 * <ol>
 *   <li>Add {@code @Nonnull OperationFingerprint opContext} as the first parameter — required for
 *       any method that performs a tenant-routable network operation so that a wrapper layer (e.g.
 *       cloud's {@code EnrichingShim}) can decorate it uniformly.
 *   <li>Annotate the method on the interface with {@code @OperationContextExempt(reason="...")} —
 *       only for methods that are genuinely exempt: pure-local introspection (no I/O),
 *       cluster-metadata probes with no tenant/actor relevance, or infrastructure lifecycle setup.
 * </ol>
 */
public class SearchClientShimCorrectnessTest {

  @Test
  public void searchClientShimPublicMethodsMustHaveOperationFingerprintAsFirstParam() {
    OperationContextArchTestUtil.checkArch(
        SearchClientShim.class, Map.of(0, OperationFingerprint.class));
  }
}
