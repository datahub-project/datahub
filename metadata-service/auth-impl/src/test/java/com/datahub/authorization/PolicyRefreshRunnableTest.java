package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class PolicyRefreshRunnableTest {

  private static final OperationContext OP =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private static PolicyFetcher.Policy policy(String id) {
    return new PolicyFetcher.Policy(
        UrnUtils.getUrn("urn:li:dataHubPolicy:" + id),
        new DataHubPolicyInfo()
            .setDisplayName(id)
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray(List.of("EDIT_ENTITY"))));
  }

  @Test
  public void testAllPagesArePublishedAsOneSnapshot() throws Exception {
    PolicyFetcher fetcher = mock(PolicyFetcher.class);
    when(fetcher.fetchPolicies(eq(OP), eq(1), isNull(), any()))
        .thenReturn(new PolicyFetcher.PolicyFetchResult(List.of(policy("a")), 2, "page2"));
    when(fetcher.fetchPolicies(eq(OP), eq(1), eq("page2"), any()))
        .thenReturn(new PolicyFetcher.PolicyFetchResult(List.of(policy("b")), 2, null));
    List<PolicySnapshot> published = new ArrayList<>();

    new DataHubAuthorizer.PolicyRefreshRunnable(OP, fetcher, published::add, 1).run();

    assertEquals(published.size(), 1);
    assertEquals(published.get(0).size(), 2);
  }

  @Test
  public void testFailedPageKeepsTheCurrentPolicies() throws Exception {
    PolicyFetcher fetcher = mock(PolicyFetcher.class);
    when(fetcher.fetchPolicies(eq(OP), eq(1), isNull(), any()))
        .thenReturn(new PolicyFetcher.PolicyFetchResult(List.of(policy("a")), 2, "page2"));
    when(fetcher.fetchPolicies(eq(OP), eq(1), eq("page2"), any()))
        .thenThrow(new IllegalStateException("search unavailable"));
    List<PolicySnapshot> published = new ArrayList<>();

    new DataHubAuthorizer.PolicyRefreshRunnable(OP, fetcher, published::add, 1).run();

    assertTrue(published.isEmpty(), "a partial result must not replace the cache");
  }
}
