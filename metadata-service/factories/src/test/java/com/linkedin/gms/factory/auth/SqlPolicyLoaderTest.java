package com.linkedin.gms.factory.auth;

import static com.linkedin.metadata.Constants.DATAHUB_POLICY_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.POLICY_ENTITY_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.PolicyFetcher;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class SqlPolicyLoaderTest {

  private static EntityAspect row(String urn, String metadata) {
    return EntityAspect.builder()
        .urn(urn)
        .aspect(DATAHUB_POLICY_INFO_ASPECT_NAME)
        .version(0)
        .metadata(metadata)
        .build();
  }

  @Test
  public void testLoadsEachPolicyWithItsUrnAndSkipsBadRows() {
    DataHubPolicyInfo info =
        new DataHubPolicyInfo()
            .setDisplayName("editors")
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray(List.of("EDIT_ENTITY")))
            .setEditable(true);

    AtomicBoolean closed = new AtomicBoolean(false);
    MetricUtils metrics = mock(MetricUtils.class);
    AspectDao aspectDao = mock(AspectDao.class);
    when(aspectDao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(
            Stream.of(
                    row("urn:li:dataHubPolicy:good", RecordUtils.toJsonString(info)),
                    row("urn:li:dataHubPolicy:bad", "{not json"))
                .onClose(() -> closed.set(true)));

    List<PolicyFetcher.Policy> loaded = new SqlPolicyLoader(aspectDao, metrics).loadAll();

    assertEquals(loaded.size(), 1);
    assertEquals(loaded.get(0).getUrn(), UrnUtils.getUrn("urn:li:dataHubPolicy:good"));
    assertEquals(loaded.get(0).getPolicyInfo(), info);
    assertTrue(closed.get(), "the SQL stream must be closed");
    verify(metrics).increment(SqlPolicyLoader.class, "policyCache.load.skipped", 1);
  }

  @Test
  public void testFailsWhenNoStoredPolicyCanBeRead() {
    AspectDao aspectDao = mock(AspectDao.class);
    when(aspectDao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(Stream.of(row("urn:li:dataHubPolicy:bad", "{not json")));

    // A failed load keeps the current cache; an empty result would replace it.
    assertThrows(IllegalStateException.class, () -> new SqlPolicyLoader(aspectDao, null).loadAll());
  }

  @Test
  public void testNoStoredPoliciesIsAnEmptyLoad() {
    AspectDao aspectDao = mock(AspectDao.class);
    when(aspectDao.streamAspects(POLICY_ENTITY_NAME, DATAHUB_POLICY_INFO_ASPECT_NAME))
        .thenReturn(Stream.empty());

    assertTrue(new SqlPolicyLoader(aspectDao, null).loadAll().isEmpty());
  }
}
