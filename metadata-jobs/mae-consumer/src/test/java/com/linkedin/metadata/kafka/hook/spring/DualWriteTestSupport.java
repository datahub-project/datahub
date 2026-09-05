package com.linkedin.metadata.kafka.hook.spring;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Shared wiring for the ZDU rollback dual-write Spring tests.
 *
 * <p>The client mock is pre-stubbed at bean-definition time rather than from inside a test method
 * because the strategy resolves its targets while the bean is being constructed — stubbing later
 * would be too late, and the strategy would come up with nothing to dual-write to.
 */
public final class DualWriteTestSupport {

  /** Physical index that completed Phase 1, i.e. the one dual-write must keep current. */
  public static final String OLD_BACKING_INDEX = "datasetindex_v2_old_456";

  public static final String DATASET_INDEX = "datasetindex_v2";

  private DualWriteTestSupport() {}

  /** Phase 1 state with a recorded old backing index, which is what makes a target eligible. */
  public static EntityResponse completedPhase1Response() {
    final Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            DATASET_INDEX,
            "datasetindex_v2_next_123",
            OLD_BACKING_INDEX,
            100L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);

    final DataHubUpgradeResult result =
        new DataHubUpgradeResult()
            .setState(DataHubUpgradeState.SUCCEEDED)
            .setResult(new StringMap(state));

    final EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(result.data()));
    aspect.setSystemMetadata(new SystemMetadata().setVersion("1"));

    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, aspect);

    final EntityResponse response = new EntityResponse();
    response.setAspects(aspects);
    return response;
  }

  /**
   * Overrides the plain {@code SystemEntityClient} mock from {@link
   * MCLSpringCommonTestConfiguration} with one that actually returns Phase 1 state, and supplies
   * the {@code GitVersion} the strategy factory needs to build its upgrade URN (its factory lives
   * in {@code com.linkedin.gms.factory.common}, which the MCL test configs do not scan).
   */
  @TestConfiguration
  public static class Phase1StateConfiguration {

    @Bean(name = "systemEntityClient")
    @Primary
    public SystemEntityClient systemEntityClient() throws Exception {
      final SystemEntityClient client = mock(SystemEntityClient.class);
      when(client.batchGetV2NoCache(any(), any(), any(Set.class), any(Set.class)))
          .thenAnswer(
              invocation -> {
                final Set<Urn> urns = invocation.getArgument(2);
                return urns.stream()
                    .collect(
                        java.util.stream.Collectors.toMap(
                            urn -> urn, urn -> completedPhase1Response()));
              });
      return client;
    }

    @Bean
    public GitVersion gitVersion() {
      return new GitVersion("0.0.0-test", "testCommit", Optional.empty());
    }
  }
}
