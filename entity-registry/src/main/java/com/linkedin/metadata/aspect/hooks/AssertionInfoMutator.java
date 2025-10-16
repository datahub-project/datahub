package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class AssertionInfoMutator extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  /*
   * Write's the assertion's entity URN to the assertionInfo aspect. This allows
   * us to search assertions by the assertion's entity URN.
   */
  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {

    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();

    for (ChangeMCP item : changeMCPS) {
      if (changeTypeFilter(item) && aspectFilter(item)) {
        results.add(Pair.of(item, processAssertionInfoAspect(item)));
      } else {
        // no op
        results.add(Pair.of(item, false));
      }
    }

    return results.stream();
  }

  private static boolean changeTypeFilter(BatchItem item) {
    return !ChangeType.DELETE.equals(item.getChangeType());
  }

  private static boolean aspectFilter(ReadItem item) {
    return item.getAspectName().equals(ASSERTION_INFO_ASPECT_NAME);
  }

  private static boolean processAssertionInfoAspect(ChangeMCP item) {
    boolean mutated = false;
    final AssertionInfo next = item.getAspect(AssertionInfo.class);
    if (next != null && next.getEntity() == null) {
      next.setEntity(getEntityFromAssertionInfo(next));
      mutated = true;
    }
    return mutated;
  }

  private static Urn getEntityFromAssertionInfo(AssertionInfo assertionInfo) {
    switch (assertionInfo.getType()) {
      case DATASET:
        return assertionInfo.getDatasetAssertion().getDataset();
      case FRESHNESS:
        return assertionInfo.getFreshnessAssertion().getEntity();
      case VOLUME:
        return assertionInfo.getVolumeAssertion().getEntity();
      case SQL:
        return assertionInfo.getSqlAssertion().getEntity();
      case FIELD:
        return assertionInfo.getFieldAssertion().getEntity();
      case DATA_SCHEMA:
        return assertionInfo.getSchemaAssertion().getEntity();
      case CUSTOM:
        return assertionInfo.getCustomAssertion().getEntity();
      default:
        throw new RuntimeException("Unsupported Assertion Type " + assertionInfo.getType());
    }
  }
}
