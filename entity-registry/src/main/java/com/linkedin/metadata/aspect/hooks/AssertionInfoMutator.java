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
import com.linkedin.metadata.aspect.utils.AssertionUtils;
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
    return ASSERTION_INFO_ASPECT_NAME.equals(item.getAspectName());
  }

  private static boolean processAssertionInfoAspect(ChangeMCP item) {
    boolean mutated = false;
    final AssertionInfo next = item.getAspect(AssertionInfo.class);
    if (next != null && !next.hasEntityUrn()) {
      final Urn entityUrn = AssertionUtils.getEntityFromAssertionInfo(next);
      if (entityUrn != null) {
        next.setEntityUrn(entityUrn);
        mutated = true;
      }
    }
    return mutated;
  }
}
