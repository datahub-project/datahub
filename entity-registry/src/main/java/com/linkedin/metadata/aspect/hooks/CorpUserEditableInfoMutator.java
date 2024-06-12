package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserEditableInfo;
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
public class CorpUserEditableInfoMutator extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  /*
    TODO: Ideally we are sending in patches that don't overwrite, this mutator should be removed once this logic is in
          place
  */
  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {

    List<Pair<ChangeMCP, Boolean>> results = new LinkedList<>();

    for (ChangeMCP item : changeMCPS) {
      if (changeTypeFilter(item) && aspectFilter(item)) {
        results.add(Pair.of(item, processEditableCorpUserInfoAspect(item)));
      } else {
        // no op
        results.add(Pair.of(item, false));
      }
    }

    return results.stream();
  }

  private static boolean changeTypeFilter(BatchItem item) {
    return !ChangeType.DELETE.equals(item.getChangeType())
        && !ChangeType.PATCH.equals(item.getChangeType());
  }

  private static boolean aspectFilter(ReadItem item) {
    return item.getAspectName().equals(CORP_USER_EDITABLE_INFO_ASPECT_NAME);
  }

  private static boolean processEditableCorpUserInfoAspect(ChangeMCP item) {
    boolean mutated = false;
    final CorpUserEditableInfo previous = item.getPreviousAspect(CorpUserEditableInfo.class);
    final CorpUserEditableInfo next = item.getAspect(CorpUserEditableInfo.class);
    if (next != null
        && next.getPersona() == null
        && previous != null
        && previous.getPersona() != null) {
      next.setPersona(previous.getPersona());
      mutated = true;
      log.error(
          "Invalid change detected for CorpUserEditableInfo for urn: {}, metadata: {}. "
              + "Persona should not be removed.",
          item.getUrn(),
          item.getSystemMetadata());
    }
    return mutated;
  }
}
