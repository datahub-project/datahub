package com.linkedin.metadata.test.action;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.definition.ActionType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is responsible for taking actions based on results of Metadata Tests.
 *
 * <p>It wraps a set of {@link Action} and invokes them accordingly when an action needs to be
 * applied.
 */
@Slf4j
public class ActionApplier {

  private final Map<ActionType, Action> actionAppliers;

  public ActionApplier(@Nonnull final List<Action> applierList) {
    this.actionAppliers =
        applierList.stream().collect(Collectors.toMap(Action::getActionType, applier -> applier));
  }

  /**
   * Apply an action of a particular {@link ActionType} to a set of entity URNs.
   *
   * @param type The type of the action. This will be mapped into the corresponding {@link Action}.
   * @param urns The set of entities that need to have the action applied.
   * @param params The parameters required to apply the action to the entities.
   */
  public void apply(
      @Nonnull OperationContext opContext,
      @Nonnull final ActionType type,
      @Nonnull final List<Urn> urns,
      @Nonnull final ActionParameters params) {
    if (actionAppliers.containsKey(type)) {
      try {
        actionAppliers.get(type).apply(opContext, urns, params);
        return;
      } catch (Exception e) {
        log.error(
            String.format(
                "Caught exception while attempting to apply action of type %s for entity urns %s. Continuing with processing...",
                type, urns),
            e);
      }
    }
    throw new IllegalArgumentException(
        String.format("Unrecognized action applier with type %s provided!", type));
  }
}
