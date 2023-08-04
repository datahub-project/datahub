package io.datahub.test.action;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahub.test.definition.value.ActionType;
import java.util.List;


/**
 * An Action Applier is responsible for executing an action of a specific {@link ActionType}
 */
public interface Action {
  /**
   * Action being applied
   */
  ActionType getActionType();

  /**
   * Validate params for the given action.
   *
   * @param params Parameters for applying action
   * @throws InvalidOperandException if params are not sufficient to apply the action
   */
  void validate(ActionParameters params) throws InvalidActionParamsException;

  /**
   * Apply the actions to a given set of urns.
   */
  void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException;
}
