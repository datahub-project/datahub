package com.linkedin.metadata.test.action;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** An Action Applier is responsible for executing an action of a specific {@link ActionType} */
public interface Action {

  Urn METADATA_TEST_ACTOR_URN = EntityUtils.getUrnFromString(Constants.METATDATA_TEST_ACTOR);

  /** Action being applied */
  ActionType getActionType();

  /**
   * Validate params for the given action.
   *
   * @param params Parameters for applying action
   * @throws InvalidOperandException if params are not sufficient to apply the action
   */
  void validate(ActionParameters params) throws InvalidActionParamsException;

  /** Apply the actions to a given set of urns. */
  void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException;

  default List<ResourceReference> getResourceReferences(List<Urn> urns) {
    return urns.stream()
        .map(urn -> new ResourceReference(urn, null, null))
        .collect(Collectors.toList());
  }
}
