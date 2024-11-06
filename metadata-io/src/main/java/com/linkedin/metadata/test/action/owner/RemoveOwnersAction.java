package com.linkedin.metadata.test.action.owner;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.OwnerServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.ValuesAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RemoveOwnersAction extends ValuesAction {

  private final OwnerServiceAsync ownerService;

  @Override
  public ActionType getActionType() {
    return ActionType.REMOVE_OWNERS;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // For each entity type, group then apply the action.
    List<String> ownerUrnStrs = params.getParams().get(VALUES_PARAM);
    List<Urn> ownerUrns = ownerUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(opContext, ownerUrns, entityTypeToUrns.getValue());
    }
  }

  private void applyInternal(
      @Nonnull OperationContext opContext, List<Urn> ownerUrns, List<Urn> urns) {
    if (!urns.isEmpty()) {
      this.ownerService.batchRemoveOwners(
          opContext, ownerUrns, getResourceReferences(urns), METADATA_TESTS_SOURCE);
    }
  }
}
