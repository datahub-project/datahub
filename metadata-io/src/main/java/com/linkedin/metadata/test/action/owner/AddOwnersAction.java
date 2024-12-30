package com.linkedin.metadata.test.action.owner;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.OwnerServiceAsync;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.UrnValuesAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class AddOwnersAction extends UrnValuesAction {

  private static final OwnershipType DEFAULT_OWNERSHIP_TYPE = OwnershipType.TECHNICAL_OWNER;
  private static final String OWNERSHIP_TYPE_PARAM = "ownerType";

  private final OwnerServiceAsync ownerService;

  @Override
  public ActionType getActionType() {
    return ActionType.ADD_OWNERS;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // For each entity type, group then apply the action.
    List<String> ownerUrnStrs = params.getParams().get(VALUES_PARAM);
    List<String> ownershipTypes = params.getParams().get(OWNERSHIP_TYPE_PARAM);
    com.linkedin.common.OwnershipType ownershipType =
        ownershipTypes != null && ownershipTypes.size() == 1
            ? OwnershipType.valueOf(ownershipTypes.get(0))
            : OwnershipType.NONE;
    List<Urn> ownerUrns = ownerUrnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(opContext, ownerUrns, entityTypeToUrns.getValue(), ownershipType);
    }
  }

  @Override
  protected Set<String> validValueEntityTypes() {
    return Set.of(CORP_USER_ENTITY_NAME, CORP_GROUP_ENTITY_NAME);
  }

  private void applyInternal(
      @Nonnull OperationContext opContext,
      @Nonnull final List<Urn> ownerUrns,
      @Nonnull final List<Urn> urns,
      @Nonnull final OwnershipType ownershipType) {
    if (!urns.isEmpty() && !ownerUrns.isEmpty()) {
      this.ownerService.batchAddOwners(
          opContext,
          ownerUrns,
          getResourceReferences(urns),
          ownershipType,
          null,
          METADATA_TESTS_SOURCE,
          null);
    }
  }
}
