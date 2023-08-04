package io.datahub.test.action.owner;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.resource.ResourceReference;
import com.linkedin.metadata.service.OwnerService;
import com.linkedin.metadata.test.action.Action;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.ActionType;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.test.action.ActionUtils.*;


@Slf4j
@RequiredArgsConstructor
public class AddOwnersAction implements Action {

  private static final OwnershipType DEFAULT_OWNERSHIP_TYPE = OwnershipType.TECHNICAL_OWNER;
  private static final String VALUES_PARAM = "values";
  private static final String OWNERSHIP_TYPE_PARAM = "ownerType";

  private final OwnerService ownerService;

  @Override
  public ActionType getActionType() {
    return ActionType.ADD_OWNERS;
  }

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    if (!params.getParams().containsKey(VALUES_PARAM)) {
      throw new InvalidActionParamsException("Action parameters are missing the required 'values' parameter.");
    }
  }

  @Override
  public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
    // For each entity type, group then apply the action.
    List<String> ownerUrnStrs = params.getParams().get(VALUES_PARAM);
    List<String> ownershipTypes = params.getParams().get(OWNERSHIP_TYPE_PARAM);
    com.linkedin.common.OwnershipType ownershipType = ownershipTypes != null && ownershipTypes.size() == 1
        ? OwnershipType.valueOf(ownershipTypes.get(0))
        : OwnershipType.NONE;
    List<Urn> ownerUrns = ownerUrnStrs.stream()
        .map(UrnUtils::getUrn)
        .collect(Collectors.toList());

    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrns : entityTypesToUrns.entrySet()) {
      applyInternal(ownerUrns, entityTypeToUrns.getValue(), ownershipType);
    }
  }

  private void applyInternal(
      @Nonnull final List<Urn> ownerUrns,
      @Nonnull final List<Urn> urns,
      @Nonnull final OwnershipType ownershipType) {
    if (!urns.isEmpty()) {
      this.ownerService.batchAddOwners(ownerUrns, urns.stream()
          .map(urn -> new ResourceReference(urn, null, null))
          .collect(Collectors.toList()),
          ownershipType, Constants.METADATA_TESTS_SOURCE);
    }
  }
}