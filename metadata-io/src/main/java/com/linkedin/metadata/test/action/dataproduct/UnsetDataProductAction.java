package com.linkedin.metadata.test.action.dataproduct;

import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.NoValidationAction;
import com.linkedin.metadata.test.definition.ActionType;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UnsetDataProductAction extends NoValidationAction {

  private final DataProductService dataProductService;

  @Override
  public ActionType getActionType() {
    return ActionType.UNSET_DATA_PRODUCT;
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    if (urns.isEmpty()) return;

    try {
      // Use batch method for consistency with other services
      this.dataProductService.batchUnsetDataProduct(
          opContext, this.getResourceReferences(urns), METADATA_TESTS_SOURCE);
      log.info("Successfully unset data products for {} entities", urns.size());
    } catch (Exception e) {
      log.error("Failed to unset data product for entities {}", urns, e);
      throw new InvalidOperandException(
          String.format("Failed to unset data product for entities %s", urns), e);
    }
  }
}
