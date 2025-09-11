package com.linkedin.metadata.test.action.dataproduct;

import static com.linkedin.metadata.Constants.METADATA_TESTS_SOURCE;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.test.definition.ActionType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SetDataProductAction extends BaseDataProductAction {

  public SetDataProductAction(DataProductService dataProductService) {
    super(dataProductService);
  }

  @Override
  public ActionType getActionType() {
    return ActionType.SET_DATA_PRODUCT;
  }

  @Override
  void applyInternal(@Nonnull OperationContext opContext, Urn dataProductUrn, List<Urn> urns) {
    log.info("Setting data product {} for {} entities", dataProductUrn, urns.size());

    try {
      dataProductService.batchSetDataProduct(
          opContext, dataProductUrn, getResourceReferences(urns), METADATA_TESTS_SOURCE);
      log.info("Successfully set data product {} for {} entities", dataProductUrn, urns.size());
    } catch (Exception e) {
      log.error(
          "Failed to set data product {} for entities: {}", dataProductUrn, e.getMessage(), e);
      throw new RuntimeException("Failed to set data product: " + e.getMessage(), e);
    }
  }
}
