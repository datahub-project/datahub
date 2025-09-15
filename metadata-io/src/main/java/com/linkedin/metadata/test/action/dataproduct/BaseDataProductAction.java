package com.linkedin.metadata.test.action.dataproduct;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.test.action.ActionUtils.getEntityTypeToUrns;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.UrnValuesAction;
import com.linkedin.metadata.test.exception.InvalidActionParamsException;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class BaseDataProductAction extends UrnValuesAction {

  protected final DataProductService dataProductService;

  @Override
  public void validate(ActionParameters params) throws InvalidActionParamsException {
    super.validate(params);
    List<String> dataProductUrns = params.getParams().get(VALUES_PARAM);
    if (dataProductUrns.size() != 1) {
      throw new InvalidActionParamsException(
          "Data product actions require exactly one data product URN. Found: "
              + dataProductUrns.size());
    }
  }

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // For each entity type, group then apply the action.
    final List<String> dataProductUrnStrs = params.getParams().get(VALUES_PARAM);
    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrn : entityTypesToUrns.entrySet()) {
      List<Urn> entityUrns = entityTypeToUrn.getValue();
      if (entityUrns.isEmpty()) continue;

      // Apply the single data product (validated above) to all entities
      Urn dataProductUrn = UrnUtils.getUrn(dataProductUrnStrs.get(0));
      applyInternal(opContext, dataProductUrn, entityUrns);
    }
  }

  @Override
  protected Set<String> validValueEntityTypes() {
    return Set.of(DATA_PRODUCT_ENTITY_NAME);
  }

  abstract void applyInternal(
      @Nonnull OperationContext opContext, Urn dataProductUrn, List<Urn> urns);
}
