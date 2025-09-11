package com.linkedin.metadata.test.action.structuredproperty;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.test.action.ActionUtils.getEntityTypeToUrns;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.StructuredPropertyService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.UrnValuesAction;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class BaseStructuredPropertyAction extends UrnValuesAction {
  protected final StructuredPropertyService structuredPropertyService;

  @Override
  public void apply(@Nonnull OperationContext opContext, List<Urn> urns, ActionParameters params)
      throws InvalidOperandException {
    // For each entity type, group then apply the action.
    final List<String> structuredPropertyUrnStrs = params.getParams().get(VALUES_PARAM);
    final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
    for (Map.Entry<String, List<Urn>> entityTypeToUrn : entityTypesToUrns.entrySet()) {
      List<Urn> entityUrns = entityTypeToUrn.getValue();
      if (entityUrns.isEmpty()) continue;
      Urn structuredPropertyUrn = UrnUtils.getUrn(structuredPropertyUrnStrs.get(0));
      applyInternal(opContext, structuredPropertyUrn, entityUrns, params);
    }
  }

  @Override
  protected Set<String> validValueEntityTypes() {
    return Set.of(STRUCTURED_PROPERTY_ENTITY_NAME);
  }

  abstract void applyInternal(
      @Nonnull OperationContext opContext,
      Urn structuredPropertyUrn,
      List<Urn> urns,
      ActionParameters params);
}
