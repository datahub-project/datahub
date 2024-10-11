package com.linkedin.metadata.service.util;

import com.linkedin.common.urn.Urn;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;


@FunctionalInterface
public interface FormToEntitiesConsumer<O extends OperationContext,E extends List<Urn>, F extends Urn> {

  /**
   * Consumes the given parameters and performs the specified operation by the implementing function
   * @param opContext an operation context
   * @param entityUrns a list of urns
   * @param formUrn a form urn
   */
  void accept(O opContext, E entityUrns, F formUrn);

}
