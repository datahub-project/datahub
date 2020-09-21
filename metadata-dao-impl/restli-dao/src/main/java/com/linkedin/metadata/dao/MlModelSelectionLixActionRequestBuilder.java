package com.linkedin.metadata.dao;

import com.linkedin.common.urn.MlModelSelectionLixUrn;
import com.linkedin.metadata.snapshot.MlModelSelectionLixSnapshot;


/**
 * An action request builder for mlModelSelectionLix entities.
 */
public class MlModelSelectionLixActionRequestBuilder extends BaseActionRequestBuilder<MlModelSelectionLixSnapshot, MlModelSelectionLixUrn> {

  private static final String BASE_URI_TEMPLATE = "mlModelSelectionLixes";

  public MlModelSelectionLixActionRequestBuilder() {
    super(MlModelSelectionLixSnapshot.class, MlModelSelectionLixUrn.class, BASE_URI_TEMPLATE);
  }
}
