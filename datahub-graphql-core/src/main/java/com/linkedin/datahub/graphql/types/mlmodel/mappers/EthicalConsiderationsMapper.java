package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.EthicalConsiderations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class EthicalConsiderationsMapper
    implements ModelMapper<com.linkedin.ml.metadata.EthicalConsiderations, EthicalConsiderations> {

  public static final EthicalConsiderationsMapper INSTANCE = new EthicalConsiderationsMapper();

  public static EthicalConsiderations map(
      @NonNull final com.linkedin.ml.metadata.EthicalConsiderations ethicalConsiderations) {
    return INSTANCE.apply(ethicalConsiderations);
  }

  @Override
  public EthicalConsiderations apply(
      @NonNull final com.linkedin.ml.metadata.EthicalConsiderations ethicalConsiderations) {
    final EthicalConsiderations result = new EthicalConsiderations();
    result.setData(ethicalConsiderations.getData());
    result.setHumanLife(ethicalConsiderations.getHumanLife());
    result.setMitigations(ethicalConsiderations.getMitigations());
    result.setRisksAndHarms(ethicalConsiderations.getRisksAndHarms());
    result.setUseCases(ethicalConsiderations.getUseCases());
    return result;
  }
}
