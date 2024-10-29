package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EthicalConsiderations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class EthicalConsiderationsMapper
    implements ModelMapper<com.linkedin.ml.metadata.EthicalConsiderations, EthicalConsiderations> {

  public static final EthicalConsiderationsMapper INSTANCE = new EthicalConsiderationsMapper();

  public static EthicalConsiderations map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.EthicalConsiderations ethicalConsiderations) {
    return INSTANCE.apply(context, ethicalConsiderations);
  }

  @Override
  public EthicalConsiderations apply(
      @Nullable QueryContext context,
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
