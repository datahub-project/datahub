package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.SignalRequest;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestSignal;
import com.linkedin.metadata.Constants;
import javax.annotation.Nullable;

public class SignalRequestUtils {
  @Nullable
  public static SignalRequest mapSignalRequest(
      final Urn entityUrn, final EntityResponse entityResponse) {
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    final EnvelopedAspect envelopedInput =
        aspects.get(Constants.EXECUTION_REQUEST_SIGNAL_ASPECT_NAME);
    if (envelopedInput != null) {
      final ExecutionRequestSignal inputSignalRequest =
          new ExecutionRequestSignal(envelopedInput.getValue().data());
      final String executorId = inputSignalRequest.getExecutorId();

      if (executorId != null) {
        final SignalRequest signalRequest = new SignalRequest();
        signalRequest.setExecId(entityUrn.toString());
        signalRequest.setExecutorId(executorId);
        signalRequest.setSignal(inputSignalRequest.getSignal());
        return signalRequest;
      }
    }
    return null;
  }

  private SignalRequestUtils() {}
}
