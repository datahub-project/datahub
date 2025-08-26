package com.linkedin.datahub.upgrade.system.executorpools;

import com.linkedin.executorpool.*;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.utils.GenericRecordUtils;
import org.mockito.ArgumentMatcher;

public class ExecutorPoolsStepMatcherTest implements ArgumentMatcher<AspectsBatch> {

  private AspectsBatch left;

  public ExecutorPoolsStepMatcherTest(AspectsBatch left) {
    this.left = left;
  }

  @Override
  public boolean matches(AspectsBatch right) {
    if (left.getItems().size() != right.getItems().size()) {
      return false;
    }
    for (int i = 0; i < left.getMCPItems().size(); i++) {
      RemoteExecutorPoolInfo leftItem =
          GenericRecordUtils.deserializeAspect(
              left.getMCPItems().get(i).getMetadataChangeProposal().getAspect().getValue(),
              left.getMCPItems().get(i).getMetadataChangeProposal().getAspect().getContentType(),
              RemoteExecutorPoolInfo.class);
      RemoteExecutorPoolInfo rightItem =
          GenericRecordUtils.deserializeAspect(
              right.getMCPItems().get(i).getMetadataChangeProposal().getAspect().getValue(),
              right.getMCPItems().get(i).getMetadataChangeProposal().getAspect().getContentType(),
              RemoteExecutorPoolInfo.class);

      if (!leftItem.getQueueUrl().equals(rightItem.getQueueUrl())
          || !leftItem.getQueueRegion().equals(rightItem.getQueueRegion())
          || !leftItem.getDescription().equals(rightItem.getDescription())
          || (leftItem.isIsEmbedded() != rightItem.isIsEmbedded())
          || (leftItem.getState().getStatus() != rightItem.getState().getStatus())) {
        return false;
      }
    }
    return true;
  }
}
