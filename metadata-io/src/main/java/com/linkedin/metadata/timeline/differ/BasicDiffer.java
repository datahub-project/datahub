package com.linkedin.metadata.timeline.differ;

import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.Collections;


public class BasicDiffer implements Differ {
  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {

    return ChangeTransaction.builder()
        .semVerChange(SemanticChangeType.NONE)
        .changeEvents(Collections.singletonList(ChangeEvent.builder()
            .category(element)
            .changeType(previousValue.getVersion() == -1 ? ChangeOperation.ADD : ChangeOperation.MODIFY)
            .description(
                "A change in aspect " + currentValue.getAspect() + " happened at time " + currentValue.getCreatedOn())
            .target(currentValue.getUrn())
            .build()))
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }
}
