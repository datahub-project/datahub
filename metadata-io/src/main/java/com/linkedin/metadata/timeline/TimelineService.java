package com.linkedin.metadata.timeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public interface TimelineService {

  List<ChangeTransaction> getTimeline(@Nonnull final Urn urn,
      @Nonnull Set<ChangeCategory> elements,
      long startMillis,
      long endMillis,
      String startVersionStamp,
      String endVersionStamp,
      boolean rawDiffRequested) throws JsonProcessingException;
}
