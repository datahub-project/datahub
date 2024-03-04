package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;

public interface SearchIndicesService {
  void handleChangeEvent(@Nonnull MetadataChangeLog metadataChangeLog);

  void initializeAspectRetriever(@Nonnull AspectRetriever aspectRetriever);
}
