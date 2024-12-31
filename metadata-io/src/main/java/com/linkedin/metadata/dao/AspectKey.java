package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import lombok.NonNull;
import lombok.Value;

/** A value class that holds the components of a key for metadata retrieval. */
@Value
public class AspectKey<URN extends Urn, ASPECT extends RecordTemplate> {

  @NonNull Class<ASPECT> aspectClass;

  @NonNull URN urn;

  @NonNull Long version;
}
