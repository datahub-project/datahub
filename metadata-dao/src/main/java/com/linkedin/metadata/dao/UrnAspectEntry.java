package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.List;
import lombok.NonNull;
import lombok.Value;


/**
 * A value class that holds urn of a given entity and list of aspects (as {@link RecordTemplate}) associated with the urn.
 */
@Value
public class UrnAspectEntry<URN extends Urn> {

  @NonNull
  URN urn;

  @NonNull
  List<RecordTemplate> aspects;
}