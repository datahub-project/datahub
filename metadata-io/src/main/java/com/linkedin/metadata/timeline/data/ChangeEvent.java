package com.linkedin.metadata.timeline.data;

import java.util.Map;
import lombok.Builder;
import lombok.Value;


@Value
@Builder
public class ChangeEvent {
  ChangeOperation changeType;
  SemanticChangeType semVerChange;
  /**
   * Target entity of the change
   */
  String target;
  ChangeCategory category;
  /**
   * ID of the element in the category type i.e. field Urn
   */
  String elementId;
  String description;
  //Enum? Subtype included, evaluate for propagation up to top level field case by case
  Map<String, Object> changeDetails;
}
