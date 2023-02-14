package com.linkedin.metadata.resource;

import com.linkedin.common.urn.Urn;
import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class ResourceReference {
  /**
   * The urn of an entity
   */
  Urn urn;

  /**
   * The type of the SubResource
   */
  SubResourceType subResourceType;

  /**
   * The subresource being targeted
   */
  String subResource;
}
