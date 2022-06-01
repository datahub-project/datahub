package com.datahub.authorization;

import javax.annotation.Nonnull;
import lombok.Value;


/**
 * Details about a specific resource being acted upon. Resource types currently supported
 * can be found inside of {@link PoliciesConfig}.
 */
@Value
public class ResourceSpec {
  /**
   * The resource type. Most often, this corresponds to the entity type. (dataset, chart, dashboard, corpGroup, etc).
   */
  @Nonnull
  String type;
  /**
   * The resource identity. Most often, this corresponds to the raw entity urn. (urn:li:corpGroup:groupId)
   */
  @Nonnull
  String resource;
}