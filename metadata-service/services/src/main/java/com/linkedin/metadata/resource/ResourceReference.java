package com.linkedin.metadata.resource;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ResourceReference {
  /** The urn of an entity */
  Urn urn;

  /** The type of the SubResource */
  SubResourceType subResourceType;

  /** The subresource being targeted */
  String subResource;

  public static Optional<ResourceReference> fromSchemaFieldUrn(@Nonnull Urn urn) {
    return SchemaFieldUtils.parseSchemaFieldUrn(urn)
        .map(
            pair ->
                new ResourceReference(
                    pair.getFirst(), SubResourceType.DATASET_FIELD, pair.getSecond()));
  }
}
