package io.datahubproject.openapi.v2.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.linkedin.common.urn.Urn;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenericRelationship {
  @Nonnull private String relationshipType;
  @Nonnull private GenericNode destination;
  @Nonnull private GenericNode source;
  @Nullable private NodeProperties properties;

  @Data
  @Builder
  public static class GenericNode {
    @Nonnull private String entityType;
    @Nonnull private String urn;

    public static GenericNode fromUrn(@Nonnull Urn urn) {
      return GenericNode.builder().entityType(urn.getEntityType()).urn(urn.toString()).build();
    }
  }

  @Data
  @Builder
  public static class NodeProperties {
    private List<String> source;
  }
}
