package com.linkedin.metadata.graph;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.Filter;
import com.linkedin.metadata.query.RelationshipFilter;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface GraphService {

  void addEdge(final Edge edge);

  @Nonnull
  List<String> findRelatedUrns(
      @Nullable final String sourceType,
      @Nonnull final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      final int offset,
      final int count);

  void removeNode(@Nonnull final Urn urn);

  void removeEdgesFromNode(
      @Nonnull final Urn urn,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter);

  void configure();

  void clear();
}
