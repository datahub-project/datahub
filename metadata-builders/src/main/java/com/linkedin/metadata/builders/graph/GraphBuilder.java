package com.linkedin.metadata.builders.graph;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.NonNull;
import lombok.Value;


public interface GraphBuilder<SNAPSHOT extends RecordTemplate> {

  @Nonnull
  GraphUpdates build(@Nonnull SNAPSHOT snapshot);

  @Value
  class GraphUpdates {

    @NonNull
    List<? extends RecordTemplate> entities;

    @NonNull
    List<RelationshipUpdates> relationshipUpdates;
  }

  @Value
  class RelationshipUpdates {

    @NonNull
    List<? extends RecordTemplate> relationships;

    @NonNull
    BaseGraphWriterDAO.RemovalOption preUpdateOperation;
  }
}
