package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import java.util.List;
import javax.annotation.Nonnull;


public abstract class BaseRelationshipBuilder<ASPECT extends RecordTemplate> {

  private Class<ASPECT> _aspectClass;

  public BaseRelationshipBuilder(@Nonnull Class<ASPECT> aspectClass) {
    _aspectClass = aspectClass;
  }

  /**
   * Returns the aspect class this {@link BaseRelationshipBuilder} supports
   */
  @Nonnull
  public Class<ASPECT> supportedAspectClass() {
    return _aspectClass;
  }

  /**
   * Returns a list of corresponding relationship updates for the given metadata aspect
   */
  @Nonnull
  public abstract <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull ASPECT aspect);
}
