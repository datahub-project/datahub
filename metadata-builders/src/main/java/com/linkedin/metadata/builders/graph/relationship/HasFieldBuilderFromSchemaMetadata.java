package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.HasField;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;

public class HasFieldBuilderFromSchemaMetadata  extends BaseRelationshipBuilder<SchemaMetadata> {

  public HasFieldBuilderFromSchemaMetadata() {
    super(SchemaMetadata.class);
  }

  @Nonnull
  @Override
  public <URN extends Urn> List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull URN urn,
      @Nonnull SchemaMetadata schemaMetadata) {
    if (schemaMetadata.getFields() == null) {
      return Collections.emptyList();
    }
    List<HasField> list = new ArrayList();
    for (SchemaField field : schemaMetadata.getFields()) {
      try {
        list.add(new HasField().setSource(urn).setDestination(new Urn(urn.toString() + ":" + field.getFieldPath())));
      } catch (URISyntaxException e) {
        return null;
      }
    }
    return Collections.singletonList(new GraphBuilder.RelationshipUpdates(
        list,
        REMOVE_ALL_EDGES_FROM_SOURCE)
    );
  }
}
