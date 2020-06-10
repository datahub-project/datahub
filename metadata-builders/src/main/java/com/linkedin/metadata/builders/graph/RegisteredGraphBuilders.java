package com.linkedin.metadata.builders.graph;

import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;


/**
 * A class that holds all the registered {@link BaseGraphBuilder}.
 *
 * Register new type of graph builders by adding them to {@link #REGISTERED_GRAPH_BUILDERS}.
 */
public class RegisteredGraphBuilders {

  private static final List<BaseGraphBuilder> REGISTERED_GRAPH_BUILDERS =
      Collections.unmodifiableList(new LinkedList<BaseGraphBuilder>() {
        {
          add(new CorpUserGraphBuilder());
          add(new DataProcessGraphBuilder());
          add(new DatasetGraphBuilder());
        }
      });

  private static final Map<Class<? extends RecordTemplate>, BaseGraphBuilder> SNAPSHOT_BUILDER_MAP =
      Collections.unmodifiableMap(new HashMap<Class<? extends RecordTemplate>, BaseGraphBuilder>() {
        {
          REGISTERED_GRAPH_BUILDERS.forEach(builder -> put(builder.supportedSnapshotClass(), builder));
        }
      });

  private RegisteredGraphBuilders() {
    // Util class
  }

  /**
   * Gets a {@link BaseGraphBuilder} registered for a snapshot class.
   */
  @Nonnull
  public static Optional<BaseGraphBuilder> getGraphBuilder(@Nonnull Class<? extends RecordTemplate> snapshotClass) {
    return Optional.ofNullable(SNAPSHOT_BUILDER_MAP.get(snapshotClass));
  }
}
