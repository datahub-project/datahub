package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * A class that holds a list of registered {@link BaseSnapshotRequestBuilder} and provides Urn-to-builder lookup.
 */
public class RequestBuilders {

  private static final Set<BaseSnapshotRequestBuilder> REQUEST_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseSnapshotRequestBuilder>() {
        {
          add(new CorpUserSnapshotRequestBuilder());
          add(new DatasetSnapshotRequestBuilder());
        }
      });

  private static final Map<Class<? extends Urn>, BaseSnapshotRequestBuilder> URN_BUILDER_MAP =
      REQUEST_BUILDERS.stream().collect(Collectors.toMap(builder -> builder.urnClass(), Function.identity()));

  private RequestBuilders() {
    // Util class
  }

  @Nonnull
  public static <URN extends Urn> BaseSnapshotRequestBuilder getBuilder(@Nonnull URN urn) {
    final Class<? extends Urn> urnClass = urn.getClass();
    final BaseSnapshotRequestBuilder builder = URN_BUILDER_MAP.get(urnClass);
    if (builder == null) {
      throw new IllegalArgumentException(urnClass.getCanonicalName() + " is not a supported URN type");
    }

    return builder;
  }
}