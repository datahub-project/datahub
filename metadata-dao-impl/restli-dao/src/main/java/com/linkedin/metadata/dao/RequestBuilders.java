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
 * A class that holds a list of registered {@link BaseRequestBuilder} and provides Urn-to-builder lookup.
 */
public class RequestBuilders {

  private static final Set<BaseRequestBuilder> REQUEST_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRequestBuilder>() {
        {
          add(new CorpGroupActionRequestBuilder());
          add(new CorpUserActionRequestBuilder());
          add(new DataProcessActionRequestBuilder());
          add(new DatasetActionRequestBuilder());
        }
      });

  private static final Map<Class<? extends Urn>, BaseRequestBuilder> URN_BUILDER_MAP =
      REQUEST_BUILDERS.stream().collect(Collectors.toMap(builder -> builder.urnClass(), Function.identity()));

  private RequestBuilders() {
    // Util class
  }

  @Nonnull
  public static <URN extends Urn> BaseRequestBuilder getBuilder(@Nonnull URN urn) {
    final Class<? extends Urn> urnClass = getBaseUrnClass(urn);
    final BaseRequestBuilder builder = URN_BUILDER_MAP.get(urnClass);
    if (builder == null) {
      throw new IllegalArgumentException(urnClass.getCanonicalName() + " is not a supported URN type");
    }

    return builder;
  }

  /**
   * Returns the base URN class for an urn object that may actually be an instance of a subclass of the base URN.
   */
  @Nonnull
  private static <URN extends Urn> Class<? extends Urn> getBaseUrnClass(@Nonnull URN urn) {
    Class<? extends Urn> clazz = urn.getClass();
    if (clazz == Urn.class) {
      throw new IllegalArgumentException("urn must be a subclass of Urn");
    }

    while (clazz.getSuperclass() != Urn.class) {
      clazz = (Class<? extends Urn>) clazz.getSuperclass();
    }
    return clazz;
  }
}