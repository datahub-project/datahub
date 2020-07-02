package com.linkedin.metadata.builders.search;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * A class that holds all the registered {@link BaseIndexBuilder}.
 *
 * Register new type of index builders by adding them to {@link #REGISTERED_INDEX_BUILDERS}.
 */
public class RegisteredIndexBuilders {

  public static final Set<BaseIndexBuilder> REGISTERED_INDEX_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseIndexBuilder>() {
        {
          add(new CorpUserInfoIndexBuilder());
          add(new DataProcessIndexBuilder());
          add(new DatasetIndexBuilder());
        }
      });

  private RegisteredIndexBuilders() {
    // Util class
  }
}