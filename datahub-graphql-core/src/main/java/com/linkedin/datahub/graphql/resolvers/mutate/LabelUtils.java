package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableMap;
import java.util.Map;


public class LabelUtils {
  private LabelUtils() { }

  public static Map<String, String> entityTypeToMethod = ImmutableMap.of(
      "tag", "getGlobalTags",
      "glossaryTerm", "getGlossaryTerms"
  );
}
