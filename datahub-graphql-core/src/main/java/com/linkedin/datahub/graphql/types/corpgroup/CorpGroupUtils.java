package com.linkedin.datahub.graphql.types.corpgroup;

import com.linkedin.common.urn.CorpGroupUrn;
import java.net.URISyntaxException;

public class CorpGroupUtils {

  private CorpGroupUtils() {}

  public static CorpGroupUrn getCorpGroupUrn(final String urnStr) {
    if (urnStr == null) {
      return null;
    }
    try {
      return CorpGroupUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to create CorpGroupUrn from string %s", urnStr), e);
    }
  }
}
