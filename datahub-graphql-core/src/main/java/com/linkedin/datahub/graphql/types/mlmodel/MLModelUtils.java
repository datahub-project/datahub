package com.linkedin.datahub.graphql.types.mlmodel;

import com.linkedin.common.urn.MLFeatureUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;

public class MLModelUtils {

  private MLModelUtils() {}

  static MLModelUrn getMLModelUrn(String modelUrn) {
    try {
      return MLModelUrn.createFromString(modelUrn);
    } catch (URISyntaxException uriSyntaxException) {
      throw new RuntimeException(
          String.format("Failed to retrieve mlmodel with urn %s, invalid urn", modelUrn));
    }
  }

  static Urn getMLModelGroupUrn(String modelUrn) {
    try {
      return Urn.createFromString(modelUrn);
    } catch (URISyntaxException uriSyntaxException) {
      throw new RuntimeException(
          String.format("Failed to retrieve mlModelGroup with urn %s, invalid urn", modelUrn));
    }
  }

  static MLFeatureUrn getMLFeatureUrn(String modelUrn) {
    try {
      return MLFeatureUrn.createFromString(modelUrn);
    } catch (URISyntaxException uriSyntaxException) {
      throw new RuntimeException(
          String.format("Failed to retrieve mlFeature with urn %s, invalid urn", modelUrn));
    }
  }

  static Urn getUrn(String modelUrn) {
    try {
      return Urn.createFromString(modelUrn);
    } catch (URISyntaxException uriSyntaxException) {
      throw new RuntimeException(
          String.format("Failed to retrieve with urn %s, invalid urn", modelUrn));
    }
  }
}
