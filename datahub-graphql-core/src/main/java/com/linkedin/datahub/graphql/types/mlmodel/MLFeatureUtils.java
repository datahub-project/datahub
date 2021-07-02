package com.linkedin.datahub.graphql.types.mlmodel;

import com.linkedin.common.urn.MLFeatureUrn;

import java.net.URISyntaxException;

public class MLFeatureUtils {

    private MLFeatureUtils() { }

    static MLFeatureUrn getMLFeatureUrn(String modelUrn) {
        try {
            return MLFeatureUrn.createFromString(modelUrn);
        } catch (URISyntaxException uriSyntaxException) {
            throw new RuntimeException(String.format("Failed to retrieve mlmodel with urn %s, invalid urn", modelUrn));
        }
    }
}
