package com.linkedin.datahub.graphql.types.mlmodel;

import java.net.URISyntaxException;

import com.linkedin.common.urn.MLModelUrn;

public class MLModelUtils {

    private MLModelUtils() { }

    static MLModelUrn getMLModelUrn(String modelUrn) {
        try {
            return MLModelUrn.createFromString(modelUrn);
        } catch (URISyntaxException uriSyntaxException) {
            throw new RuntimeException(String.format("Failed to retrieve mlmodel with urn %s, invalid urn", modelUrn));
        }
    }
}
