package com.linkedin.datahub.graphql.types.glossary;

import com.linkedin.common.urn.GlossaryTermUrn;

import java.net.URISyntaxException;

public class GlossaryTermUtils {

    private GlossaryTermUtils() { }

    static GlossaryTermUrn getGlossaryTermUrn(String urnStr) {
        try {
            return GlossaryTermUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve glossary with urn %s, invalid urn", urnStr));
        }
    }
}
