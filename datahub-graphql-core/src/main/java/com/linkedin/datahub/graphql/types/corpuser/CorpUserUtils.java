package com.linkedin.datahub.graphql.types.corpuser;

import java.net.URISyntaxException;

import com.linkedin.common.urn.CorpuserUrn;

public class CorpUserUtils {

    private CorpUserUtils() { }

    public static CorpuserUrn getCorpUserUrn(final String urnStr) {
        if (urnStr == null) {
            return null;
        }
        try {
            return CorpuserUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to create CorpUserUrn from string %s", urnStr), e);
        }
    }
}
