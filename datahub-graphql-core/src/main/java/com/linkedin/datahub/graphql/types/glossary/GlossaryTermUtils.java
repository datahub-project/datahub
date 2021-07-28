package com.linkedin.datahub.graphql.types.glossary;

import com.linkedin.common.urn.GlossaryTermUrn;

import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class GlossaryTermUtils {

    private GlossaryTermUtils() { }

    static GlossaryTermUrn getGlossaryTermUrn(String urnStr) {
        try {
            return GlossaryTermUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve glossary with urn %s, invalid urn", urnStr));
        }
    }

    public static String getGlossaryTermName(String hierarchicalName) {
        if (hierarchicalName.contains(".")) {
            String[] nodes = hierarchicalName.split(Pattern.quote("."));
            return nodes[nodes.length - 1];
        }
        return hierarchicalName;
    }

    public static String getGlossaryTermDomain(String hierarchicalName){
        String domain = "";
        if(hierarchicalName.contains(".")){
            String[] nodes = hierarchicalName.split(Pattern.quote("."));
            for(int i = 0; i < nodes.length - 2; i++) {
                domain = domain + nodes[i] + ".";
            }
            domain = domain + nodes[nodes.length - 2];
        }
        return domain;
    }
}
