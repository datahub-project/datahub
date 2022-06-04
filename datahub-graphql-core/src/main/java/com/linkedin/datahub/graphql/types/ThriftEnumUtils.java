package com.linkedin.datahub.graphql.types;


import java.net.URISyntaxException;
import java.util.regex.Pattern;

import com.linkedin.common.urn.ThriftEnumUrn;

public class ThriftEnumUtils {

    private ThriftEnumUtils() { }

    public static ThriftEnumUrn getThriftEnumUrn(String urnStr) {
        try {
            return ThriftEnumUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve ThriftEnum with urn %s, invalid urn", urnStr));
        }
    }

    public static String getThriftEnumName(String hierarchicalName) {
        if (hierarchicalName.contains(".")) {
            String[] nodes = hierarchicalName.split(Pattern.quote("."));
            return nodes[nodes.length - 1];
        }
        return hierarchicalName;
    }
}