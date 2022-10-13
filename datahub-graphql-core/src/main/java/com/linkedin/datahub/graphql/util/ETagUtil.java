package com.linkedin.datahub.graphql.util;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ETagUtil {

    // ETAG Comment: Utils class to convert eTag String into Map
    // eTag String format: "urn1=createdOn1:urn2=createdOn2:...:urnN=createdOnN"

    public static Map<String, Long> extractETag(String eTag) {
        Map<String, Long> createdOnMap = new HashMap<>();
        if (eTag != null) {
            Arrays.stream(eTag.split(":"))
                    .forEach(item -> {
                        String[] values = item.split("=");
                        if (values.length == 2) {
                            try {
                                createdOnMap.put(values[0], Long.parseLong(values[1]));
                            } catch (Exception e) {
                                log.warn("Invalid eTag: " + item);
                            }
                        }
                    });
        }
        return createdOnMap;
    }

    private ETagUtil() {
    }
}
