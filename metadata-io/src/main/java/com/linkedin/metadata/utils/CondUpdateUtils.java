package com.linkedin.metadata.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CondUpdateUtils {

    // condUpdate String format: "urn1=createdOn1;urn2=createdOn2;...;urnN=createdOnN"

    public static Map<String, Long> extractCondUpdate(String condUpdate) {
        Map<String, Long> createdOnMap = new HashMap<>();
        if (condUpdate != null) {
            Arrays.stream(condUpdate.split(";"))
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

    private CondUpdateUtils() {
    }
}
