package io.datahub.test.fixtures.elasticsearch;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Utils {
    private Utils() {

    }
    final public static String FIXTURE_BASE = "src/test/resources/elasticsearch";

    final public static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
}
