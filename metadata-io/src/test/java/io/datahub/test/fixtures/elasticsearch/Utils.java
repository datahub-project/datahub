package io.datahub.test.fixtures.elasticsearch;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.linkedin.metadata.Constants.*;


public class Utils {
    private Utils() {

    }
    final public static String FIXTURE_BASE = "src/test/resources/elasticsearch";

    final public static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    static {
        int maxSize = Integer.parseInt(System.getenv().getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
        OBJECT_MAPPER.getFactory().setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    }
}
