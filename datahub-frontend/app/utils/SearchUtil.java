package utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;


/**
 * Utility functions for Search
 */
public class SearchUtil {

    public static final String REQUEST_TYPE = "type";
    public static final String REQUEST_INPUT = "input";
    public static final String REQUEST_FIELD = "field";
    public static final String REQUEST_START = "start";
    public static final String REQUEST_COUNT = "count";
    public static final String REQUEST_LIMIT = "limit";

    public static final String CORP_USER_TYPE = "corpuser";
    public static final String DATASET_TYPE = "dataset";

    public static final Set<String> CORP_USER_FACET_FIELDS = Collections.emptySet();
    public static final Set<String> DATASET_FACET_FIELDS =
            ImmutableSet.of("origin", "platform");
    public static final ImmutableMap<String, Set<String>> FACET_FIELDS = ImmutableMap.of(
            CORP_USER_TYPE, CORP_USER_FACET_FIELDS,
            DATASET_TYPE, DATASET_FACET_FIELDS
    );

    public static final int _DEFAULT_START_VALUE = 0;
    public static final int _DEFAULT_PAGE_SIZE = 10;
    public static final int _DEFAULT_LIMIT_VALUE = 20;

    private SearchUtil() {
        //utility class
    }

    /**
     * Returns the string with the forward slash escaped
     * More details on reserved characters in Elasticsearch can be found at,
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
     *
     * @param input
     * @return
     */
    @Nonnull
    public static String escapeForwardSlash(@Nonnull String input) {
        if (input.contains("/")) {
            input = input.replace("/", "\\\\/");
        }
        return input;
    }
}
