package datahub.hive.consumer.config;

/**
 * Application constants.
 */
public final class Constants {
    
    public static final String PLATFORM_NAME = "hive";

    public static final String ENVIRONMENT_KEY = "environment";

    public static final String PLATFORM_INSTANCE_KEY = "platformInstance";

    public static final String TABLE_KEY = "Table";

    public static final String DATASET_KEY = "dataset";

    public static final String INPUTS_KEY = "inputs";

    public static final String OUTPUTS_KEY = "outputs";

    public static final String HASH_KEY = "hash";

    public static final String QUERY_KEY = "query";

    public static final String QUERY_URN_PREFIX = "urn:li:query:";

    public static final String CORP_USER_URN_PREFIX = "urn:li:corpuser:";

    public static final String EDGES_KEY = "edges";

    public static final String VERTICES_KEY = "vertices";

    public static final String SOURCES_KEY = "sources";

    public static final String TARGETS_KEY = "targets";

    public static final String EDGE_TYPE_KEY = "edgeType";

    public static final String VERTEX_TYPE_KEY = "vertexType";

    public static final String VERTEX_ID_KEY = "vertexId";

    public static final String EXPRESSION_KEY = "expression";

    public static final String QUERY_TEXT_KEY = "queryText";

    public static final String PROJECTION_KEY = "PROJECTION";

    public static final String COLUMN_KEY = "COLUMN";

    public static final String SCHEMA_FIELD_URN_PREFIX = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:";

    public static final String DATA_PLATFORM_INSTANCE_URN_PREFIX = "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hive,";

    private Constants() {
        // Private constructor to prevent instantiation
    }
}
