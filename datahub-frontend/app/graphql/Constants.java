package graphql;

/**
 * Constants relating to GraphQL type system & execution.
 */
public class Constants {

    private Constants() { };

    /**
     * Type Names.
     */
    public static final String QUERY_TYPE_NAME = "Query";
    public static final String MUTATION_TYPE_NAME = "Mutation";
    public static final String CORP_USER_TYPE_NAME = "CorpUser";
    public static final String OWNER_TYPE_NAME = "Owner";
    public static final String SEARCH_RESULT_TYPE_NAME = "SearchResult";

    /**
     * Field Names.
     */
    public static final String LOG_IN_FIELD_NAME = "logIn";
    public static final String URN_FIELD_NAME = "urn";
    public static final String DATASETS_FIELD_NAME = "dataset";
    public static final String SEARCH_FIELD_NAME = "search";
    public static final String AUTO_COMPLETE_FIELD_NAME = "autoComplete";
    public static final String BROWSE_FIELD_NAME = "browse";
    public static final String BROWSE_PATHS_FIELD_NAME = "browsePaths";
    public static final String UPDATE_DATASET_FIELD_NAME = "updateDataset";
    public static final String MANAGER_FIELD_NAME = "manager";
    public static final String OWNER_FIELD_NAME = "owner";
    public static final String INPUT_FIELD_NAME = "input";

    /**
     * DataLoader Names
     */
    public static final String DATASET_LOADER_NAME = "datasetLoader";
    public static final String OWNERSHIP_LOADER_NAME = "ownershipLoader";
    public static final String CORP_USER_LOADER_NAME = "corpUserLoader";

    /**
     * Misc.
     */
    public static final String GRAPH_SCHEMA_FILE = "datahub-frontend.graphql";
}
