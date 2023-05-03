export type Maybe<T> = T | null;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
export type MakeOptional<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]?: Maybe<T[SubKey]> };
export type MakeMaybe<T, K extends keyof T> = Omit<T, K> & { [SubKey in K]: Maybe<T[SubKey]> };
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
    ID: string;
    String: string;
    Boolean: boolean;
    Int: number;
    Float: number;
    Long: number;
};

/** Input provided when accepting a DataHub role using an invite token */
export type AcceptRoleInput = {
    /** The token needed to accept the role */
    inviteToken: Scalars['String'];
};

/** The access level for a Metadata Entity, either public or private */
export enum AccessLevel {
    /** Publicly available */
    Public = 'PUBLIC',
    /** Restricted to a subset of viewers */
    Private = 'PRIVATE',
}

export type AccessToken = {
    __typename?: 'AccessToken';
    /** The access token itself */
    accessToken: Scalars['String'];
    /** Metadata about the generated token */
    metadata?: Maybe<AccessTokenMetadata>;
};

/** The duration for which an Access Token is valid. */
export enum AccessTokenDuration {
    /** 1 hour */
    OneHour = 'ONE_HOUR',
    /** 1 day */
    OneDay = 'ONE_DAY',
    /** 1 week */
    OneWeek = 'ONE_WEEK',
    /** 1 month */
    OneMonth = 'ONE_MONTH',
    /** 3 months */
    ThreeMonths = 'THREE_MONTHS',
    /** 6 months */
    SixMonths = 'SIX_MONTHS',
    /** 1 year */
    OneYear = 'ONE_YEAR',
    /** No expiry */
    NoExpiry = 'NO_EXPIRY',
}

export type AccessTokenMetadata = Entity & {
    __typename?: 'AccessTokenMetadata';
    /** The primary key of the access token */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** The unique identifier of the token. */
    id: Scalars['String'];
    /** The name of the token, if it exists. */
    name: Scalars['String'];
    /** The description of the token if defined. */
    description?: Maybe<Scalars['String']>;
    /** The actor associated with the Access Token. */
    actorUrn: Scalars['String'];
    /** The actor who created the Access Token. */
    ownerUrn: Scalars['String'];
    /** The time when token was generated at. */
    createdAt: Scalars['Long'];
    /** Time when token will be expired. */
    expiresAt?: Maybe<Scalars['Long']>;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
};

export type AccessTokenMetadataRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A type of DataHub Access Token. */
export enum AccessTokenType {
    /** Generates a personal access token */
    Personal = 'PERSONAL',
}

/** The actors that a DataHub Access Policy applies to */
export type ActorFilter = {
    __typename?: 'ActorFilter';
    /** A disjunctive set of users to apply the policy to */
    users?: Maybe<Array<Scalars['String']>>;
    /** A disjunctive set of groups to apply the policy to */
    groups?: Maybe<Array<Scalars['String']>>;
    /** A disjunctive set of roles to apply the policy to */
    roles?: Maybe<Array<Scalars['String']>>;
    /**
     * Whether the filter should return TRUE for owners of a particular resource
     * Only applies to policies of type METADATA, which have a resource associated with them
     */
    resourceOwners: Scalars['Boolean'];
    /** Whether the filter should apply to all users */
    allUsers: Scalars['Boolean'];
    /** Whether the filter should apply to all groups */
    allGroups: Scalars['Boolean'];
    /** The list of users on the Policy, resolved. */
    resolvedUsers?: Maybe<Array<CorpUser>>;
    /** The list of groups on the Policy, resolved. */
    resolvedGroups?: Maybe<Array<CorpGroup>>;
    /** The list of roles on the Policy, resolved. */
    resolvedRoles?: Maybe<Array<DataHubRole>>;
};

/** Input required when creating or updating an Access Policies Determines which actors the Policy applies to */
export type ActorFilterInput = {
    /** A disjunctive set of users to apply the policy to */
    users?: Maybe<Array<Scalars['String']>>;
    /** A disjunctive set of groups to apply the policy to */
    groups?: Maybe<Array<Scalars['String']>>;
    /**
     * Whether the filter should return TRUE for owners of a particular resource
     * Only applies to policies of type METADATA, which have a resource associated with them
     */
    resourceOwners: Scalars['Boolean'];
    /** Whether the filter should apply to all users */
    allUsers: Scalars['Boolean'];
    /** Whether the filter should apply to all groups */
    allGroups: Scalars['Boolean'];
};

/** Input required to add members to an external DataHub group */
export type AddGroupMembersInput = {
    /** The group to add members to */
    groupUrn: Scalars['String'];
    /** The members to add to the group */
    userUrns: Array<Scalars['String']>;
};

/** Input provided when adding the association between a Metadata Entity and a Link */
export type AddLinkInput = {
    /** The url of the link to add or remove */
    linkUrl: Scalars['String'];
    /** A label to attach to the link */
    label: Scalars['String'];
    /** The urn of the resource or entity to attach the link to, for example a dataset urn */
    resourceUrn: Scalars['String'];
};

/** Input required to add members to a native DataHub group */
export type AddNativeGroupMembersInput = {
    /** The group to add members to */
    groupUrn: Scalars['String'];
    /** The members to add to the group */
    userUrns: Array<Scalars['String']>;
};

/** Input provided when adding the association between a Metadata Entity and an user or group owner */
export type AddOwnerInput = {
    /** The primary key of the Owner to add or remove */
    ownerUrn: Scalars['String'];
    /** The owner type, either a user or group */
    ownerEntityType: OwnerEntityType;
    /** The ownership type for the new owner. If none is provided, then a new NONE will be added. */
    type?: Maybe<OwnershipType>;
    /** The urn of the resource or entity to attach or remove the owner from, for example a dataset urn */
    resourceUrn: Scalars['String'];
};

/** Input provided when adding multiple associations between a Metadata Entity and an user or group owner */
export type AddOwnersInput = {
    /** The primary key of the Owner to add or remove */
    owners: Array<OwnerInput>;
    /** The urn of the resource or entity to attach or remove the owner from, for example a dataset urn */
    resourceUrn: Scalars['String'];
};

/** Input provided when adding tags to an asset */
export type AddTagsInput = {
    /** The primary key of the Tags */
    tagUrns: Array<Scalars['String']>;
    /** The target Metadata Entity to add or remove the Tag to */
    resourceUrn: Scalars['String'];
    /** An optional type of a sub resource to attach the Tag to */
    subResourceType?: Maybe<SubResourceType>;
    /** An optional sub resource identifier to attach the Tag to */
    subResource?: Maybe<Scalars['String']>;
};

/** Input provided when adding Terms to an asset */
export type AddTermsInput = {
    /** The primary key of the Glossary Term to add or remove */
    termUrns: Array<Scalars['String']>;
    /** The target Metadata Entity to add or remove the Glossary Term from */
    resourceUrn: Scalars['String'];
    /** An optional type of a sub resource to attach the Glossary Term to */
    subResourceType?: Maybe<SubResourceType>;
    /** An optional sub resource identifier to attach the Glossary Term to */
    subResource?: Maybe<Scalars['String']>;
};

/** Information about the aggregation that can be used for filtering, included the field value and number of results */
export type AggregationMetadata = {
    __typename?: 'AggregationMetadata';
    /** A particular value of a facet field */
    value: Scalars['String'];
    /** The number of search results containing the value */
    count: Scalars['Long'];
    /** Entity corresponding to the facet field */
    entity?: Maybe<Entity>;
};

/** For consumption by UI only */
export type AnalyticsChart = TimeSeriesChart | BarChart | TableChart;

/** For consumption by UI only */
export type AnalyticsChartGroup = {
    __typename?: 'AnalyticsChartGroup';
    groupId: Scalars['String'];
    title: Scalars['String'];
    charts: Array<AnalyticsChart>;
};

/** Configurations related to the Analytics Feature */
export type AnalyticsConfig = {
    __typename?: 'AnalyticsConfig';
    /** Whether the Analytics feature is enabled and should be displayed */
    enabled: Scalars['Boolean'];
};

/** A list of disjunctive criterion for the filter. (or operation to combine filters) */
export type AndFilterInput = {
    /** A list of and criteria the filter applies to the query */
    and?: Maybe<Array<FacetFilterInput>>;
};

/**
 * Config loaded at application boot time
 * This configuration dictates the behavior of the UI, such as which features are enabled or disabled
 */
export type AppConfig = {
    __typename?: 'AppConfig';
    /** App version */
    appVersion?: Maybe<Scalars['String']>;
    /** Auth-related configurations */
    authConfig: AuthConfig;
    /** Configurations related to the Analytics Feature */
    analyticsConfig: AnalyticsConfig;
    /** Configurations related to the Policies Feature */
    policiesConfig: PoliciesConfig;
    /** Configurations related to the User & Group management */
    identityManagementConfig: IdentityManagementConfig;
    /** Configurations related to UI-based ingestion */
    managedIngestionConfig: ManagedIngestionConfig;
    /** Configurations related to Lineage */
    lineageConfig: LineageConfig;
    /** Configurations related to visual appearance, allows styling the UI without rebuilding the bundle */
    visualConfig: VisualConfig;
    /** Configurations related to tracking users in the app */
    telemetryConfig: TelemetryConfig;
    /** Configurations related to DataHub tests */
    testsConfig: TestsConfig;
    /** Configurations related to DataHub Views */
    viewsConfig: ViewsConfig;
};

/** A versioned aspect, or single group of related metadata, associated with an Entity and having a unique version */
export type Aspect = {
    /** The version of the aspect, where zero represents the latest version */
    version?: Maybe<Scalars['Long']>;
};

/** Params to configure what list of aspects should be fetched by the aspects property */
export type AspectParams = {
    /** Only fetch auto render aspects */
    autoRenderOnly?: Maybe<Scalars['Boolean']>;
};

/** Details for the frontend on how the raw aspect should be rendered */
export type AspectRenderSpec = {
    __typename?: 'AspectRenderSpec';
    /** Format the aspect should be displayed in for the UI. Powered by the renderSpec annotation on the aspect model */
    displayType?: Maybe<Scalars['String']>;
    /** Name to refer to the aspect type by for the UI. Powered by the renderSpec annotation on the aspect model */
    displayName?: Maybe<Scalars['String']>;
    /** Field in the aspect payload to index into for rendering. */
    key?: Maybe<Scalars['String']>;
};

/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type Assertion = EntityWithRelationships &
    Entity & {
        __typename?: 'Assertion';
        /** The primary key of the Assertion */
        urn: Scalars['String'];
        /** The standard Entity Type */
        type: EntityType;
        /** Standardized platform urn where the assertion is evaluated */
        platform: DataPlatform;
        /** Details about assertion */
        info?: Maybe<AssertionInfo>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /**
         * Lifecycle events detailing individual runs of this assertion. If startTimeMillis & endTimeMillis are not provided, the most
         * recent events will be returned.
         */
        runEvents?: Maybe<AssertionRunEventsResult>;
        /** Edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
    };

/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionRunEventsArgs = {
    status?: Maybe<AssertionRunStatus>;
    startTimeMillis?: Maybe<Scalars['Long']>;
    endTimeMillis?: Maybe<Scalars['Long']>;
    filter?: Maybe<FilterInput>;
    limit?: Maybe<Scalars['Int']>;
};

/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionLineageArgs = {
    input: LineageInput;
};

/** Type of assertion. Assertion types can evolve to span Datasets, Flows (Pipelines), Models, Features etc. */
export type AssertionInfo = {
    __typename?: 'AssertionInfo';
    /** Top-level type of the assertion. */
    type: AssertionType;
    /** Dataset-specific assertion information */
    datasetAssertion?: Maybe<DatasetAssertionInfo>;
};

/** The result of evaluating an assertion. */
export type AssertionResult = {
    __typename?: 'AssertionResult';
    /** The final result, e.g. either SUCCESS or FAILURE. */
    type: AssertionResultType;
    /** Number of rows for evaluated batch */
    rowCount?: Maybe<Scalars['Long']>;
    /** Number of rows with missing value for evaluated batch */
    missingCount?: Maybe<Scalars['Long']>;
    /** Number of rows with unexpected value for evaluated batch */
    unexpectedCount?: Maybe<Scalars['Long']>;
    /** Observed aggregate value for evaluated batch */
    actualAggValue?: Maybe<Scalars['Float']>;
    /** URL where full results are available */
    externalUrl?: Maybe<Scalars['String']>;
    /** Native results / properties of evaluation */
    nativeResults?: Maybe<Array<StringMapEntry>>;
};

/** The result type of an assertion, success or failure. */
export enum AssertionResultType {
    /** The assertion succeeded. */
    Success = 'SUCCESS',
    /** The assertion failed. */
    Failure = 'FAILURE',
}

/** An event representing an event in the assertion evaluation lifecycle. */
export type AssertionRunEvent = TimeSeriesAspect & {
    __typename?: 'AssertionRunEvent';
    /** The time at which the assertion was evaluated */
    timestampMillis: Scalars['Long'];
    /** Urn of assertion which is evaluated */
    assertionUrn: Scalars['String'];
    /** Urn of entity on which the assertion is applicable */
    asserteeUrn: Scalars['String'];
    /** Native (platform-specific) identifier for this run */
    runId: Scalars['String'];
    /** The status of the assertion run as per this timeseries event. */
    status: AssertionRunStatus;
    /** Specification of the batch which this run is evaluating */
    batchSpec?: Maybe<BatchSpec>;
    /** Information about the partition that was evaluated */
    partitionSpec?: Maybe<PartitionSpec>;
    /** Runtime parameters of evaluation */
    runtimeContext?: Maybe<Array<StringMapEntry>>;
    /** Results of assertion, present if the status is COMPLETE */
    result?: Maybe<AssertionResult>;
};

/** Result returned when fetching run events for an assertion. */
export type AssertionRunEventsResult = {
    __typename?: 'AssertionRunEventsResult';
    /** The total number of run events returned */
    total: Scalars['Int'];
    /** The number of failed run events */
    failed: Scalars['Int'];
    /** The number of succeeded run events */
    succeeded: Scalars['Int'];
    /** The run events themselves */
    runEvents: Array<AssertionRunEvent>;
};

/** The state of an assertion run, as defined within an Assertion Run Event. */
export enum AssertionRunStatus {
    /** An assertion run has completed. */
    Complete = 'COMPLETE',
}

/** An "aggregation" function that can be applied to column values of a Dataset to create the input to an Assertion Operator. */
export enum AssertionStdAggregation {
    /** Assertion is applied on individual column value */
    Identity = 'IDENTITY',
    /** Assertion is applied on column mean */
    Mean = 'MEAN',
    /** Assertion is applied on column median */
    Median = 'MEDIAN',
    /** Assertion is applied on number of distinct values in column */
    UniqueCount = 'UNIQUE_COUNT',
    /** Assertion is applied on proportion of distinct values in column */
    UniquePropotion = 'UNIQUE_PROPOTION',
    /** Assertion is applied on number of null values in column */
    NullCount = 'NULL_COUNT',
    /** Assertion is applied on proportion of null values in column */
    NullProportion = 'NULL_PROPORTION',
    /** Assertion is applied on column std deviation */
    Stddev = 'STDDEV',
    /** Assertion is applied on column min */
    Min = 'MIN',
    /** Assertion is applied on column std deviation */
    Max = 'MAX',
    /** Assertion is applied on column sum */
    Sum = 'SUM',
    /** Assertion is applied on all columns */
    Columns = 'COLUMNS',
    /** Assertion is applied on number of columns */
    ColumnCount = 'COLUMN_COUNT',
    /** Assertion is applied on number of rows */
    RowCount = 'ROW_COUNT',
    /** Other */
    Native = '_NATIVE_',
}

/** A standard operator or condition that constitutes an assertion definition */
export enum AssertionStdOperator {
    /** Value being asserted is between min_value and max_value */
    Between = 'BETWEEN',
    /** Value being asserted is less than max_value */
    LessThan = 'LESS_THAN',
    /** Value being asserted is less than or equal to max_value */
    LessThanOrEqualTo = 'LESS_THAN_OR_EQUAL_TO',
    /** Value being asserted is greater than min_value */
    GreaterThan = 'GREATER_THAN',
    /** Value being asserted is greater than or equal to min_value */
    GreaterThanOrEqualTo = 'GREATER_THAN_OR_EQUAL_TO',
    /** Value being asserted is equal to value */
    EqualTo = 'EQUAL_TO',
    /** Value being asserted is not null */
    NotNull = 'NOT_NULL',
    /** Value being asserted contains value */
    Contain = 'CONTAIN',
    /** Value being asserted ends with value */
    EndWith = 'END_WITH',
    /** Value being asserted starts with value */
    StartWith = 'START_WITH',
    /** Value being asserted matches the regex value. */
    RegexMatch = 'REGEX_MATCH',
    /** Value being asserted is one of the array values */
    In = 'IN',
    /** Value being asserted is not in one of the array values. */
    NotIn = 'NOT_IN',
    /** Other */
    Native = '_NATIVE_',
}

/** Parameter for AssertionStdOperator. */
export type AssertionStdParameter = {
    __typename?: 'AssertionStdParameter';
    /** The parameter value */
    value: Scalars['String'];
    /** The type of the parameter */
    type: AssertionStdParameterType;
};

/** The type of an AssertionStdParameter */
export enum AssertionStdParameterType {
    String = 'STRING',
    Number = 'NUMBER',
    List = 'LIST',
    Set = 'SET',
    Unknown = 'UNKNOWN',
}

/** Parameters for AssertionStdOperators */
export type AssertionStdParameters = {
    __typename?: 'AssertionStdParameters';
    /** The value parameter of an assertion */
    value?: Maybe<AssertionStdParameter>;
    /** The maxValue parameter of an assertion */
    maxValue?: Maybe<AssertionStdParameter>;
    /** The minValue parameter of an assertion */
    minValue?: Maybe<AssertionStdParameter>;
};

/** The top-level assertion type. Currently single Dataset assertions are the only type supported. */
export enum AssertionType {
    Dataset = 'DATASET',
}

/** A time stamp along with an optional actor */
export type AuditStamp = {
    __typename?: 'AuditStamp';
    /** When the audited action took place */
    time: Scalars['Long'];
    /** Who performed the audited action */
    actor?: Maybe<Scalars['String']>;
};

/** Configurations related to auth */
export type AuthConfig = {
    __typename?: 'AuthConfig';
    /** Whether token-based auth is enabled. */
    tokenAuthEnabled: Scalars['Boolean'];
};

/** Information about the currently authenticated user */
export type AuthenticatedUser = {
    __typename?: 'AuthenticatedUser';
    /** The user information associated with the authenticated user, including properties used in rendering the profile */
    corpUser: CorpUser;
    /** The privileges assigned to the currently authenticated user, which dictates which parts of the UI they should be able to use */
    platformPrivileges: PlatformPrivileges;
};

/** Input for performing an auto completion query against a single Metadata Entity */
export type AutoCompleteInput = {
    /** Entity type to be autocompleted against */
    type?: Maybe<EntityType>;
    /** The raw query string */
    query: Scalars['String'];
    /** An optional entity field name to autocomplete on */
    field?: Maybe<Scalars['String']>;
    /** The maximum number of autocomplete results to be returned */
    limit?: Maybe<Scalars['Int']>;
    /** Faceted filters applied to autocomplete results */
    filters?: Maybe<Array<FacetFilterInput>>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
};

/** Input for performing an auto completion query against a a set of Metadata Entities */
export type AutoCompleteMultipleInput = {
    /**
     * Entity types to be autocompleted against
     * Optional, if none supplied, all searchable types will be autocompleted against
     */
    types?: Maybe<Array<EntityType>>;
    /** The raw query string */
    query: Scalars['String'];
    /** An optional field to autocomplete against */
    field?: Maybe<Scalars['String']>;
    /** The maximum number of autocomplete results */
    limit?: Maybe<Scalars['Int']>;
    /** Faceted filters applied to autocomplete results */
    filters?: Maybe<Array<FacetFilterInput>>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
    /** Optional - A View to apply when generating results */
    viewUrn?: Maybe<Scalars['String']>;
};

/** The results returned on a multi entity autocomplete query */
export type AutoCompleteMultipleResults = {
    __typename?: 'AutoCompleteMultipleResults';
    /** The raw query string */
    query: Scalars['String'];
    /** The autocompletion suggestions */
    suggestions: Array<AutoCompleteResultForEntity>;
};

/** An individual auto complete result specific to an individual Metadata Entity Type */
export type AutoCompleteResultForEntity = {
    __typename?: 'AutoCompleteResultForEntity';
    /** Entity type */
    type: EntityType;
    /** The autocompletion results for specified entity type */
    suggestions: Array<Scalars['String']>;
    /** A list of entities to render in autocomplete */
    entities: Array<Entity>;
};

/** The results returned on a single entity autocomplete query */
export type AutoCompleteResults = {
    __typename?: 'AutoCompleteResults';
    /** The query string */
    query: Scalars['String'];
    /** The autocompletion results */
    suggestions: Array<Scalars['String']>;
    /** A list of entities to render in autocomplete */
    entities: Array<Entity>;
};

/** For consumption by UI only */
export type BarChart = {
    __typename?: 'BarChart';
    title: Scalars['String'];
    bars: Array<NamedBar>;
};

/** For consumption by UI only */
export type BarSegment = {
    __typename?: 'BarSegment';
    label: Scalars['String'];
    value: Scalars['Int'];
};

export type BaseData = {
    __typename?: 'BaseData';
    /** Dataset used for the Training or Evaluation of the MLModel */
    dataset: Scalars['String'];
    /** Motivation to pick these datasets */
    motivation?: Maybe<Scalars['String']>;
    /** Details of Data Proprocessing */
    preProcessing?: Maybe<Array<Scalars['String']>>;
};

/** Input provided when adding owners to a batch of assets */
export type BatchAddOwnersInput = {
    /** The primary key of the owners */
    owners: Array<OwnerInput>;
    /** The target assets to attach the owners to */
    resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when adding tags to a batch of assets */
export type BatchAddTagsInput = {
    /** The primary key of the Tags */
    tagUrns: Array<Scalars['String']>;
    /** The target assets to attach the tags to */
    resources: Array<ResourceRefInput>;
};

/** Input provided when adding glossary terms to a batch of assets */
export type BatchAddTermsInput = {
    /** The primary key of the Glossary Terms */
    termUrns: Array<Scalars['String']>;
    /** The target assets to attach the glossary terms to */
    resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when batch assigning a role to a list of users */
export type BatchAssignRoleInput = {
    /** The urn of the role to assign to the actors. If undefined, will remove the role. */
    roleUrn?: Maybe<Scalars['String']>;
    /** The urns of the actors to assign the role to */
    actors: Array<Scalars['String']>;
};

/** Arguments provided to batch update Dataset entities */
export type BatchDatasetUpdateInput = {
    /** Primary key of the Dataset to which the update will be applied */
    urn: Scalars['String'];
    /** Arguments provided to update the Dataset */
    update: DatasetUpdateInput;
};

/** Input arguments required for fetching step states */
export type BatchGetStepStatesInput = {
    /** The unique ids for the steps to retrieve */
    ids: Array<Scalars['String']>;
};

/** Result returned when fetching step state */
export type BatchGetStepStatesResult = {
    __typename?: 'BatchGetStepStatesResult';
    /** The step states */
    results: Array<StepStateResult>;
};

/** Input provided when removing owners from a batch of assets */
export type BatchRemoveOwnersInput = {
    /** The primary key of the owners */
    ownerUrns: Array<Scalars['String']>;
    /** The target assets to remove the owners from */
    resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when removing tags from a batch of assets */
export type BatchRemoveTagsInput = {
    /** The primary key of the Tags */
    tagUrns: Array<Scalars['String']>;
    /** The target assets to remove the tags from */
    resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when removing glossary terms from a batch of assets */
export type BatchRemoveTermsInput = {
    /** The primary key of the Glossary Terms */
    termUrns: Array<Scalars['String']>;
    /** The target assets to remove the glossary terms from */
    resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when adding tags to a batch of assets */
export type BatchSetDomainInput = {
    /** The primary key of the Domain, or null if the domain will be unset */
    domainUrn?: Maybe<Scalars['String']>;
    /** The target assets to attach the Domain */
    resources: Array<ResourceRefInput>;
};

export type BatchSpec = {
    __typename?: 'BatchSpec';
    /** The native identifier as specified by the system operating on the batch. */
    nativeBatchId?: Maybe<Scalars['String']>;
    /** A query that identifies a batch of data */
    query?: Maybe<Scalars['String']>;
    /** Any limit to the number of rows in the batch, if applied */
    limit?: Maybe<Scalars['Int']>;
    /** Custom properties of the Batch */
    customProperties?: Maybe<Array<StringMapEntry>>;
};

/** Input provided when updating the deprecation status for a batch of assets. */
export type BatchUpdateDeprecationInput = {
    /** Whether the Entity is marked as deprecated. */
    deprecated: Scalars['Boolean'];
    /** Optional - The time user plan to decommission this entity */
    decommissionTime?: Maybe<Scalars['Long']>;
    /** Optional - Additional information about the entity deprecation plan */
    note?: Maybe<Scalars['String']>;
    /** The target assets to attach the tags to */
    resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when updating the soft-deleted status for a batch of assets */
export type BatchUpdateSoftDeletedInput = {
    /** The urns of the assets to soft delete */
    urns: Array<Scalars['String']>;
    /** Whether to mark the asset as soft-deleted (hidden) */
    deleted: Scalars['Boolean'];
};

/** Input arguments required for updating step states */
export type BatchUpdateStepStatesInput = {
    /** Set of step states. If the id does not exist, it will be created. */
    states: Array<StepStateInput>;
};

/** Result returned when fetching step state */
export type BatchUpdateStepStatesResult = {
    __typename?: 'BatchUpdateStepStatesResult';
    /** Results for each step */
    results: Array<UpdateStepStateResult>;
};

export type BooleanBox = {
    __typename?: 'BooleanBox';
    booleanValue: Scalars['Boolean'];
};

/** A Metadata Entity which is browsable, or has browse paths. */
export type BrowsableEntity = {
    /** The browse paths corresponding to an entity. If no Browse Paths have been generated before, this will be null. */
    browsePaths?: Maybe<Array<BrowsePath>>;
};

/** Input required for browse queries */
export type BrowseInput = {
    /** The browse entity type */
    type: EntityType;
    /** The browse path */
    path?: Maybe<Array<Scalars['String']>>;
    /** The starting point of paginated results */
    start?: Maybe<Scalars['Int']>;
    /** The number of elements included in the results */
    count?: Maybe<Scalars['Int']>;
    /**
     * Deprecated in favor of the more expressive orFilters field
     * Facet filters to apply to search results. These will be 'AND'-ed together.
     */
    filters?: Maybe<Array<FacetFilterInput>>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
};

/** A hierarchical entity path */
export type BrowsePath = {
    __typename?: 'BrowsePath';
    /** The components of the browse path */
    path: Array<Scalars['String']>;
};

/** Inputs for fetching the browse paths for a Metadata Entity */
export type BrowsePathsInput = {
    /** The browse entity type */
    type: EntityType;
    /** The entity urn */
    urn: Scalars['String'];
};

/** A group of Entities under a given browse path */
export type BrowseResultGroup = {
    __typename?: 'BrowseResultGroup';
    /** The path name of a group of browse results */
    name: Scalars['String'];
    /** The number of entities within the group */
    count: Scalars['Long'];
};

/** Metadata about the Browse Paths response */
export type BrowseResultMetadata = {
    __typename?: 'BrowseResultMetadata';
    /** The provided path */
    path: Array<Scalars['String']>;
    /** The total number of entities under the provided browse path */
    totalNumEntities: Scalars['Long'];
};

/** The results of a browse path traversal query */
export type BrowseResults = {
    __typename?: 'BrowseResults';
    /** The browse results */
    entities: Array<Entity>;
    /** The groups present at the provided browse path */
    groups: Array<BrowseResultGroup>;
    /** The starting point of paginated results */
    start: Scalars['Int'];
    /** The number of elements included in the results */
    count: Scalars['Int'];
    /** The total number of browse results under the path with filters applied */
    total: Scalars['Int'];
    /** Metadata containing resulting browse groups */
    metadata: BrowseResultMetadata;
};

/** Input for cancelling an execution request input */
export type CancelIngestionExecutionRequestInput = {
    /** Urn of the ingestion source */
    ingestionSourceUrn: Scalars['String'];
    /** Urn of the specific execution request to cancel */
    executionRequestUrn: Scalars['String'];
};

export type CaveatDetails = {
    __typename?: 'CaveatDetails';
    /** Did the results suggest any further testing */
    needsFurtherTesting?: Maybe<Scalars['Boolean']>;
    /** Caveat Description */
    caveatDescription?: Maybe<Scalars['String']>;
    /** Relevant groups that were not represented in the evaluation dataset */
    groupsNotRepresented?: Maybe<Array<Scalars['String']>>;
};

export type CaveatsAndRecommendations = {
    __typename?: 'CaveatsAndRecommendations';
    /** Caveats on using this MLModel */
    caveats?: Maybe<CaveatDetails>;
    /** Recommendations on where this MLModel should be used */
    recommendations?: Maybe<Scalars['String']>;
    /** Ideal characteristics of an evaluation dataset for this MLModel */
    idealDatasetCharacteristics?: Maybe<Array<Scalars['String']>>;
};

/** For consumption by UI only */
export type Cell = {
    __typename?: 'Cell';
    value: Scalars['String'];
    entity?: Maybe<Entity>;
    linkParams?: Maybe<LinkParams>;
};

/** Captures information about who created/last modified/deleted the entity and when */
export type ChangeAuditStamps = {
    __typename?: 'ChangeAuditStamps';
    /** An AuditStamp corresponding to the creation */
    created: AuditStamp;
    /** An AuditStamp corresponding to the modification */
    lastModified: AuditStamp;
    /** An optional AuditStamp corresponding to the deletion */
    deleted?: Maybe<AuditStamp>;
};

/** Enum of CategoryTypes */
export enum ChangeCategoryType {
    /** When documentation has been edited */
    Documentation = 'DOCUMENTATION',
    /** When glossary terms have been added or removed */
    GlossaryTerm = 'GLOSSARY_TERM',
    /** When ownership has been modified */
    Ownership = 'OWNERSHIP',
    /** When technical schemas have been added or removed */
    TechnicalSchema = 'TECHNICAL_SCHEMA',
    /** When tags have been added or removed */
    Tag = 'TAG',
}

/** Enum of types of changes */
export enum ChangeOperationType {
    /** When an element is added */
    Add = 'ADD',
    /** When an element is modified */
    Modify = 'MODIFY',
    /** When an element is removed */
    Remove = 'REMOVE',
}

/** A Chart Metadata Entity */
export type Chart = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'Chart';
        /** The primary key of the Chart */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** The parent container in which the entity resides */
        container?: Maybe<Container>;
        /** Recursively get the lineage of containers for this entity */
        parentContainers?: Maybe<ParentContainersResult>;
        /**
         * The chart tool name
         * Note that this field will soon be deprecated in favor a unified notion of Data Platform
         */
        tool: Scalars['String'];
        /** An id unique within the charting tool */
        chartId: Scalars['String'];
        /** Additional read only properties about the Chart */
        properties?: Maybe<ChartProperties>;
        /** Additional read write properties about the Chart */
        editableProperties?: Maybe<ChartEditableProperties>;
        /** Info about the query which is used to render the chart */
        query?: Maybe<ChartQuery>;
        /** Ownership metadata of the chart */
        ownership?: Maybe<Ownership>;
        /** Status metadata of the chart */
        status?: Maybe<Status>;
        /** The deprecation status of the chart */
        deprecation?: Maybe<Deprecation>;
        /** Embed information about the Chart */
        embed?: Maybe<Embed>;
        /** The tags associated with the chart */
        tags?: Maybe<GlobalTags>;
        /** References to internal resources related to the dashboard */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** The structured glossary terms associated with the dashboard */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the Chart */
        domain?: Maybe<DomainAssociation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /**
         * Not yet implemented.
         *
         * Experimental - Summary operational & usage statistics about a Chart
         */
        statsSummary?: Maybe<ChartStatsSummary>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the chart. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /**
         * Deprecated, use properties field instead
         * Additional read only information about the chart
         * @deprecated Field no longer supported
         */
        info?: Maybe<ChartInfo>;
        /**
         * Deprecated, use editableProperties field instead
         * Additional read write information about the Chart
         * @deprecated Field no longer supported
         */
        editableInfo?: Maybe<ChartEditableProperties>;
        /**
         * Deprecated, use tags instead
         * The structured tags associated with the chart
         * @deprecated Field no longer supported
         */
        globalTags?: Maybe<GlobalTags>;
        /** Standardized platform urn where the chart is defined */
        platform: DataPlatform;
        /** Input fields to power the chart */
        inputFields?: Maybe<InputFields>;
        /** Privileges given to a user relevant to this entity */
        privileges?: Maybe<EntityPrivileges>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** A Chart Metadata Entity */
export type ChartRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A Chart Metadata Entity */
export type ChartLineageArgs = {
    input: LineageInput;
};

/** A Notebook cell which contains chart as content */
export type ChartCell = {
    __typename?: 'ChartCell';
    /** Title of the cell */
    cellTitle: Scalars['String'];
    /** Unique id for the cell. */
    cellId: Scalars['String'];
    /** Captures information about who created/last modified/deleted this TextCell and when */
    changeAuditStamps?: Maybe<ChangeAuditStamps>;
};

/**
 * Chart properties that are editable via the UI This represents logical metadata,
 * as opposed to technical metadata
 */
export type ChartEditableProperties = {
    __typename?: 'ChartEditableProperties';
    /** Description of the Chart */
    description?: Maybe<Scalars['String']>;
};

/** Update to writable Chart fields */
export type ChartEditablePropertiesUpdate = {
    /** Writable description aka documentation for a Chart */
    description: Scalars['String'];
};

/**
 * Deprecated, use ChartProperties instead
 * Additional read only information about the chart
 */
export type ChartInfo = {
    __typename?: 'ChartInfo';
    /** Display name of the chart */
    name: Scalars['String'];
    /** Description of the chart */
    description?: Maybe<Scalars['String']>;
    /**
     * Deprecated, use relationship Consumes instead
     * Data sources for the chart
     * @deprecated Field no longer supported
     */
    inputs?: Maybe<Array<Dataset>>;
    /** Native platform URL of the chart */
    externalUrl?: Maybe<Scalars['String']>;
    /** Access level for the chart */
    type?: Maybe<ChartType>;
    /** Access level for the chart */
    access?: Maybe<AccessLevel>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** The time when this chart last refreshed */
    lastRefreshed?: Maybe<Scalars['Long']>;
    /** An AuditStamp corresponding to the creation of this chart */
    created: AuditStamp;
    /** An AuditStamp corresponding to the modification of this chart */
    lastModified: AuditStamp;
    /** An optional AuditStamp corresponding to the deletion of this chart */
    deleted?: Maybe<AuditStamp>;
};

/** Additional read only properties about the chart */
export type ChartProperties = {
    __typename?: 'ChartProperties';
    /** Display name of the chart */
    name: Scalars['String'];
    /** Description of the chart */
    description?: Maybe<Scalars['String']>;
    /** Native platform URL of the chart */
    externalUrl?: Maybe<Scalars['String']>;
    /** Access level for the chart */
    type?: Maybe<ChartType>;
    /** Access level for the chart */
    access?: Maybe<AccessLevel>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** The time when this chart last refreshed */
    lastRefreshed?: Maybe<Scalars['Long']>;
    /** An AuditStamp corresponding to the creation of this chart */
    created: AuditStamp;
    /** An AuditStamp corresponding to the modification of this chart */
    lastModified: AuditStamp;
    /** An optional AuditStamp corresponding to the deletion of this chart */
    deleted?: Maybe<AuditStamp>;
};

/** The query that was used to populate a Chart */
export type ChartQuery = {
    __typename?: 'ChartQuery';
    /** Raw query to build a chart from input datasets */
    rawQuery: Scalars['String'];
    /** The type of the chart query */
    type: ChartQueryType;
};

/** The type of the Chart Query */
export enum ChartQueryType {
    /** Standard ANSI SQL */
    Sql = 'SQL',
    /** LookML */
    Lookml = 'LOOKML',
}

/** Experimental - subject to change. A summary of usage metrics about a Chart. */
export type ChartStatsSummary = {
    __typename?: 'ChartStatsSummary';
    /** The total view count for the chart */
    viewCount?: Maybe<Scalars['Int']>;
    /** The view count in the last 30 days */
    viewCountLast30Days?: Maybe<Scalars['Int']>;
    /** The unique user count in the past 30 days */
    uniqueUserCountLast30Days?: Maybe<Scalars['Int']>;
    /** The top users in the past 30 days */
    topUsersLast30Days?: Maybe<Array<CorpUser>>;
};

/** The type of a Chart Entity */
export enum ChartType {
    /** Bar graph */
    Bar = 'BAR',
    /** Pie chart */
    Pie = 'PIE',
    /** Scatter plot */
    Scatter = 'SCATTER',
    /** Table */
    Table = 'TABLE',
    /** Markdown formatted text */
    Text = 'TEXT',
    /** A line chart */
    Line = 'LINE',
    /** An area chart */
    Area = 'AREA',
    /** A histogram chart */
    Histogram = 'HISTOGRAM',
    /** A box plot chart */
    BoxPlot = 'BOX_PLOT',
    /** A word cloud chart */
    WordCloud = 'WORD_CLOUD',
    /** A Cohort Analysis chart */
    Cohort = 'COHORT',
}

/** Arguments provided to update a Chart Entity */
export type ChartUpdateInput = {
    /** Update to ownership */
    ownership?: Maybe<OwnershipUpdate>;
    /**
     * Deprecated, use tags field instead
     * Update to global tags
     */
    globalTags?: Maybe<GlobalTagsUpdate>;
    /** Update to tags */
    tags?: Maybe<GlobalTagsUpdate>;
    /** Update to editable properties */
    editableProperties?: Maybe<ChartEditablePropertiesUpdate>;
};

/** A container of other Metadata Entities */
export type Container = Entity & {
    __typename?: 'Container';
    /** The primary key of the container */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** The timestamp for the last time this entity was ingested */
    lastIngested?: Maybe<Scalars['Long']>;
    /** Standardized platform. */
    platform: DataPlatform;
    /** Fetch an Entity Container by primary key (urn) */
    container?: Maybe<Container>;
    /** Recursively get the lineage of containers for this entity */
    parentContainers?: Maybe<ParentContainersResult>;
    /** Read-only properties that originate in the source data platform */
    properties?: Maybe<ContainerProperties>;
    /** Read-write properties that originate in DataHub */
    editableProperties?: Maybe<ContainerEditableProperties>;
    /** Ownership metadata of the dataset */
    ownership?: Maybe<Ownership>;
    /** References to internal resources related to the dataset */
    institutionalMemory?: Maybe<InstitutionalMemory>;
    /** Tags used for searching dataset */
    tags?: Maybe<GlobalTags>;
    /** The structured glossary terms associated with the dataset */
    glossaryTerms?: Maybe<GlossaryTerms>;
    /** Sub types of the container, e.g. "Database" etc */
    subTypes?: Maybe<SubTypes>;
    /** The Domain associated with the Dataset */
    domain?: Maybe<DomainAssociation>;
    /** The deprecation status of the container */
    deprecation?: Maybe<Deprecation>;
    /** The specific instance of the data platform that this entity belongs to */
    dataPlatformInstance?: Maybe<DataPlatformInstance>;
    /** Children entities inside of the Container */
    entities?: Maybe<SearchResults>;
    /** Edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Status metadata of the container */
    status?: Maybe<Status>;
    /** Whether or not this entity exists on DataHub */
    exists?: Maybe<Scalars['Boolean']>;
};

/** A container of other Metadata Entities */
export type ContainerEntitiesArgs = {
    input?: Maybe<ContainerEntitiesInput>;
};

/** A container of other Metadata Entities */
export type ContainerRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Read-write properties that originate in DataHub */
export type ContainerEditableProperties = {
    __typename?: 'ContainerEditableProperties';
    /** DataHub description of the Container */
    description?: Maybe<Scalars['String']>;
};

/** Input required to fetch the entities inside of a container. */
export type ContainerEntitiesInput = {
    /** Optional query filter for particular entities inside the container */
    query?: Maybe<Scalars['String']>;
    /** The offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of entities to include in result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional Facet filters to apply to the result set */
    filters?: Maybe<Array<FacetFilterInput>>;
};

/** Read-only properties that originate in the source data platform */
export type ContainerProperties = {
    __typename?: 'ContainerProperties';
    /** Display name of the Container */
    name: Scalars['String'];
    /** System description of the Container */
    description?: Maybe<Scalars['String']>;
    /** Custom properties of the Container */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** Native platform URL of the Container */
    externalUrl?: Maybe<Scalars['String']>;
    /** Fully-qualified name of the Container */
    qualifiedName?: Maybe<Scalars['String']>;
};

/** Params about the recommended content */
export type ContentParams = {
    __typename?: 'ContentParams';
    /** Number of entities corresponding to the recommended content */
    count?: Maybe<Scalars['Long']>;
};

/** A DataHub Group entity, which represents a Person on the Metadata Entity Graph */
export type CorpGroup = Entity & {
    __typename?: 'CorpGroup';
    /** The primary key of the group */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** Group name eg wherehows dev, ask_metadata */
    name: Scalars['String'];
    /** Ownership metadata of the Corp Group */
    ownership?: Maybe<Ownership>;
    /** Additional read only properties about the group */
    properties?: Maybe<CorpGroupProperties>;
    /** Additional read write properties about the group */
    editableProperties?: Maybe<CorpGroupEditableProperties>;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Origin info about this group. */
    origin?: Maybe<Origin>;
    /**
     * Deprecated, use properties field instead
     * Additional read only info about the group
     * @deprecated Field no longer supported
     */
    info?: Maybe<CorpGroupInfo>;
};

/** A DataHub Group entity, which represents a Person on the Metadata Entity Graph */
export type CorpGroupRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Additional read write properties about a group */
export type CorpGroupEditableProperties = {
    __typename?: 'CorpGroupEditableProperties';
    /** DataHub description of the group */
    description?: Maybe<Scalars['String']>;
    /** Slack handle for the group */
    slack?: Maybe<Scalars['String']>;
    /** Email address for the group */
    email?: Maybe<Scalars['String']>;
};

/**
 * Deprecated, use CorpUserProperties instead
 * Additional read only info about a group
 */
export type CorpGroupInfo = {
    __typename?: 'CorpGroupInfo';
    /** The name to display when rendering the group */
    displayName?: Maybe<Scalars['String']>;
    /** The description provided for the group */
    description?: Maybe<Scalars['String']>;
    /** email of this group */
    email?: Maybe<Scalars['String']>;
    /**
     * Deprecated, do not use
     * owners of this group
     * @deprecated Field no longer supported
     */
    admins?: Maybe<Array<CorpUser>>;
    /**
     * Deprecated, use relationship IsMemberOfGroup instead
     * List of ldap urn in this group
     * @deprecated Field no longer supported
     */
    members?: Maybe<Array<CorpUser>>;
    /**
     * Deprecated, do not use
     * List of groups urns in this group
     * @deprecated Field no longer supported
     */
    groups?: Maybe<Array<Scalars['String']>>;
};

/** Additional read only properties about a group */
export type CorpGroupProperties = {
    __typename?: 'CorpGroupProperties';
    /** display name of this group */
    displayName?: Maybe<Scalars['String']>;
    /** The description provided for the group */
    description?: Maybe<Scalars['String']>;
    /** email of this group */
    email?: Maybe<Scalars['String']>;
    /** Slack handle for the group */
    slack?: Maybe<Scalars['String']>;
};

/** Arguments provided to update a CorpGroup Entity */
export type CorpGroupUpdateInput = {
    /** DataHub description of the group */
    description?: Maybe<Scalars['String']>;
    /** Slack handle for the group */
    slack?: Maybe<Scalars['String']>;
    /** Email address for the group */
    email?: Maybe<Scalars['String']>;
};

/** A DataHub User entity, which represents a Person on the Metadata Entity Graph */
export type CorpUser = Entity & {
    __typename?: 'CorpUser';
    /** The primary key of the user */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /**
     * A username associated with the user
     * This uniquely identifies the user within DataHub
     */
    username: Scalars['String'];
    /** Additional read only properties about the corp user */
    properties?: Maybe<CorpUserProperties>;
    /** Read write properties about the corp user */
    editableProperties?: Maybe<CorpUserEditableProperties>;
    /** The status of the user */
    status?: Maybe<CorpUserStatus>;
    /** The tags associated with the user */
    tags?: Maybe<GlobalTags>;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Whether or not this user is a native DataHub user */
    isNativeUser?: Maybe<Scalars['Boolean']>;
    /**
     * Deprecated, use properties field instead
     * Additional read only info about the corp user
     * @deprecated Field no longer supported
     */
    info?: Maybe<CorpUserInfo>;
    /**
     * Deprecated, use editableProperties field instead
     * Read write info about the corp user
     * @deprecated Field no longer supported
     */
    editableInfo?: Maybe<CorpUserEditableInfo>;
    /**
     * Deprecated, use the tags field instead
     * The structured tags associated with the user
     * @deprecated Field no longer supported
     */
    globalTags?: Maybe<GlobalTags>;
    /** Settings that a user can customize through the datahub ui */
    settings?: Maybe<CorpUserSettings>;
};

/** A DataHub User entity, which represents a Person on the Metadata Entity Graph */
export type CorpUserRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Settings that control look and feel of the DataHub UI for the user */
export type CorpUserAppearanceSettings = {
    __typename?: 'CorpUserAppearanceSettings';
    /**
     * Flag whether the user should see a homepage with only datasets, charts & dashboards. Intended for users
     * who have less operational use cases for the datahub tool.
     */
    showSimplifiedHomepage?: Maybe<Scalars['Boolean']>;
};

/**
 * Deprecated, use CorpUserEditableProperties instead
 * Additional read write info about a user
 */
export type CorpUserEditableInfo = {
    __typename?: 'CorpUserEditableInfo';
    /** Display name to show on DataHub */
    displayName?: Maybe<Scalars['String']>;
    /** Title to show on DataHub */
    title?: Maybe<Scalars['String']>;
    /** About me section of the user */
    aboutMe?: Maybe<Scalars['String']>;
    /** Teams that the user belongs to */
    teams?: Maybe<Array<Scalars['String']>>;
    /** Skills that the user possesses */
    skills?: Maybe<Array<Scalars['String']>>;
    /** A URL which points to a picture which user wants to set as a profile photo */
    pictureLink?: Maybe<Scalars['String']>;
};

/** Additional read write properties about a user */
export type CorpUserEditableProperties = {
    __typename?: 'CorpUserEditableProperties';
    /** Display name to show on DataHub */
    displayName?: Maybe<Scalars['String']>;
    /** Title to show on DataHub */
    title?: Maybe<Scalars['String']>;
    /** About me section of the user */
    aboutMe?: Maybe<Scalars['String']>;
    /** Teams that the user belongs to */
    teams?: Maybe<Array<Scalars['String']>>;
    /** Skills that the user possesses */
    skills?: Maybe<Array<Scalars['String']>>;
    /** A URL which points to a picture which user wants to set as a profile photo */
    pictureLink?: Maybe<Scalars['String']>;
    /** The slack handle of the user */
    slack?: Maybe<Scalars['String']>;
    /** Phone number for the user */
    phone?: Maybe<Scalars['String']>;
    /** Email address for the user */
    email?: Maybe<Scalars['String']>;
};

/**
 * Deprecated, use CorpUserProperties instead
 * Additional read only info about a user
 */
export type CorpUserInfo = {
    __typename?: 'CorpUserInfo';
    /** Whether the user is active */
    active: Scalars['Boolean'];
    /** Display name of the user */
    displayName?: Maybe<Scalars['String']>;
    /** Email address of the user */
    email?: Maybe<Scalars['String']>;
    /** Title of the user */
    title?: Maybe<Scalars['String']>;
    /** Direct manager of the user */
    manager?: Maybe<CorpUser>;
    /** department id the user belong to */
    departmentId?: Maybe<Scalars['Long']>;
    /** department name this user belong to */
    departmentName?: Maybe<Scalars['String']>;
    /** first name of the user */
    firstName?: Maybe<Scalars['String']>;
    /** last name of the user */
    lastName?: Maybe<Scalars['String']>;
    /** Common name of this user, format is firstName plus lastName */
    fullName?: Maybe<Scalars['String']>;
    /** two uppercase letters country code */
    countryCode?: Maybe<Scalars['String']>;
    /** Custom properties of the ldap */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

/** Additional read only properties about a user */
export type CorpUserProperties = {
    __typename?: 'CorpUserProperties';
    /** Whether the user is active */
    active: Scalars['Boolean'];
    /** Display name of the user */
    displayName?: Maybe<Scalars['String']>;
    /** Email address of the user */
    email?: Maybe<Scalars['String']>;
    /** Title of the user */
    title?: Maybe<Scalars['String']>;
    /** Direct manager of the user */
    manager?: Maybe<CorpUser>;
    /** department id the user belong to */
    departmentId?: Maybe<Scalars['Long']>;
    /** department name this user belong to */
    departmentName?: Maybe<Scalars['String']>;
    /** first name of the user */
    firstName?: Maybe<Scalars['String']>;
    /** last name of the user */
    lastName?: Maybe<Scalars['String']>;
    /** Common name of this user, format is firstName plus lastName */
    fullName?: Maybe<Scalars['String']>;
    /** two uppercase letters country code */
    countryCode?: Maybe<Scalars['String']>;
    /** Custom properties of the ldap */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

/** Settings that a user can customize through the datahub ui */
export type CorpUserSettings = {
    __typename?: 'CorpUserSettings';
    /** Settings that control look and feel of the DataHub UI for the user */
    appearance?: Maybe<CorpUserAppearanceSettings>;
    /** Settings related to the DataHub Views feature */
    views?: Maybe<CorpUserViewsSettings>;
};

/** The state of a CorpUser */
export enum CorpUserStatus {
    /** A User that has been provisioned and logged in */
    Active = 'ACTIVE',
}

/** Arguments provided to update a CorpUser Entity */
export type CorpUserUpdateInput = {
    /** Display name to show on DataHub */
    displayName?: Maybe<Scalars['String']>;
    /** Title to show on DataHub */
    title?: Maybe<Scalars['String']>;
    /** About me section of the user */
    aboutMe?: Maybe<Scalars['String']>;
    /** Teams that the user belongs to */
    teams?: Maybe<Array<Scalars['String']>>;
    /** Skills that the user possesses */
    skills?: Maybe<Array<Scalars['String']>>;
    /** A URL which points to a picture which user wants to set as a profile photo */
    pictureLink?: Maybe<Scalars['String']>;
    /** The slack handle of the user */
    slack?: Maybe<Scalars['String']>;
    /** Phone number for the user */
    phone?: Maybe<Scalars['String']>;
    /** Email address for the user */
    email?: Maybe<Scalars['String']>;
};

/** Settings related to the Views feature of DataHub. */
export type CorpUserViewsSettings = {
    __typename?: 'CorpUserViewsSettings';
    /** The default view for the User. */
    defaultView?: Maybe<DataHubView>;
};

export type Cost = {
    __typename?: 'Cost';
    /** Type of Cost Code */
    costType: CostType;
    /** Code to which the Cost of this entity should be attributed to ie organizational cost ID */
    costValue: CostValue;
};

export enum CostType {
    /** Org Cost Type to which the Cost of this entity should be attributed to */
    OrgCostType = 'ORG_COST_TYPE',
}

export type CostValue = {
    __typename?: 'CostValue';
    /** Organizational Cost ID */
    costId?: Maybe<Scalars['Float']>;
    /** Organizational Cost Code */
    costCode?: Maybe<Scalars['String']>;
};

export type CreateAccessTokenInput = {
    /** The type of the Access Token. */
    type: AccessTokenType;
    /** The actor associated with the Access Token. */
    actorUrn: Scalars['String'];
    /** The duration for which the Access Token is valid. */
    duration: AccessTokenDuration;
    /** The name of the token to be generated. */
    name: Scalars['String'];
    /** Description of the token if defined. */
    description?: Maybe<Scalars['String']>;
};

/** Input required to create a new Domain. */
export type CreateDomainInput = {
    /** Optional! A custom id to use as the primary key identifier for the domain. If not provided, a random UUID will be generated as the id. */
    id?: Maybe<Scalars['String']>;
    /** Display name for the Domain */
    name: Scalars['String'];
    /** Optional description for the Domain */
    description?: Maybe<Scalars['String']>;
};

/** Input required to create a new Glossary Entity - a Node or a Term. */
export type CreateGlossaryEntityInput = {
    /** Optional! A custom id to use as the primary key identifier for the domain. If not provided, a random UUID will be generated as the id. */
    id?: Maybe<Scalars['String']>;
    /** Display name for the Node or Term */
    name: Scalars['String'];
    /** Description for the Node or Term */
    description?: Maybe<Scalars['String']>;
    /** Optional parent node urn for the Glossary Node or Term */
    parentNode?: Maybe<Scalars['String']>;
};

/** Input for creating a new group */
export type CreateGroupInput = {
    /** Optional! A custom id to use as the primary key identifier for the group. If not provided, a random UUID will be generated as the id. */
    id?: Maybe<Scalars['String']>;
    /** The display name of the group */
    name: Scalars['String'];
    /** The description of the group */
    description?: Maybe<Scalars['String']>;
};

/** Input for creating an execution request input */
export type CreateIngestionExecutionRequestInput = {
    /** Urn of the ingestion source to execute */
    ingestionSourceUrn: Scalars['String'];
};

/** Input provided when creating an invite token */
export type CreateInviteTokenInput = {
    /** The urn of the role to create the invite token for */
    roleUrn?: Maybe<Scalars['String']>;
};

/** Input required to generate a password reset token for a native user. */
export type CreateNativeUserResetTokenInput = {
    /** The urn of the user to reset the password of */
    userUrn: Scalars['String'];
};

/** Input provided when creating a Post */
export type CreatePostInput = {
    /** The type of post */
    postType: PostType;
    /** The content of the post */
    content: UpdatePostContentInput;
};

/** Input required for creating a Query. Requires the 'Edit Queries' privilege for all query subjects. */
export type CreateQueryInput = {
    /** Properties about the Query */
    properties: CreateQueryPropertiesInput;
    /** Subjects for the query */
    subjects: Array<CreateQuerySubjectInput>;
};

/** Input properties required for creating a Query */
export type CreateQueryPropertiesInput = {
    /** An optional display name for the Query */
    name?: Maybe<Scalars['String']>;
    /** An optional description for the Query */
    description?: Maybe<Scalars['String']>;
    /** The Query contents */
    statement: QueryStatementInput;
};

/** Input required for creating a Query. For now, only datasets are supported. */
export type CreateQuerySubjectInput = {
    /** The urn of the dataset that is the subject of the query */
    datasetUrn: Scalars['String'];
};

/** Input arguments for creating a new Secret */
export type CreateSecretInput = {
    /** The name of the secret for reference in ingestion recipes */
    name: Scalars['String'];
    /** The value of the secret, to be encrypted and stored */
    value: Scalars['String'];
    /** An optional description for the secret */
    description?: Maybe<Scalars['String']>;
};

/** Input required to create a new Tag */
export type CreateTagInput = {
    /** Optional! A custom id to use as the primary key identifier for the Tag. If not provided, a random UUID will be generated as the id. */
    id?: Maybe<Scalars['String']>;
    /** Display name for the Tag */
    name: Scalars['String'];
    /** Optional description for the Tag */
    description?: Maybe<Scalars['String']>;
};

/** Input for creating a test connection request */
export type CreateTestConnectionRequestInput = {
    /** A JSON-encoded recipe */
    recipe: Scalars['String'];
    /** Advanced: The version of the ingestion framework to use */
    version?: Maybe<Scalars['String']>;
};

export type CreateTestInput = {
    /** Advanced: a custom id for the test. */
    id?: Maybe<Scalars['String']>;
    /** The name of the Test */
    name: Scalars['String'];
    /** The category of the Test (user defined) */
    category: Scalars['String'];
    /** Description of the test */
    description?: Maybe<Scalars['String']>;
    /** The test definition */
    definition: TestDefinitionInput;
};

/** Input provided when creating a DataHub View */
export type CreateViewInput = {
    /** The type of View */
    viewType: DataHubViewType;
    /** The name of the View */
    name: Scalars['String'];
    /** An optional description of the View */
    description?: Maybe<Scalars['String']>;
    /** The view definition itself */
    definition: DataHubViewDefinitionInput;
};

/** An entry in a custom properties map represented as a tuple */
export type CustomPropertiesEntry = {
    __typename?: 'CustomPropertiesEntry';
    /** The key of the map entry */
    key: Scalars['String'];
    /** The value fo the map entry */
    value?: Maybe<Scalars['String']>;
    /** The urn of the entity this property came from for tracking purposes e.g. when sibling nodes are merged together */
    associatedUrn: Scalars['String'];
};

/** A Dashboard Metadata Entity */
export type Dashboard = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'Dashboard';
        /** The primary key of the Dashboard */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** The parent container in which the entity resides */
        container?: Maybe<Container>;
        /** Recursively get the lineage of containers for this entity */
        parentContainers?: Maybe<ParentContainersResult>;
        /**
         * The dashboard tool name
         * Note that this will soon be deprecated in favor of a standardized notion of Data Platform
         */
        tool: Scalars['String'];
        /** An id unique within the dashboard tool */
        dashboardId: Scalars['String'];
        /** Additional read only properties about the dashboard */
        properties?: Maybe<DashboardProperties>;
        /** Additional read write properties about the dashboard */
        editableProperties?: Maybe<DashboardEditableProperties>;
        /** Ownership metadata of the dashboard */
        ownership?: Maybe<Ownership>;
        /** Status metadata of the dashboard */
        status?: Maybe<Status>;
        /** Embed information about the Dashboard */
        embed?: Maybe<Embed>;
        /** The deprecation status of the dashboard */
        deprecation?: Maybe<Deprecation>;
        /** The tags associated with the dashboard */
        tags?: Maybe<GlobalTags>;
        /** References to internal resources related to the dashboard */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** The structured glossary terms associated with the dashboard */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the Dashboard */
        domain?: Maybe<DomainAssociation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the dashboard. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /** Experimental (Subject to breaking change) -- Statistics about how this Dashboard is used */
        usageStats?: Maybe<DashboardUsageQueryResult>;
        /** Experimental - Summary operational & usage statistics about a Dashboard */
        statsSummary?: Maybe<DashboardStatsSummary>;
        /**
         * Deprecated, use properties field instead
         * Additional read only information about the dashboard
         * @deprecated Field no longer supported
         */
        info?: Maybe<DashboardInfo>;
        /**
         * Deprecated, use editableProperties instead
         * Additional read write properties about the Dashboard
         * @deprecated Field no longer supported
         */
        editableInfo?: Maybe<DashboardEditableProperties>;
        /**
         * Deprecated, use tags field instead
         * The structured tags associated with the dashboard
         * @deprecated Field no longer supported
         */
        globalTags?: Maybe<GlobalTags>;
        /** Standardized platform urn where the dashboard is defined */
        platform: DataPlatform;
        /** Input fields that power all the charts in the dashboard */
        inputFields?: Maybe<InputFields>;
        /** Sub Types of the dashboard */
        subTypes?: Maybe<SubTypes>;
        /** Privileges given to a user relevant to this entity */
        privileges?: Maybe<EntityPrivileges>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** A Dashboard Metadata Entity */
export type DashboardRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A Dashboard Metadata Entity */
export type DashboardLineageArgs = {
    input: LineageInput;
};

/** A Dashboard Metadata Entity */
export type DashboardUsageStatsArgs = {
    startTimeMillis?: Maybe<Scalars['Long']>;
    endTimeMillis?: Maybe<Scalars['Long']>;
    limit?: Maybe<Scalars['Int']>;
};

/**
 * Dashboard properties that are editable via the UI This represents logical metadata,
 * as opposed to technical metadata
 */
export type DashboardEditableProperties = {
    __typename?: 'DashboardEditableProperties';
    /** Description of the Dashboard */
    description?: Maybe<Scalars['String']>;
};

/** Update to writable Dashboard fields */
export type DashboardEditablePropertiesUpdate = {
    /** Writable description aka documentation for a Dashboard */
    description: Scalars['String'];
};

/**
 * Deprecated, use DashboardProperties instead
 * Additional read only info about a Dashboard
 */
export type DashboardInfo = {
    __typename?: 'DashboardInfo';
    /** Display of the dashboard */
    name: Scalars['String'];
    /** Description of the dashboard */
    description?: Maybe<Scalars['String']>;
    /**
     * Deprecated, use relationship Contains instead
     * Charts that comprise the dashboard
     * @deprecated Field no longer supported
     */
    charts: Array<Chart>;
    /** Native platform URL of the dashboard */
    externalUrl?: Maybe<Scalars['String']>;
    /**
     * Access level for the dashboard
     * Note that this will soon be deprecated for low usage
     */
    access?: Maybe<AccessLevel>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** The time when this dashboard last refreshed */
    lastRefreshed?: Maybe<Scalars['Long']>;
    /** An AuditStamp corresponding to the creation of this dashboard */
    created: AuditStamp;
    /** An AuditStamp corresponding to the modification of this dashboard */
    lastModified: AuditStamp;
    /** An optional AuditStamp corresponding to the deletion of this dashboard */
    deleted?: Maybe<AuditStamp>;
};

/** Additional read only properties about a Dashboard */
export type DashboardProperties = {
    __typename?: 'DashboardProperties';
    /** Display of the dashboard */
    name: Scalars['String'];
    /** Description of the dashboard */
    description?: Maybe<Scalars['String']>;
    /** Native platform URL of the dashboard */
    externalUrl?: Maybe<Scalars['String']>;
    /**
     * Access level for the dashboard
     * Note that this will soon be deprecated for low usage
     */
    access?: Maybe<AccessLevel>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** The time when this dashboard last refreshed */
    lastRefreshed?: Maybe<Scalars['Long']>;
    /** An AuditStamp corresponding to the creation of this dashboard */
    created: AuditStamp;
    /** An AuditStamp corresponding to the modification of this dashboard */
    lastModified: AuditStamp;
    /** An optional AuditStamp corresponding to the deletion of this dashboard */
    deleted?: Maybe<AuditStamp>;
};

/** Experimental - subject to change. A summary of usage metrics about a Dashboard. */
export type DashboardStatsSummary = {
    __typename?: 'DashboardStatsSummary';
    /** The total view count for the dashboard */
    viewCount?: Maybe<Scalars['Int']>;
    /** The view count in the last 30 days */
    viewCountLast30Days?: Maybe<Scalars['Int']>;
    /** The unique user count in the past 30 days */
    uniqueUserCountLast30Days?: Maybe<Scalars['Int']>;
    /** The top users in the past 30 days */
    topUsersLast30Days?: Maybe<Array<CorpUser>>;
};

/** Arguments provided to update a Dashboard Entity */
export type DashboardUpdateInput = {
    /** Update to ownership */
    ownership?: Maybe<OwnershipUpdate>;
    /**
     * Deprecated, use tags field instead
     * Update to global tags
     */
    globalTags?: Maybe<GlobalTagsUpdate>;
    /** Update to tags */
    tags?: Maybe<GlobalTagsUpdate>;
    /** Update to editable properties */
    editableProperties?: Maybe<DashboardEditablePropertiesUpdate>;
};

/** An aggregation of Dashboard usage statistics */
export type DashboardUsageAggregation = {
    __typename?: 'DashboardUsageAggregation';
    /** The time window start time */
    bucket?: Maybe<Scalars['Long']>;
    /** The time window span */
    duration?: Maybe<WindowDuration>;
    /** The resource urn associated with the usage information, eg a Dashboard urn */
    resource?: Maybe<Scalars['String']>;
    /** The rolled up usage metrics */
    metrics?: Maybe<DashboardUsageAggregationMetrics>;
};

/** Rolled up metrics about Dashboard usage over time */
export type DashboardUsageAggregationMetrics = {
    __typename?: 'DashboardUsageAggregationMetrics';
    /** The unique number of dashboard users within the time range */
    uniqueUserCount?: Maybe<Scalars['Int']>;
    /** The total number of dashboard views within the time range */
    viewsCount?: Maybe<Scalars['Int']>;
    /** The total number of dashboard executions within the time range */
    executionsCount?: Maybe<Scalars['Int']>;
};

/** A set of absolute dashboard usage metrics */
export type DashboardUsageMetrics = TimeSeriesAspect & {
    __typename?: 'DashboardUsageMetrics';
    /** The time at which the metrics were reported */
    timestampMillis: Scalars['Long'];
    /**
     * The total number of times dashboard has been favorited
     * FIXME: Qualifies as Popularity Metric rather than Usage Metric?
     */
    favoritesCount?: Maybe<Scalars['Int']>;
    /** The total number of dashboard views */
    viewsCount?: Maybe<Scalars['Int']>;
    /** The total number of dashboard execution */
    executionsCount?: Maybe<Scalars['Int']>;
    /** The time when this dashboard was last viewed */
    lastViewed?: Maybe<Scalars['Long']>;
};

/** The result of a dashboard usage query */
export type DashboardUsageQueryResult = {
    __typename?: 'DashboardUsageQueryResult';
    /** A set of relevant time windows for use in displaying usage statistics */
    buckets?: Maybe<Array<Maybe<DashboardUsageAggregation>>>;
    /** A set of rolled up aggregations about the dashboard usage */
    aggregations?: Maybe<DashboardUsageQueryResultAggregations>;
    /** A set of absolute dashboard usage metrics */
    metrics?: Maybe<Array<DashboardUsageMetrics>>;
};

/** A set of rolled up aggregations about the Dashboard usage */
export type DashboardUsageQueryResultAggregations = {
    __typename?: 'DashboardUsageQueryResultAggregations';
    /** The count of unique Dashboard users within the queried time range */
    uniqueUserCount?: Maybe<Scalars['Int']>;
    /** The specific per user usage counts within the queried time range */
    users?: Maybe<Array<Maybe<DashboardUserUsageCounts>>>;
    /** The total number of dashboard views within the queried time range */
    viewsCount?: Maybe<Scalars['Int']>;
    /** The total number of dashboard executions within the queried time range */
    executionsCount?: Maybe<Scalars['Int']>;
};

/** Information about individual user usage of a Dashboard */
export type DashboardUserUsageCounts = {
    __typename?: 'DashboardUserUsageCounts';
    /** The user of the Dashboard */
    user?: Maybe<CorpUser>;
    /** number of times dashboard has been viewed by the user */
    viewsCount?: Maybe<Scalars['Int']>;
    /** number of dashboard executions by the user */
    executionsCount?: Maybe<Scalars['Int']>;
    /**
     * Normalized numeric metric representing user's dashboard usage
     * Higher value represents more usage
     */
    usageCount?: Maybe<Scalars['Int']>;
};

/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlow = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'DataFlow';
        /** The primary key of a Data Flow */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** Workflow orchestrator ei Azkaban, Airflow */
        orchestrator: Scalars['String'];
        /** Id of the flow */
        flowId: Scalars['String'];
        /** Cluster of the flow */
        cluster: Scalars['String'];
        /** Additional read only properties about a Data flow */
        properties?: Maybe<DataFlowProperties>;
        /** Additional read write properties about a Data Flow */
        editableProperties?: Maybe<DataFlowEditableProperties>;
        /** Ownership metadata of the flow */
        ownership?: Maybe<Ownership>;
        /** The tags associated with the dataflow */
        tags?: Maybe<GlobalTags>;
        /** Status metadata of the dataflow */
        status?: Maybe<Status>;
        /** The deprecation status of the Data Flow */
        deprecation?: Maybe<Deprecation>;
        /** References to internal resources related to the dashboard */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** The structured glossary terms associated with the dashboard */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the DataFlow */
        domain?: Maybe<DomainAssociation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the data flow. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /**
         * Deprecated, use properties field instead
         * Additional read only information about a Data flow
         * @deprecated Field no longer supported
         */
        info?: Maybe<DataFlowInfo>;
        /**
         * Deprecated, use tags field instead
         * The structured tags associated with the dataflow
         * @deprecated Field no longer supported
         */
        globalTags?: Maybe<GlobalTags>;
        /**
         * Deprecated, use relationship IsPartOf instead
         * Data Jobs
         * @deprecated Field no longer supported
         */
        dataJobs?: Maybe<DataFlowDataJobsRelationships>;
        /** Standardized platform urn where the datflow is defined */
        platform: DataPlatform;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlowRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlowLineageArgs = {
    input: LineageInput;
};

/** Deprecated, use relationships query instead */
export type DataFlowDataJobsRelationships = {
    __typename?: 'DataFlowDataJobsRelationships';
    entities?: Maybe<Array<Maybe<EntityRelationshipLegacy>>>;
};

/**
 * Data Flow properties that are editable via the UI This represents logical metadata,
 * as opposed to technical metadata
 */
export type DataFlowEditableProperties = {
    __typename?: 'DataFlowEditableProperties';
    /** Description of the Data Flow */
    description?: Maybe<Scalars['String']>;
};

/** Update to writable Data Flow fields */
export type DataFlowEditablePropertiesUpdate = {
    /** Writable description aka documentation for a Data Flow */
    description: Scalars['String'];
};

/**
 * Deprecated, use DataFlowProperties instead
 * Additional read only properties about a Data Flow aka Pipeline
 */
export type DataFlowInfo = {
    __typename?: 'DataFlowInfo';
    /** Display name of the flow */
    name: Scalars['String'];
    /** Description of the flow */
    description?: Maybe<Scalars['String']>;
    /** Optional project or namespace associated with the flow */
    project?: Maybe<Scalars['String']>;
    /** External URL associated with the DataFlow */
    externalUrl?: Maybe<Scalars['String']>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

/** Additional read only properties about a Data Flow aka Pipeline */
export type DataFlowProperties = {
    __typename?: 'DataFlowProperties';
    /** Display name of the flow */
    name: Scalars['String'];
    /** Description of the flow */
    description?: Maybe<Scalars['String']>;
    /** Optional project or namespace associated with the flow */
    project?: Maybe<Scalars['String']>;
    /** External URL associated with the DataFlow */
    externalUrl?: Maybe<Scalars['String']>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

/** Arguments provided to update a Data Flow aka Pipeline Entity */
export type DataFlowUpdateInput = {
    /** Update to ownership */
    ownership?: Maybe<OwnershipUpdate>;
    /**
     * Deprecated, use tags field instead
     * Update to global tags
     */
    globalTags?: Maybe<GlobalTagsUpdate>;
    /** Update to tags */
    tags?: Maybe<GlobalTagsUpdate>;
    /** Update to editable properties */
    editableProperties?: Maybe<DataFlowEditablePropertiesUpdate>;
};

/** An DataHub Platform Access Policy -  Policies determine who can perform what actions against which resources on the platform */
export type DataHubPolicy = Entity & {
    __typename?: 'DataHubPolicy';
    /** The primary key of the Policy */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** Granular API for querying edges extending from the Role */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** The type of the Policy */
    policyType: PolicyType;
    /** The name of the Policy */
    name: Scalars['String'];
    /** The present state of the Policy */
    state: PolicyState;
    /** The description of the Policy */
    description?: Maybe<Scalars['String']>;
    /** The resources that the Policy privileges apply to */
    resources?: Maybe<ResourceFilter>;
    /** The privileges that the Policy grants */
    privileges: Array<Scalars['String']>;
    /** The actors that the Policy grants privileges to */
    actors: ActorFilter;
    /** Whether the Policy is editable, ie system policies, or not */
    editable: Scalars['Boolean'];
};

/** An DataHub Platform Access Policy -  Policies determine who can perform what actions against which resources on the platform */
export type DataHubPolicyRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A DataHub Role is a high-level abstraction on top of Policies that dictates what actions users can take. */
export type DataHubRole = Entity & {
    __typename?: 'DataHubRole';
    /** The primary key of the role */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** Granular API for querying edges extending from the Role */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** The name of the Role. */
    name: Scalars['String'];
    /** The description of the Role */
    description: Scalars['String'];
};

/** A DataHub Role is a high-level abstraction on top of Policies that dictates what actions users can take. */
export type DataHubRoleRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An DataHub View - Filters that are applied across the application automatically. */
export type DataHubView = Entity & {
    __typename?: 'DataHubView';
    /** The primary key of the View */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** The type of the View */
    viewType: DataHubViewType;
    /** The name of the View */
    name: Scalars['String'];
    /** The description of the View */
    description?: Maybe<Scalars['String']>;
    /** The definition of the View */
    definition: DataHubViewDefinition;
    /** Granular API for querying edges extending from the View */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/** An DataHub View - Filters that are applied across the application automatically. */
export type DataHubViewRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An DataHub View Definition */
export type DataHubViewDefinition = {
    __typename?: 'DataHubViewDefinition';
    /** A set of filters to apply. If left empty, then ALL entity types are in scope. */
    entityTypes: Array<EntityType>;
    /** A set of filters to apply. If left empty, then no filters will be applied. */
    filter: DataHubViewFilter;
};

/** Input required for creating a DataHub View Definition */
export type DataHubViewDefinitionInput = {
    /** A set of entity types that the view applies for. If left empty, then ALL entities will be in scope. */
    entityTypes: Array<EntityType>;
    /** A set of filters to apply. */
    filter: DataHubViewFilterInput;
};

/** A DataHub View Filter. Note that */
export type DataHubViewFilter = {
    __typename?: 'DataHubViewFilter';
    /** The operator used to combine the filters. */
    operator: LogicalOperator;
    /** A set of filters combined using the operator. If left empty, then no filters will be applied. */
    filters: Array<FacetFilter>;
};

/** Input required for creating a DataHub View Definition */
export type DataHubViewFilterInput = {
    /** The operator used to combine the filters. */
    operator: LogicalOperator;
    /** A set of filters combined via an operator. If left empty, then no filters will be applied. */
    filters: Array<FacetFilterInput>;
};

/** The type of a DataHub View */
export enum DataHubViewType {
    /** A personal view - e.g. saved filters */
    Personal = 'PERSONAL',
    /** A global view, e.g. role view */
    Global = 'GLOBAL',
}

/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJob = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'DataJob';
        /** The primary key of the Data Job */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /**
         * Deprecated, use relationship IsPartOf instead
         * The associated data flow
         */
        dataFlow?: Maybe<DataFlow>;
        /** Id of the job */
        jobId: Scalars['String'];
        /** Additional read only properties associated with the Data Job */
        properties?: Maybe<DataJobProperties>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Additional read write properties associated with the Data Job */
        editableProperties?: Maybe<DataJobEditableProperties>;
        /** The tags associated with the DataJob */
        tags?: Maybe<GlobalTags>;
        /** Ownership metadata of the job */
        ownership?: Maybe<Ownership>;
        /** Status metadata of the DataJob */
        status?: Maybe<Status>;
        /** The deprecation status of the Data Flow */
        deprecation?: Maybe<Deprecation>;
        /** References to internal resources related to the dashboard */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** The structured glossary terms associated with the dashboard */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the Data Job */
        domain?: Maybe<DomainAssociation>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the data job. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /**
         * Deprecated, use properties field instead
         * Additional read only information about a Data processing job
         * @deprecated Field no longer supported
         */
        info?: Maybe<DataJobInfo>;
        /**
         * Deprecated, use relationship Produces, Consumes, DownstreamOf instead
         * Information about the inputs and outputs of a Data processing job
         * @deprecated Field no longer supported
         */
        inputOutput?: Maybe<DataJobInputOutput>;
        /**
         * Deprecated, use the tags field instead
         * The structured tags associated with the DataJob
         * @deprecated Field no longer supported
         */
        globalTags?: Maybe<GlobalTags>;
        /** History of runs of this task */
        runs?: Maybe<DataProcessInstanceResult>;
        /** Privileges given to a user relevant to this entity */
        privileges?: Maybe<EntityPrivileges>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJobRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJobLineageArgs = {
    input: LineageInput;
};

/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJobRunsArgs = {
    start?: Maybe<Scalars['Int']>;
    count?: Maybe<Scalars['Int']>;
};

/**
 * Data Job properties that are editable via the UI This represents logical metadata,
 * as opposed to technical metadata
 */
export type DataJobEditableProperties = {
    __typename?: 'DataJobEditableProperties';
    /** Description of the Data Job */
    description?: Maybe<Scalars['String']>;
};

/** Update to writable Data Job fields */
export type DataJobEditablePropertiesUpdate = {
    /** Writable description aka documentation for a Data Job */
    description: Scalars['String'];
};

/**
 * Deprecated, use DataJobProperties instead
 * Additional read only information about a Data Job aka Task
 */
export type DataJobInfo = {
    __typename?: 'DataJobInfo';
    /** Job display name */
    name: Scalars['String'];
    /** Job description */
    description?: Maybe<Scalars['String']>;
    /** External URL associated with the DataJob */
    externalUrl?: Maybe<Scalars['String']>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

/**
 * The lineage information for a DataJob
 * TODO Rename this to align with other Lineage models
 */
export type DataJobInputOutput = {
    __typename?: 'DataJobInputOutput';
    /**
     * Deprecated, use relationship Consumes instead
     * Input datasets produced by the data job during processing
     * @deprecated Field no longer supported
     */
    inputDatasets?: Maybe<Array<Dataset>>;
    /**
     * Deprecated, use relationship Produces instead
     * Output datasets produced by the data job during processing
     * @deprecated Field no longer supported
     */
    outputDatasets?: Maybe<Array<Dataset>>;
    /**
     * Deprecated, use relationship DownstreamOf instead
     * Input datajobs that this data job depends on
     * @deprecated Field no longer supported
     */
    inputDatajobs?: Maybe<Array<DataJob>>;
};

/** Additional read only properties about a Data Job aka Task */
export type DataJobProperties = {
    __typename?: 'DataJobProperties';
    /** Job display name */
    name: Scalars['String'];
    /** Job description */
    description?: Maybe<Scalars['String']>;
    /** External URL associated with the DataJob */
    externalUrl?: Maybe<Scalars['String']>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

/** Arguments provided to update a Data Job aka Task Entity */
export type DataJobUpdateInput = {
    /** Update to ownership */
    ownership?: Maybe<OwnershipUpdate>;
    /**
     * Deprecated, use tags field instead
     * Update to global tags
     */
    globalTags?: Maybe<GlobalTagsUpdate>;
    /** Update to tags */
    tags?: Maybe<GlobalTagsUpdate>;
    /** Update to editable properties */
    editableProperties?: Maybe<DataJobEditablePropertiesUpdate>;
};

/**
 * A Data Platform represents a specific third party Data System or Tool Examples include
 * warehouses like Snowflake, orchestrators like Airflow, and dashboarding tools like Looker
 */
export type DataPlatform = Entity & {
    __typename?: 'DataPlatform';
    /** Urn of the data platform */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** The timestamp for the last time this entity was ingested */
    lastIngested?: Maybe<Scalars['Long']>;
    /** Name of the data platform */
    name: Scalars['String'];
    /** Additional read only properties associated with a data platform */
    properties?: Maybe<DataPlatformProperties>;
    /**
     * Deprecated, use properties displayName instead
     * Display name of the data platform
     * @deprecated Field no longer supported
     */
    displayName?: Maybe<Scalars['String']>;
    /**
     * Deprecated, use properties field instead
     * Additional properties associated with a data platform
     * @deprecated Field no longer supported
     */
    info?: Maybe<DataPlatformInfo>;
    /** Edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/**
 * A Data Platform represents a specific third party Data System or Tool Examples include
 * warehouses like Snowflake, orchestrators like Airflow, and dashboarding tools like Looker
 */
export type DataPlatformRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * Deprecated, use DataPlatformProperties instead
 * Additional read only information about a Data Platform
 */
export type DataPlatformInfo = {
    __typename?: 'DataPlatformInfo';
    /** The platform category */
    type: PlatformType;
    /** Display name associated with the platform */
    displayName?: Maybe<Scalars['String']>;
    /** The delimiter in the dataset names on the data platform */
    datasetNameDelimiter: Scalars['String'];
    /** A logo URL associated with the platform */
    logoUrl?: Maybe<Scalars['String']>;
};

/** A Data Platform instance represents an instance of a 3rd party platform like Looker, Snowflake, etc. */
export type DataPlatformInstance = Entity & {
    __typename?: 'DataPlatformInstance';
    /** Urn of the data platform */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** Name of the data platform */
    platform: DataPlatform;
    /** The platform instance id */
    instanceId: Scalars['String'];
    /** Edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Additional read only properties associated with a data platform instance */
    properties?: Maybe<DataPlatformInstanceProperties>;
    /** Ownership metadata of the data platform instance */
    ownership?: Maybe<Ownership>;
    /** References to internal resources related to the data platform instance */
    institutionalMemory?: Maybe<InstitutionalMemory>;
    /** Tags used for searching the data platform instance */
    tags?: Maybe<GlobalTags>;
    /** The deprecation status of the data platform instance */
    deprecation?: Maybe<Deprecation>;
    /** Status metadata of the container */
    status?: Maybe<Status>;
};

/** A Data Platform instance represents an instance of a 3rd party platform like Looker, Snowflake, etc. */
export type DataPlatformInstanceRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Additional read only properties about a DataPlatformInstance */
export type DataPlatformInstanceProperties = {
    __typename?: 'DataPlatformInstanceProperties';
    /** The name of the data platform instance used in display */
    name?: Maybe<Scalars['String']>;
    /** Read only technical description for the data platform instance */
    description?: Maybe<Scalars['String']>;
    /** Custom properties of the data platform instance */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** External URL associated with the data platform instance */
    externalUrl?: Maybe<Scalars['String']>;
};

/** Additional read only properties about a Data Platform */
export type DataPlatformProperties = {
    __typename?: 'DataPlatformProperties';
    /** The platform category */
    type: PlatformType;
    /** Display name associated with the platform */
    displayName?: Maybe<Scalars['String']>;
    /** The delimiter in the dataset names on the data platform */
    datasetNameDelimiter: Scalars['String'];
    /** A logo URL associated with the platform */
    logoUrl?: Maybe<Scalars['String']>;
};

/**
 * A DataProcessInstance Metadata Entity, representing an individual run of
 * a task or datajob.
 */
export type DataProcessInstance = EntityWithRelationships &
    Entity & {
        __typename?: 'DataProcessInstance';
        /** The primary key of the DataProcessInstance */
        urn: Scalars['String'];
        /** The standard Entity Type */
        type: EntityType;
        /** The history of state changes for the run */
        state?: Maybe<Array<Maybe<DataProcessRunEvent>>>;
        /** When the run was kicked off */
        created?: Maybe<AuditStamp>;
        /** The name of the data process */
        name?: Maybe<Scalars['String']>;
        /**
         * Edges extending from this entity.
         * In the UI, used for inputs, outputs and parentTemplate
         */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The link to view the task run in the source system */
        externalUrl?: Maybe<Scalars['String']>;
    };

/**
 * A DataProcessInstance Metadata Entity, representing an individual run of
 * a task or datajob.
 */
export type DataProcessInstanceStateArgs = {
    startTimeMillis?: Maybe<Scalars['Long']>;
    endTimeMillis?: Maybe<Scalars['Long']>;
    limit?: Maybe<Scalars['Int']>;
};

/**
 * A DataProcessInstance Metadata Entity, representing an individual run of
 * a task or datajob.
 */
export type DataProcessInstanceRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * A DataProcessInstance Metadata Entity, representing an individual run of
 * a task or datajob.
 */
export type DataProcessInstanceLineageArgs = {
    input: LineageInput;
};

/** Data Process instances that match the provided query */
export type DataProcessInstanceResult = {
    __typename?: 'DataProcessInstanceResult';
    /** The number of entities to include in result set */
    count?: Maybe<Scalars['Int']>;
    /** The offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The total number of run events returned */
    total?: Maybe<Scalars['Int']>;
    /** The data process instances that produced or consumed the entity */
    runs?: Maybe<Array<Maybe<DataProcessInstance>>>;
};

/** the result of a run, part of the run state */
export type DataProcessInstanceRunResult = {
    __typename?: 'DataProcessInstanceRunResult';
    /** The outcome of the run */
    resultType?: Maybe<DataProcessInstanceRunResultType>;
    /** The outcome of the run in the data platforms native language */
    nativeResultType?: Maybe<Scalars['String']>;
};

/** The result of the data process run */
export enum DataProcessInstanceRunResultType {
    /** The run finished successfully */
    Success = 'SUCCESS',
    /** The run finished in failure */
    Failure = 'FAILURE',
    /** The run was skipped */
    Skipped = 'SKIPPED',
    /** The run failed and is up for retry */
    UpForRetry = 'UP_FOR_RETRY',
}

/** A state change event in the data process instance lifecycle */
export type DataProcessRunEvent = TimeSeriesAspect & {
    __typename?: 'DataProcessRunEvent';
    /** The status of the data process instance */
    status?: Maybe<DataProcessRunStatus>;
    /** The try number that this instance run is in */
    attempt?: Maybe<Scalars['Int']>;
    /** The result of a run */
    result?: Maybe<DataProcessInstanceRunResult>;
    /** The timestamp associated with the run event in milliseconds */
    timestampMillis: Scalars['Long'];
};

/** The status of the data process instance */
export enum DataProcessRunStatus {
    /** The data process instance has started but not completed */
    Started = 'STARTED',
    /** The data process instance has completed */
    Complete = 'COMPLETE',
}

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type Dataset = BrowsableEntity &
    Entity &
    EntityWithRelationships & {
        __typename?: 'Dataset';
        /**
         * Experimental API.
         * For fetching extra entities that do not have custom UI code yet
         */
        aspects?: Maybe<Array<RawAspect>>;
        /** Assertions associated with the Dataset */
        assertions?: Maybe<EntityAssertionsResult>;
        /** The browse paths corresponding to the dataset. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /** The parent container in which the entity resides */
        container?: Maybe<Container>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /**
         * Profile Stats resource that retrieves the events in a previous unit of time in descending order
         * If no start or end time are provided, the most recent events will be returned
         */
        datasetProfiles?: Maybe<Array<DatasetProfile>>;
        /** The deprecation status of the dataset */
        deprecation?: Maybe<Deprecation>;
        /**
         * Deprecated, use the properties field instead
         * Read only technical description for dataset
         * @deprecated Field no longer supported
         */
        description?: Maybe<Scalars['String']>;
        /** The Domain associated with the Dataset */
        domain?: Maybe<DomainAssociation>;
        /** An additional set of of read write properties */
        editableProperties?: Maybe<DatasetEditableProperties>;
        /** Editable schema metadata of the dataset */
        editableSchemaMetadata?: Maybe<EditableSchemaMetadata>;
        /** Embed information about the Dataset */
        embed?: Maybe<Embed>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
        /**
         * Deprecated, use properties field instead
         * External URL associated with the Dataset
         * @deprecated Field no longer supported
         */
        externalUrl?: Maybe<Scalars['String']>;
        /** fine grained lineage */
        fineGrainedLineages?: Maybe<Array<FineGrainedLineage>>;
        /**
         * Deprecated, use tags field instead
         * The structured tags associated with the dataset
         * @deprecated Field no longer supported
         */
        globalTags?: Maybe<GlobalTags>;
        /** The structured glossary terms associated with the dataset */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** Experimental! The resolved health statuses of the Dataset */
        health?: Maybe<Array<Health>>;
        /** References to internal resources related to the dataset */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /**
         * Unique guid for dataset
         * No longer to be used as the Dataset display name. Use properties.name instead
         */
        name: Scalars['String'];
        /** Operational events for an entity. */
        operations?: Maybe<Array<Operation>>;
        /**
         * Deprecated, see the properties field instead
         * Environment in which the dataset belongs to or where it was generated
         * Note that this field will soon be deprecated in favor of a more standardized concept of Environment
         * @deprecated Field no longer supported
         */
        origin: FabricType;
        /** Ownership metadata of the dataset */
        ownership?: Maybe<Ownership>;
        /** Recursively get the lineage of containers for this entity */
        parentContainers?: Maybe<ParentContainersResult>;
        /** Standardized platform urn where the dataset is defined */
        platform: DataPlatform;
        /**
         * Deprecated, do not use this field
         * The logical type of the dataset ie table, stream, etc
         * @deprecated Field no longer supported
         */
        platformNativeType?: Maybe<PlatformNativeType>;
        /** Privileges given to a user relevant to this entity */
        privileges?: Maybe<EntityPrivileges>;
        /** An additional set of read only properties */
        properties?: Maybe<DatasetProperties>;
        /** Edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** History of datajob runs that either produced or consumed this dataset */
        runs?: Maybe<DataProcessInstanceResult>;
        /**
         * Schema metadata of the dataset
         * @deprecated Use `schemaMetadata`
         */
        schema?: Maybe<Schema>;
        /** Schema metadata of the dataset, available by version number */
        schemaMetadata?: Maybe<SchemaMetadata>;
        /** Metadata about the datasets siblings */
        siblings?: Maybe<SiblingProperties>;
        /** Experimental - Summary operational & usage statistics about a Dataset */
        statsSummary?: Maybe<DatasetStatsSummary>;
        /** Status of the Dataset */
        status?: Maybe<Status>;
        /** Sub Types that this entity implements */
        subTypes?: Maybe<SubTypes>;
        /** Tags used for searching dataset */
        tags?: Maybe<GlobalTags>;
        /** The results of evaluating tests */
        testResults?: Maybe<TestResults>;
        /** The standard Entity Type */
        type: EntityType;
        /**
         * Deprecated, use properties instead
         * Native Dataset Uri
         * Uri should not include any environment specific properties
         * @deprecated Field no longer supported
         */
        uri?: Maybe<Scalars['String']>;
        /** The primary key of the Dataset */
        urn: Scalars['String'];
        /**
         * Statistics about how this Dataset is used
         * The first parameter, `resource`, is deprecated and no longer needs to be provided
         */
        usageStats?: Maybe<UsageQueryResult>;
        /** View related properties. Only relevant if subtypes field contains view. */
        viewProperties?: Maybe<ViewProperties>;
    };

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetAspectsArgs = {
    input?: Maybe<AspectParams>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetAssertionsArgs = {
    start?: Maybe<Scalars['Int']>;
    count?: Maybe<Scalars['Int']>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetDatasetProfilesArgs = {
    startTimeMillis?: Maybe<Scalars['Long']>;
    endTimeMillis?: Maybe<Scalars['Long']>;
    filter?: Maybe<FilterInput>;
    limit?: Maybe<Scalars['Int']>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetLineageArgs = {
    input: LineageInput;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetOperationsArgs = {
    startTimeMillis?: Maybe<Scalars['Long']>;
    endTimeMillis?: Maybe<Scalars['Long']>;
    filter?: Maybe<FilterInput>;
    limit?: Maybe<Scalars['Int']>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetRunsArgs = {
    start?: Maybe<Scalars['Int']>;
    count?: Maybe<Scalars['Int']>;
    direction: RelationshipDirection;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetSchemaMetadataArgs = {
    version?: Maybe<Scalars['Long']>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetUsageStatsArgs = {
    resource?: Maybe<Scalars['String']>;
    range?: Maybe<TimeRange>;
};

/** Detailed information about a Dataset Assertion */
export type DatasetAssertionInfo = {
    __typename?: 'DatasetAssertionInfo';
    /** The urn of the dataset that the assertion is related to */
    datasetUrn: Scalars['String'];
    /** The scope of the Dataset assertion. */
    scope: DatasetAssertionScope;
    /** The fields serving as input to the assertion. Empty if there are none. */
    fields?: Maybe<Array<SchemaFieldRef>>;
    /** Standardized assertion operator */
    aggregation?: Maybe<AssertionStdAggregation>;
    /** Standardized assertion operator */
    operator: AssertionStdOperator;
    /** Standard parameters required for the assertion. e.g. min_value, max_value, value, columns */
    parameters?: Maybe<AssertionStdParameters>;
    /** The native operator for the assertion. For Great Expectations, this will contain the original expectation name. */
    nativeType?: Maybe<Scalars['String']>;
    /** Native parameters required for the assertion. */
    nativeParameters?: Maybe<Array<StringMapEntry>>;
    /** Logic comprising a raw, unstructured assertion. */
    logic?: Maybe<Scalars['String']>;
};

/** The scope that a Dataset-level assertion applies to. */
export enum DatasetAssertionScope {
    /** Assertion applies to columns of a dataset. */
    DatasetColumn = 'DATASET_COLUMN',
    /** Assertion applies to rows of a dataset. */
    DatasetRows = 'DATASET_ROWS',
    /** Assertion applies to schema of a dataset. */
    DatasetSchema = 'DATASET_SCHEMA',
    /** The scope of an assertion is unknown. */
    Unknown = 'UNKNOWN',
}

/**
 * Deprecated, use Deprecation instead
 * Information about Dataset deprecation status
 * Note that this model will soon be migrated to a more general purpose Entity status
 */
export type DatasetDeprecation = {
    __typename?: 'DatasetDeprecation';
    /** Whether the dataset has been deprecated by owner */
    deprecated: Scalars['Boolean'];
    /** The time user plan to decommission this dataset */
    decommissionTime?: Maybe<Scalars['Long']>;
    /** Additional information about the dataset deprecation plan */
    note: Scalars['String'];
    /** The user who will be credited for modifying this deprecation content */
    actor?: Maybe<Scalars['String']>;
};

/** An update for the deprecation information for a Metadata Entity */
export type DatasetDeprecationUpdate = {
    /** Whether the dataset is deprecated */
    deprecated: Scalars['Boolean'];
    /** The time user plan to decommission this dataset */
    decommissionTime?: Maybe<Scalars['Long']>;
    /** Additional information about the dataset deprecation plan */
    note: Scalars['String'];
};

/**
 * Dataset properties that are editable via the UI This represents logical metadata,
 * as opposed to technical metadata
 */
export type DatasetEditableProperties = {
    __typename?: 'DatasetEditableProperties';
    /** Description of the Dataset */
    description?: Maybe<Scalars['String']>;
};

/** Update to writable Dataset fields */
export type DatasetEditablePropertiesUpdate = {
    /** Writable description aka documentation for a Dataset */
    description: Scalars['String'];
};

/** An individual Dataset Field Profile */
export type DatasetFieldProfile = {
    __typename?: 'DatasetFieldProfile';
    /** The standardized path of the field */
    fieldPath: Scalars['String'];
    /** The unique value count for the field across the Dataset */
    uniqueCount?: Maybe<Scalars['Long']>;
    /** The proportion of rows with unique values across the Dataset */
    uniqueProportion?: Maybe<Scalars['Float']>;
    /** The number of NULL row values across the Dataset */
    nullCount?: Maybe<Scalars['Long']>;
    /** The proportion of rows with NULL values across the Dataset */
    nullProportion?: Maybe<Scalars['Float']>;
    /** The min value for the field */
    min?: Maybe<Scalars['String']>;
    /** The max value for the field */
    max?: Maybe<Scalars['String']>;
    /** The mean value for the field */
    mean?: Maybe<Scalars['String']>;
    /** The median value for the field */
    median?: Maybe<Scalars['String']>;
    /** The standard deviation for the field */
    stdev?: Maybe<Scalars['String']>;
    /** A set of sample values for the field */
    sampleValues?: Maybe<Array<Scalars['String']>>;
};

/**
 * Deprecated
 * The type of an edge between two Datasets
 */
export enum DatasetLineageType {
    /** Direct copy without modification */
    Copy = 'COPY',
    /** Transformed dataset */
    Transformed = 'TRANSFORMED',
    /** Represents a view defined on the sources */
    View = 'VIEW',
}

/** A Dataset Profile associated with a Dataset, containing profiling statistics about the Dataset */
export type DatasetProfile = TimeSeriesAspect & {
    __typename?: 'DatasetProfile';
    /** The time at which the profile was reported */
    timestampMillis: Scalars['Long'];
    /** An optional row count of the Dataset */
    rowCount?: Maybe<Scalars['Long']>;
    /** An optional column count of the Dataset */
    columnCount?: Maybe<Scalars['Long']>;
    /** The storage size in bytes */
    sizeInBytes?: Maybe<Scalars['Long']>;
    /** An optional set of per field statistics obtained in the profile */
    fieldProfiles?: Maybe<Array<DatasetFieldProfile>>;
    /** Information about the partition that was profiled */
    partitionSpec?: Maybe<PartitionSpec>;
};

/** Additional read only properties about a Dataset */
export type DatasetProperties = {
    __typename?: 'DatasetProperties';
    /** The name of the dataset used in display */
    name: Scalars['String'];
    /** Fully-qualified name of the Dataset */
    qualifiedName?: Maybe<Scalars['String']>;
    /**
     * Environment in which the dataset belongs to or where it was generated
     * Note that this field will soon be deprecated in favor of a more standardized concept of Environment
     */
    origin: FabricType;
    /** Read only technical description for dataset */
    description?: Maybe<Scalars['String']>;
    /** Custom properties of the Dataset */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** External URL associated with the Dataset */
    externalUrl?: Maybe<Scalars['String']>;
    /** Created timestamp millis associated with the Dataset */
    created?: Maybe<Scalars['Long']>;
    /** Actor associated with the Dataset's created timestamp */
    createdActor?: Maybe<Scalars['String']>;
    /** Last Modified timestamp millis associated with the Dataset */
    lastModified?: Maybe<Scalars['Long']>;
    /** Actor associated with the Dataset's lastModified timestamp */
    lastModifiedActor?: Maybe<Scalars['String']>;
};

/** Experimental - subject to change. A summary of usage metrics about a Dataset. */
export type DatasetStatsSummary = {
    __typename?: 'DatasetStatsSummary';
    /** The query count in the past 30 days */
    queryCountLast30Days?: Maybe<Scalars['Int']>;
    /** The unique user count in the past 30 days */
    uniqueUserCountLast30Days?: Maybe<Scalars['Int']>;
    /** The top users in the past 30 days */
    topUsersLast30Days?: Maybe<Array<CorpUser>>;
};

/** Arguments provided to update a Dataset Entity */
export type DatasetUpdateInput = {
    /** Update to ownership */
    ownership?: Maybe<OwnershipUpdate>;
    /** Update to deprecation status */
    deprecation?: Maybe<DatasetDeprecationUpdate>;
    /** Update to institutional memory, ie documentation */
    institutionalMemory?: Maybe<InstitutionalMemoryUpdate>;
    /**
     * Deprecated, use tags field instead
     * Update to global tags
     */
    globalTags?: Maybe<GlobalTagsUpdate>;
    /** Update to tags */
    tags?: Maybe<GlobalTagsUpdate>;
    /** Update to editable schema metadata of the dataset */
    editableSchemaMetadata?: Maybe<EditableSchemaMetadataUpdate>;
    /** Update to editable properties */
    editableProperties?: Maybe<DatasetEditablePropertiesUpdate>;
};

/** For consumption by UI only */
export enum DateInterval {
    Second = 'SECOND',
    Minute = 'MINUTE',
    Hour = 'HOUR',
    Day = 'DAY',
    Week = 'WEEK',
    Month = 'MONTH',
    Year = 'YEAR',
}

/** For consumption by UI only */
export type DateRange = {
    __typename?: 'DateRange';
    start: Scalars['String'];
    end: Scalars['String'];
};

/** Information about Metadata Entity deprecation status */
export type Deprecation = {
    __typename?: 'Deprecation';
    /** Whether the entity has been deprecated by owner */
    deprecated: Scalars['Boolean'];
    /** The time user plan to decommission this entity */
    decommissionTime?: Maybe<Scalars['Long']>;
    /** Additional information about the entity deprecation plan */
    note?: Maybe<Scalars['String']>;
    /** The user who will be credited for modifying this deprecation content */
    actor?: Maybe<Scalars['String']>;
};

/** Incubating. Updates the description of a resource. Currently supports DatasetField descriptions only */
export type DescriptionUpdateInput = {
    /** The new description */
    description: Scalars['String'];
    /** The primary key of the resource to attach the description to, eg dataset urn */
    resourceUrn: Scalars['String'];
    /** An optional sub resource type */
    subResourceType?: Maybe<SubResourceType>;
    /** A sub resource identifier, eg dataset field path */
    subResource?: Maybe<Scalars['String']>;
};

/** A domain, or a logical grouping of Metadata Entities */
export type Domain = Entity & {
    __typename?: 'Domain';
    /** The primary key of the domain */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** Id of the domain */
    id: Scalars['String'];
    /** Properties about a domain */
    properties?: Maybe<DomainProperties>;
    /** Ownership metadata of the dataset */
    ownership?: Maybe<Ownership>;
    /** References to internal resources related to the dataset */
    institutionalMemory?: Maybe<InstitutionalMemory>;
    /** Children entities inside of the Domain */
    entities?: Maybe<SearchResults>;
    /** Edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/** A domain, or a logical grouping of Metadata Entities */
export type DomainEntitiesArgs = {
    input?: Maybe<DomainEntitiesInput>;
};

/** A domain, or a logical grouping of Metadata Entities */
export type DomainRelationshipsArgs = {
    input: RelationshipsInput;
};

export type DomainAssociation = {
    __typename?: 'DomainAssociation';
    /** The domain related to the assocaited urn */
    domain: Domain;
    /** Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together */
    associatedUrn: Scalars['String'];
};

/** Input required to fetch the entities inside of a Domain. */
export type DomainEntitiesInput = {
    /** Optional query filter for particular entities inside the domain */
    query?: Maybe<Scalars['String']>;
    /** The offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of entities to include in result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional Facet filters to apply to the result set */
    filters?: Maybe<Array<FacetFilterInput>>;
};

/** Properties about a domain */
export type DomainProperties = {
    __typename?: 'DomainProperties';
    /** Display name of the domain */
    name: Scalars['String'];
    /** Description of the Domain */
    description?: Maybe<Scalars['String']>;
};

/** Deprecated, use relationships query instead */
export type DownstreamEntityRelationships = {
    __typename?: 'DownstreamEntityRelationships';
    entities?: Maybe<Array<Maybe<EntityRelationshipLegacy>>>;
};

/** Editable schema field metadata ie descriptions, tags, etc */
export type EditableSchemaFieldInfo = {
    __typename?: 'EditableSchemaFieldInfo';
    /** Flattened name of a field identifying the field the editable info is applied to */
    fieldPath: Scalars['String'];
    /** Edited description of the field */
    description?: Maybe<Scalars['String']>;
    /**
     * Deprecated, use tags field instead
     * Tags associated with the field
     * @deprecated Field no longer supported
     */
    globalTags?: Maybe<GlobalTags>;
    /** Tags associated with the field */
    tags?: Maybe<GlobalTags>;
    /** Glossary terms associated with the field */
    glossaryTerms?: Maybe<GlossaryTerms>;
};

/** Update to writable schema field metadata */
export type EditableSchemaFieldInfoUpdate = {
    /** Flattened name of a field identifying the field the editable info is applied to */
    fieldPath: Scalars['String'];
    /** Edited description of the field */
    description?: Maybe<Scalars['String']>;
    /** Tags associated with the field */
    globalTags?: Maybe<GlobalTagsUpdate>;
};

/** Information about schema metadata that is editable via the UI */
export type EditableSchemaMetadata = {
    __typename?: 'EditableSchemaMetadata';
    /** Editable schema field metadata */
    editableSchemaFieldInfo: Array<EditableSchemaFieldInfo>;
};

/** Update to editable schema metadata of the dataset */
export type EditableSchemaMetadataUpdate = {
    /** Update to writable schema field metadata */
    editableSchemaFieldInfo: Array<EditableSchemaFieldInfoUpdate>;
};

/**
 * Additional read write Tag properties
 * Deprecated! Replaced by TagProperties.
 */
export type EditableTagProperties = {
    __typename?: 'EditableTagProperties';
    /** A display name for the Tag */
    name?: Maybe<Scalars['String']>;
    /** A description of the Tag */
    description?: Maybe<Scalars['String']>;
};

/** Information required to render an embedded version of an asset */
export type Embed = {
    __typename?: 'Embed';
    /** A URL which can be rendered inside of an iframe. */
    renderUrl?: Maybe<Scalars['String']>;
};

/** A top level Metadata Entity */
export type Entity = {
    /** A primary key of the Metadata Entity */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** List of relationships between the source Entity and some destination entities with a given types */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/** A top level Metadata Entity */
export type EntityRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A list of Assertions Associated with an Entity */
export type EntityAssertionsResult = {
    __typename?: 'EntityAssertionsResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of assertions in the returned result set */
    count: Scalars['Int'];
    /** The total number of assertions in the result set */
    total: Scalars['Int'];
    /** The assertions themselves */
    assertions: Array<Assertion>;
};

/** Input for the get entity counts endpoint */
export type EntityCountInput = {
    types?: Maybe<Array<EntityType>>;
};

export type EntityCountResult = {
    __typename?: 'EntityCountResult';
    entityType: EntityType;
    count: Scalars['Int'];
};

export type EntityCountResults = {
    __typename?: 'EntityCountResults';
    counts?: Maybe<Array<EntityCountResult>>;
};

/** A list of lineage information associated with a source Entity */
export type EntityLineageResult = {
    __typename?: 'EntityLineageResult';
    /** Start offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** Number of results in the returned result set */
    count?: Maybe<Scalars['Int']>;
    /** Total number of results in the result set */
    total?: Maybe<Scalars['Int']>;
    /** The number of results that were filtered out of the page (soft-deleted or non-existent) */
    filtered?: Maybe<Scalars['Int']>;
    /** Relationships in the result set */
    relationships: Array<LineageRelationship>;
};

/** An overview of the field that was matched in the entity search document */
export type EntityPath = {
    __typename?: 'EntityPath';
    /** Path of entities between source and destination nodes */
    path?: Maybe<Array<Maybe<Entity>>>;
};

/** Shared privileges object across entities. Not all privileges apply to every entity. */
export type EntityPrivileges = {
    __typename?: 'EntityPrivileges';
    /**
     * Whether or not a user can create child entities under a parent entity.
     * For example, can one create Terms/Node sunder a Glossary Node.
     */
    canManageChildren?: Maybe<Scalars['Boolean']>;
    /** Whether or not a user can delete or move this entity. */
    canManageEntity?: Maybe<Scalars['Boolean']>;
    /** Whether or not a user can create or delete lineage edges for an entity. */
    canEditLineage?: Maybe<Scalars['Boolean']>;
    /** Whether or not a user update the embed information */
    canEditEmbed?: Maybe<Scalars['Boolean']>;
    /** Whether or not a user can update the Queries for the entity (e.g. dataset) */
    canEditQueries?: Maybe<Scalars['Boolean']>;
};

/** Context to define the entity profile page */
export type EntityProfileParams = {
    __typename?: 'EntityProfileParams';
    /** Urn of the entity being shown */
    urn: Scalars['String'];
    /** Type of the enity being displayed */
    type: EntityType;
};

/** A relationship between two entities TODO Migrate all entity relationships to this more generic model */
export type EntityRelationship = {
    __typename?: 'EntityRelationship';
    /** The type of the relationship */
    type: Scalars['String'];
    /** The direction of the relationship relative to the source entity */
    direction: RelationshipDirection;
    /** Entity that is related via lineage */
    entity?: Maybe<Entity>;
    /** An AuditStamp corresponding to the last modification of this relationship */
    created?: Maybe<AuditStamp>;
};

/** Deprecated, use relationships query instead */
export type EntityRelationshipLegacy = {
    __typename?: 'EntityRelationshipLegacy';
    /** Entity that is related via lineage */
    entity?: Maybe<EntityWithRelationships>;
    /** An AuditStamp corresponding to the last modification of this relationship */
    created?: Maybe<AuditStamp>;
};

/** A list of relationship information associated with a source Entity */
export type EntityRelationshipsResult = {
    __typename?: 'EntityRelationshipsResult';
    /** Start offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** Number of results in the returned result set */
    count?: Maybe<Scalars['Int']>;
    /** Total number of results in the result set */
    total?: Maybe<Scalars['Int']>;
    /** Relationships in the result set */
    relationships: Array<EntityRelationship>;
};

/** Context that defines an entity page requesting recommendations */
export type EntityRequestContext = {
    /** Type of the enity being displayed */
    type: EntityType;
    /** Urn of the entity being displayed */
    urn: Scalars['String'];
};

/** A top level Metadata Entity Type */
export enum EntityType {
    /** A Domain containing Metadata Entities */
    Domain = 'DOMAIN',
    /** The Dataset Entity */
    Dataset = 'DATASET',
    /** The CorpUser Entity */
    CorpUser = 'CORP_USER',
    /** The CorpGroup Entity */
    CorpGroup = 'CORP_GROUP',
    /** The DataPlatform Entity */
    DataPlatform = 'DATA_PLATFORM',
    /** The Dashboard Entity */
    Dashboard = 'DASHBOARD',
    /** The Notebook Entity */
    Notebook = 'NOTEBOOK',
    /** The Chart Entity */
    Chart = 'CHART',
    /** The Data Flow (or Data Pipeline) Entity, */
    DataFlow = 'DATA_FLOW',
    /** The Data Job (or Data Task) Entity */
    DataJob = 'DATA_JOB',
    /** The Tag Entity */
    Tag = 'TAG',
    /** The Glossary Term Entity */
    GlossaryTerm = 'GLOSSARY_TERM',
    /** The Glossary Node Entity */
    GlossaryNode = 'GLOSSARY_NODE',
    /** A container of Metadata Entities */
    Container = 'CONTAINER',
    /** The ML Model Entity */
    Mlmodel = 'MLMODEL',
    /** The MLModelGroup Entity */
    MlmodelGroup = 'MLMODEL_GROUP',
    /** ML Feature Table Entity */
    MlfeatureTable = 'MLFEATURE_TABLE',
    /** The ML Feature Entity */
    Mlfeature = 'MLFEATURE',
    /** The ML Primary Key Entity */
    MlprimaryKey = 'MLPRIMARY_KEY',
    /** A DataHub Managed Ingestion Source */
    IngestionSource = 'INGESTION_SOURCE',
    /** A DataHub ExecutionRequest */
    ExecutionRequest = 'EXECUTION_REQUEST',
    /** A DataHub Assertion */
    Assertion = 'ASSERTION',
    /** An instance of an individual run of a data job or data flow */
    DataProcessInstance = 'DATA_PROCESS_INSTANCE',
    /** Data Platform Instance Entity */
    DataPlatformInstance = 'DATA_PLATFORM_INSTANCE',
    /** A DataHub Access Token */
    AccessToken = 'ACCESS_TOKEN',
    /** A DataHub Test */
    Test = 'TEST',
    /** A DataHub Policy */
    DatahubPolicy = 'DATAHUB_POLICY',
    /** A DataHub Role */
    DatahubRole = 'DATAHUB_ROLE',
    /** A DataHub Post */
    Post = 'POST',
    /** A Schema Field */
    SchemaField = 'SCHEMA_FIELD',
    /** A DataHub View */
    DatahubView = 'DATAHUB_VIEW',
    /** A dataset query */
    Query = 'QUERY',
}

/** Deprecated, use relationships field instead */
export type EntityWithRelationships = {
    /** A primary key associated with the Metadata Entity */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Edges extending from this entity grouped by direction in the lineage graph */
    lineage?: Maybe<EntityLineageResult>;
};

/** Deprecated, use relationships field instead */
export type EntityWithRelationshipsRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Deprecated, use relationships field instead */
export type EntityWithRelationshipsLineageArgs = {
    input: LineageInput;
};

export type EthicalConsiderations = {
    __typename?: 'EthicalConsiderations';
    /** Does the model use any sensitive data eg, protected classes */
    data?: Maybe<Array<Scalars['String']>>;
    /** Is the model intended to inform decisions about matters central to human life or flourishing eg, health or safety */
    humanLife?: Maybe<Array<Scalars['String']>>;
    /** What risk mitigation strategies were used during model development */
    mitigations?: Maybe<Array<Scalars['String']>>;
    /**
     * What risks may be present in model usage
     * Try to identify the potential recipients, likelihood, and magnitude of harms
     * If these cannot be determined, note that they were considered but remain unknown
     */
    risksAndHarms?: Maybe<Array<Scalars['String']>>;
    /**
     * Are there any known model use cases that are especially fraught
     * This may connect directly to the intended use section
     */
    useCases?: Maybe<Array<Scalars['String']>>;
};

/** Retrieve an ingestion execution request */
export type ExecutionRequest = {
    __typename?: 'ExecutionRequest';
    /** Urn of the execution request */
    urn: Scalars['String'];
    /** Unique id for the execution request */
    id: Scalars['String'];
    /** Input provided when creating the Execution Request */
    input: ExecutionRequestInput;
    /** Result of the execution request */
    result?: Maybe<ExecutionRequestResult>;
};

/** Input provided when creating an Execution Request */
export type ExecutionRequestInput = {
    __typename?: 'ExecutionRequestInput';
    /** The type of the task to executed */
    task: Scalars['String'];
    /** The source of the execution request */
    source: ExecutionRequestSource;
    /** Arguments provided when creating the execution request */
    arguments?: Maybe<Array<StringMapEntry>>;
    /** The time at which the request was created */
    requestedAt: Scalars['Long'];
};

/** The result of an ExecutionRequest */
export type ExecutionRequestResult = {
    __typename?: 'ExecutionRequestResult';
    /** The result of the request, e.g. either SUCCEEDED or FAILED */
    status: Scalars['String'];
    /** Time at which the task began */
    startTimeMs?: Maybe<Scalars['Long']>;
    /** Duration of the task */
    durationMs?: Maybe<Scalars['Long']>;
    /** A report about the ingestion run */
    report?: Maybe<Scalars['String']>;
    /** A structured report for this Execution Request */
    structuredReport?: Maybe<StructuredReport>;
};

/** Information about the source of an execution request */
export type ExecutionRequestSource = {
    __typename?: 'ExecutionRequestSource';
    /** The type of the source, e.g. SCHEDULED_INGESTION_SOURCE */
    type?: Maybe<Scalars['String']>;
};

/**
 * An environment identifier for a particular Entity, ie staging or production
 * Note that this model will soon be deprecated in favor of a more general purpose of notion
 * of data environment
 */
export enum FabricType {
    /** Designates development fabrics */
    Dev = 'DEV',
    /** Designates testing fabrics */
    Test = 'TEST',
    /** Designates quality assurance fabrics */
    Qa = 'QA',
    /** Designates user acceptance testing fabrics */
    Uat = 'UAT',
    /** Designates early integration fabrics */
    Ei = 'EI',
    /** Designates pre-production fabrics */
    Pre = 'PRE',
    /** Designates staging fabrics */
    Stg = 'STG',
    /** Designates non-production fabrics */
    NonProd = 'NON_PROD',
    /** Designates production fabrics */
    Prod = 'PROD',
    /** Designates corporation fabrics */
    Corp = 'CORP',
}

/** A single filter value */
export type FacetFilter = {
    __typename?: 'FacetFilter';
    /** Name of field to filter by */
    field: Scalars['String'];
    /** Condition for the values. */
    condition?: Maybe<FilterOperator>;
    /** Values, one of which the intended field should match. */
    values: Array<Scalars['String']>;
    /** If the filter should or should not be matched */
    negated?: Maybe<Scalars['Boolean']>;
};

/** Facet filters to apply to search results */
export type FacetFilterInput = {
    /** Name of field to filter by */
    field: Scalars['String'];
    /**
     * Value of the field to filter by. Deprecated in favor of `values`, which should accept a single element array for a
     * value
     */
    value?: Maybe<Scalars['String']>;
    /** Values, one of which the intended field should match. */
    values?: Maybe<Array<Scalars['String']>>;
    /** If the filter should or should not be matched */
    negated?: Maybe<Scalars['Boolean']>;
    /** Condition for the values. How to If unset, assumed to be equality */
    condition?: Maybe<FilterOperator>;
};

/** Contains valid fields to filter search results further on */
export type FacetMetadata = {
    __typename?: 'FacetMetadata';
    /** Name of a field present in the search entity */
    field: Scalars['String'];
    /** Display name of the field */
    displayName?: Maybe<Scalars['String']>;
    /** Aggregated search result counts by value of the field */
    aggregations: Array<AggregationMetadata>;
};

/** The usage for a particular Dataset field */
export type FieldUsageCounts = {
    __typename?: 'FieldUsageCounts';
    /** The path of the field */
    fieldName?: Maybe<Scalars['String']>;
    /** The count of usages */
    count?: Maybe<Scalars['Int']>;
};

/** A set of filter criteria */
export type FilterInput = {
    /** A list of conjunctive filters */
    and: Array<FacetFilterInput>;
};

export enum FilterOperator {
    /** Represent the relation: String field contains value, e.g. name contains Profile */
    Contain = 'CONTAIN',
    /** Represent the relation: field = value, e.g. platform = hdfs */
    Equal = 'EQUAL',
    /** * Represent the relation: String field is one of the array values to, e.g. name in ["Profile", "Event"] */
    In = 'IN',
}

export type FineGrainedLineage = {
    __typename?: 'FineGrainedLineage';
    upstreams?: Maybe<Array<SchemaFieldRef>>;
    downstreams?: Maybe<Array<SchemaFieldRef>>;
};

export type FloatBox = {
    __typename?: 'FloatBox';
    floatValue: Scalars['Float'];
};

/** Metadata around a foreign key constraint between two datasets */
export type ForeignKeyConstraint = {
    __typename?: 'ForeignKeyConstraint';
    /** The human-readable name of the constraint */
    name?: Maybe<Scalars['String']>;
    /** List of fields in the foreign dataset */
    foreignFields?: Maybe<Array<Maybe<SchemaFieldEntity>>>;
    /** List of fields in this dataset */
    sourceFields?: Maybe<Array<Maybe<SchemaFieldEntity>>>;
    /** The foreign dataset for easy reference */
    foreignDataset?: Maybe<Dataset>;
};

/**
 * Freshness stats for a query result.
 * Captures whether the query was served out of a cache, what the staleness was, etc.
 */
export type FreshnessStats = {
    __typename?: 'FreshnessStats';
    /** Whether a cache was used to respond to this query */
    cached?: Maybe<Scalars['Boolean']>;
    /**
     * The latest timestamp in millis of the system that was used to respond to this query
     * In case a cache was consulted, this reflects the freshness of the cache
     * In case an index was consulted, this reflects the freshness of the index
     */
    systemFreshness?: Maybe<Array<Maybe<SystemFreshness>>>;
};

/** Input required to fetch a new Access Token. */
export type GetAccessTokenInput = {
    /** The type of the Access Token. */
    type: AccessTokenType;
    /** The actor associated with the Access Token. */
    actorUrn: Scalars['String'];
    /** The duration for which the Access Token is valid. */
    duration: AccessTokenDuration;
};

/** Input for getting granted privileges */
export type GetGrantedPrivilegesInput = {
    /** Urn of the actor */
    actorUrn: Scalars['String'];
    /** Spec to identify resource. If empty, gets privileges granted to the actor */
    resourceSpec?: Maybe<ResourceSpec>;
};

/** Input provided when getting an invite token */
export type GetInviteTokenInput = {
    /** The urn of the role to get the invite token for */
    roleUrn?: Maybe<Scalars['String']>;
};

/** Input for getting Quick Filters */
export type GetQuickFiltersInput = {
    /** Optional - A View to apply when generating results */
    viewUrn?: Maybe<Scalars['String']>;
};

/** The result object when fetching quick filters */
export type GetQuickFiltersResult = {
    __typename?: 'GetQuickFiltersResult';
    /** The list of quick filters to render in the UI */
    quickFilters: Array<Maybe<QuickFilter>>;
};

/** Input required when getting Business Glossary entities */
export type GetRootGlossaryEntitiesInput = {
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Glossary Entities in the returned result set */
    count: Scalars['Int'];
};

/** The result when getting Glossary entities */
export type GetRootGlossaryNodesResult = {
    __typename?: 'GetRootGlossaryNodesResult';
    /** A list of Glossary Nodes without a parent node */
    nodes: Array<GlossaryNode>;
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of nodes in the returned result */
    count: Scalars['Int'];
    /** The total number of nodes in the result set */
    total: Scalars['Int'];
};

/** The result when getting root GlossaryTerms */
export type GetRootGlossaryTermsResult = {
    __typename?: 'GetRootGlossaryTermsResult';
    /** A list of Glossary Terms without a parent node */
    terms: Array<GlossaryTerm>;
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of terms in the returned result */
    count: Scalars['Int'];
    /** The total number of terms in the result set */
    total: Scalars['Int'];
};

/** Input for getting schema changes computed at a specific version. */
export type GetSchemaBlameInput = {
    /** The dataset urn */
    datasetUrn: Scalars['String'];
    /** Changes after this version are not shown. If not provided, this is the latestVersion. */
    version?: Maybe<Scalars['String']>;
};

/** Schema changes computed at a specific version. */
export type GetSchemaBlameResult = {
    __typename?: 'GetSchemaBlameResult';
    /** Selected semantic version */
    version?: Maybe<SemanticVersionStruct>;
    /** List of schema blame. Absent when there are no fields to return history for. */
    schemaFieldBlameList?: Maybe<Array<SchemaFieldBlame>>;
};

/** Input for getting list of schema versions. */
export type GetSchemaVersionListInput = {
    /** The dataset urn */
    datasetUrn: Scalars['String'];
};

/** Schema changes computed at a specific version. */
export type GetSchemaVersionListResult = {
    __typename?: 'GetSchemaVersionListResult';
    /** Latest and current semantic version */
    latestVersion?: Maybe<SemanticVersionStruct>;
    /** Selected semantic version */
    version?: Maybe<SemanticVersionStruct>;
    /** All semantic versions. Absent when there are no versions. */
    semanticVersionList?: Maybe<Array<SemanticVersionStruct>>;
};

/** Input arguments for retrieving the plaintext values of a set of secrets */
export type GetSecretValuesInput = {
    /** A list of secret names */
    secrets: Array<Scalars['String']>;
};

/** Tags attached to a particular Metadata Entity */
export type GlobalTags = {
    __typename?: 'GlobalTags';
    /** The set of tags attached to the Metadata Entity */
    tags?: Maybe<Array<TagAssociation>>;
};

/**
 * Deprecated, use addTag or removeTag mutation instead
 * Update to the Tags associated with a Metadata Entity
 */
export type GlobalTagsUpdate = {
    /** The new set of tags */
    tags?: Maybe<Array<TagAssociationUpdate>>;
};

/** Global (platform-level) settings related to the Views feature */
export type GlobalViewsSettings = {
    __typename?: 'GlobalViewsSettings';
    /**
     * The global default View. If a user does not have a personal default, then
     * this will be the default view.
     */
    defaultView?: Maybe<Scalars['String']>;
};

/**
 * A Glossary Node, or a directory in a Business Glossary represents a container of
 * Glossary Terms or other Glossary Nodes
 */
export type GlossaryNode = Entity & {
    __typename?: 'GlossaryNode';
    /** The primary key of the glossary term */
    urn: Scalars['String'];
    /** Ownership metadata of the glossary term */
    ownership?: Maybe<Ownership>;
    /** A standard Entity Type */
    type: EntityType;
    /** Additional properties associated with the Glossary Term */
    properties?: Maybe<GlossaryNodeProperties>;
    /** Edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Recursively get the lineage of glossary nodes for this entity */
    parentNodes?: Maybe<ParentNodesResult>;
    /** Privileges given to a user relevant to this entity */
    privileges?: Maybe<EntityPrivileges>;
    /** Whether or not this entity exists on DataHub */
    exists?: Maybe<Scalars['Boolean']>;
};

/**
 * A Glossary Node, or a directory in a Business Glossary represents a container of
 * Glossary Terms or other Glossary Nodes
 */
export type GlossaryNodeRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Additional read only properties about a Glossary Node */
export type GlossaryNodeProperties = {
    __typename?: 'GlossaryNodeProperties';
    /** The name of the Glossary Term */
    name: Scalars['String'];
    /** Description of the glossary term */
    description?: Maybe<Scalars['String']>;
};

/**
 * A Glossary Term, or a node in a Business Glossary representing a standardized domain
 * data type
 */
export type GlossaryTerm = Entity & {
    __typename?: 'GlossaryTerm';
    /** The primary key of the glossary term */
    urn: Scalars['String'];
    /** Ownership metadata of the glossary term */
    ownership?: Maybe<Ownership>;
    /** The Domain associated with the glossary term */
    domain?: Maybe<DomainAssociation>;
    /** References to internal resources related to the Glossary Term */
    institutionalMemory?: Maybe<InstitutionalMemory>;
    /** A standard Entity Type */
    type: EntityType;
    /**
     * A unique identifier for the Glossary Term. Deprecated - Use properties.name field instead.
     * @deprecated Field no longer supported
     */
    name: Scalars['String'];
    /** hierarchicalName of glossary term */
    hierarchicalName: Scalars['String'];
    /** Additional properties associated with the Glossary Term */
    properties?: Maybe<GlossaryTermProperties>;
    /**
     * Deprecated, use properties field instead
     * Details of the Glossary Term
     */
    glossaryTermInfo?: Maybe<GlossaryTermInfo>;
    /** The deprecation status of the Glossary Term */
    deprecation?: Maybe<Deprecation>;
    /** Edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** Schema metadata of the dataset */
    schemaMetadata?: Maybe<SchemaMetadata>;
    /** Recursively get the lineage of glossary nodes for this entity */
    parentNodes?: Maybe<ParentNodesResult>;
    /** Privileges given to a user relevant to this entity */
    privileges?: Maybe<EntityPrivileges>;
    /** Whether or not this entity exists on DataHub */
    exists?: Maybe<Scalars['Boolean']>;
};

/**
 * A Glossary Term, or a node in a Business Glossary representing a standardized domain
 * data type
 */
export type GlossaryTermRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * A Glossary Term, or a node in a Business Glossary representing a standardized domain
 * data type
 */
export type GlossaryTermSchemaMetadataArgs = {
    version?: Maybe<Scalars['Long']>;
};

/**
 * An edge between a Metadata Entity and a Glossary Term Modeled as a struct to permit
 * additional attributes
 * TODO Consider whether this query should be serviced by the relationships field
 */
export type GlossaryTermAssociation = {
    __typename?: 'GlossaryTermAssociation';
    /** The glossary term itself */
    term: GlossaryTerm;
    /** Reference back to the associated urn for tracking purposes e.g. when sibling nodes are merged together */
    associatedUrn: Scalars['String'];
};

/**
 * Deprecated, use GlossaryTermProperties instead
 * Information about a glossary term
 */
export type GlossaryTermInfo = {
    __typename?: 'GlossaryTermInfo';
    /** The name of the Glossary Term */
    name?: Maybe<Scalars['String']>;
    /** Description of the glossary term */
    description?: Maybe<Scalars['String']>;
    /**
     * Definition of the glossary term. Deprecated - Use 'description' instead.
     * @deprecated Field no longer supported
     */
    definition: Scalars['String'];
    /** Term Source of the glossary term */
    termSource: Scalars['String'];
    /** Source Ref of the glossary term */
    sourceRef?: Maybe<Scalars['String']>;
    /** Source Url of the glossary term */
    sourceUrl?: Maybe<Scalars['String']>;
    /** Properties of the glossary term */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** Schema definition of glossary term */
    rawSchema?: Maybe<Scalars['String']>;
};

/** Additional read only properties about a Glossary Term */
export type GlossaryTermProperties = {
    __typename?: 'GlossaryTermProperties';
    /** The name of the Glossary Term */
    name: Scalars['String'];
    /** Description of the glossary term */
    description?: Maybe<Scalars['String']>;
    /**
     * Definition of the glossary term. Deprecated - Use 'description' instead.
     * @deprecated Field no longer supported
     */
    definition: Scalars['String'];
    /** Term Source of the glossary term */
    termSource: Scalars['String'];
    /** Source Ref of the glossary term */
    sourceRef?: Maybe<Scalars['String']>;
    /** Source Url of the glossary term */
    sourceUrl?: Maybe<Scalars['String']>;
    /** Properties of the glossary term */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** Schema definition of glossary term */
    rawSchema?: Maybe<Scalars['String']>;
};

/** Glossary Terms attached to a particular Metadata Entity */
export type GlossaryTerms = {
    __typename?: 'GlossaryTerms';
    /** The set of glossary terms attached to the Metadata Entity */
    terms?: Maybe<Array<GlossaryTermAssociation>>;
};

/** The resolved Health of an Asset */
export type Health = {
    __typename?: 'Health';
    /** An enum representing the type of health indicator */
    type: HealthStatusType;
    /** An enum representing the resolved Health status of an Asset */
    status: HealthStatus;
    /** An optional message describing the resolved health status */
    message?: Maybe<Scalars['String']>;
    /** The causes responsible for the health status */
    causes?: Maybe<Array<Scalars['String']>>;
};

export enum HealthStatus {
    /** The Asset is in a healthy state */
    Pass = 'PASS',
    /** The Asset is in a warning state */
    Warn = 'WARN',
    /** The Asset is in a failing (unhealthy) state */
    Fail = 'FAIL',
}

/** The type of the health status */
export enum HealthStatusType {
    /** Assertions status */
    Assertions = 'ASSERTIONS',
}

/** For consumption by UI only */
export type Highlight = {
    __typename?: 'Highlight';
    value: Scalars['Int'];
    title: Scalars['String'];
    body: Scalars['String'];
};

export type HyperParameterMap = {
    __typename?: 'HyperParameterMap';
    key: Scalars['String'];
    value: HyperParameterValueType;
};

export type HyperParameterValueType = StringBox | IntBox | FloatBox | BooleanBox;

/** Configurations related to Identity Management */
export type IdentityManagementConfig = {
    __typename?: 'IdentityManagementConfig';
    /** Whether identity management screen is able to be shown in the UI */
    enabled: Scalars['Boolean'];
};

/** A set of configurations for an Ingestion Source */
export type IngestionConfig = {
    __typename?: 'IngestionConfig';
    /** The JSON-encoded recipe to use for ingestion */
    recipe: Scalars['String'];
    /** Advanced: The specific executor that should handle the execution request. Defaults to 'default'. */
    executorId: Scalars['String'];
    /** Advanced: The version of the ingestion framework to use */
    version?: Maybe<Scalars['String']>;
    /** Advanced: Whether or not to run ingestion in debug mode */
    debugMode?: Maybe<Scalars['Boolean']>;
};

/** The runs associated with an Ingestion Source managed by DataHub */
export type IngestionRun = {
    __typename?: 'IngestionRun';
    /** The urn of the execution request associated with the user */
    executionRequestUrn?: Maybe<Scalars['String']>;
};

/** A schedule associated with an Ingestion Source */
export type IngestionSchedule = {
    __typename?: 'IngestionSchedule';
    /** Time Zone abbreviation (e.g. GMT, EDT). Defaults to UTC. */
    timezone?: Maybe<Scalars['String']>;
    /** The cron-formatted interval to execute the ingestion source on */
    interval: Scalars['String'];
};

/** An Ingestion Source Entity */
export type IngestionSource = {
    __typename?: 'IngestionSource';
    /** The primary key of the Ingestion Source */
    urn: Scalars['String'];
    /** The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe. */
    type: Scalars['String'];
    /** The display name of the Ingestion Source */
    name: Scalars['String'];
    /** An optional schedule associated with the Ingestion Source */
    schedule?: Maybe<IngestionSchedule>;
    /** The data platform associated with this ingestion source */
    platform?: Maybe<DataPlatform>;
    /** An type-specific set of configurations for the ingestion source */
    config: IngestionConfig;
    /** Previous requests to execute the ingestion source */
    executions?: Maybe<IngestionSourceExecutionRequests>;
};

/** An Ingestion Source Entity */
export type IngestionSourceExecutionsArgs = {
    start?: Maybe<Scalars['Int']>;
    count?: Maybe<Scalars['Int']>;
};

/** Requests for execution associated with an ingestion source */
export type IngestionSourceExecutionRequests = {
    __typename?: 'IngestionSourceExecutionRequests';
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
    /** The total number of results in the result set */
    total?: Maybe<Scalars['Int']>;
    /** The execution request objects comprising the result set */
    executionRequests: Array<ExecutionRequest>;
};

/** Input field of the chart */
export type InputField = {
    __typename?: 'InputField';
    schemaFieldUrn?: Maybe<Scalars['String']>;
    schemaField?: Maybe<SchemaField>;
};

/** Input fields of the chart */
export type InputFields = {
    __typename?: 'InputFields';
    fields?: Maybe<Array<Maybe<InputField>>>;
};

/** Institutional memory metadata, meaning internal links and pointers related to an Entity */
export type InstitutionalMemory = {
    __typename?: 'InstitutionalMemory';
    /** List of records that represent the institutional memory or internal documentation of an entity */
    elements: Array<InstitutionalMemoryMetadata>;
};

/** An institutional memory resource about a particular Metadata Entity */
export type InstitutionalMemoryMetadata = {
    __typename?: 'InstitutionalMemoryMetadata';
    /** Link to a document or wiki page or another internal resource */
    url: Scalars['String'];
    /** Label associated with the URL */
    label: Scalars['String'];
    /** The author of this metadata */
    author: CorpUser;
    /** An AuditStamp corresponding to the creation of this resource */
    created: AuditStamp;
    /**
     * Deprecated, use label instead
     * Description of the resource
     * @deprecated Field no longer supported
     */
    description: Scalars['String'];
};

/**
 * An institutional memory to add to a Metadata Entity
 * TODO Add a USER or GROUP actor enum
 */
export type InstitutionalMemoryMetadataUpdate = {
    /** Link to a document or wiki page or another internal resource */
    url: Scalars['String'];
    /** Description of the resource */
    description?: Maybe<Scalars['String']>;
    /** The corp user urn of the author of the metadata */
    author: Scalars['String'];
    /** The time at which this metadata was created */
    createdAt?: Maybe<Scalars['Long']>;
};

/** An update for the institutional memory information for a Metadata Entity */
export type InstitutionalMemoryUpdate = {
    /** The individual references in the institutional memory */
    elements: Array<InstitutionalMemoryMetadataUpdate>;
};

export type IntBox = {
    __typename?: 'IntBox';
    intValue: Scalars['Int'];
};

export type IntendedUse = {
    __typename?: 'IntendedUse';
    /** Primary Use cases for the model */
    primaryUses?: Maybe<Array<Scalars['String']>>;
    /** Primary Intended Users */
    primaryUsers?: Maybe<Array<IntendedUserType>>;
    /** Out of scope uses of the MLModel */
    outOfScopeUses?: Maybe<Array<Scalars['String']>>;
};

export enum IntendedUserType {
    /** Developed for Enterprise Users */
    Enterprise = 'ENTERPRISE',
    /** Developed for Hobbyists */
    Hobby = 'HOBBY',
    /** Developed for Entertainment Purposes */
    Entertainment = 'ENTERTAINMENT',
}

/** Token that allows users to sign up as a native user */
export type InviteToken = {
    __typename?: 'InviteToken';
    /** The invite token */
    inviteToken: Scalars['String'];
};

/** Information about a raw Key Value Schema */
export type KeyValueSchema = {
    __typename?: 'KeyValueSchema';
    /** Raw key schema */
    keySchema: Scalars['String'];
    /** Raw value schema */
    valueSchema: Scalars['String'];
};

/** Configurations related to Lineage */
export type LineageConfig = {
    __typename?: 'LineageConfig';
    /** Whether the backend support impact analysis feature */
    supportsImpactAnalysis: Scalars['Boolean'];
};

/** Direction between two nodes in the lineage graph */
export enum LineageDirection {
    /** Upstream, or left-to-right in the lineage visualization */
    Upstream = 'UPSTREAM',
    /** Downstream, or right-to-left in the lineage visualization */
    Downstream = 'DOWNSTREAM',
}

export type LineageEdge = {
    /** Urn of the source entity. This urn is downstream of the destinationUrn. */
    downstreamUrn: Scalars['String'];
    /** Urn of the destination entity. This urn is upstream of the destinationUrn */
    upstreamUrn: Scalars['String'];
};

/** Input for the list lineage property of an Entity */
export type LineageInput = {
    /** The direction of the relationship, either incoming or outgoing from the source entity */
    direction: LineageDirection;
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
    /** Optional flag to not merge siblings in the response. They are merged by default. */
    separateSiblings?: Maybe<Scalars['Boolean']>;
    /** An optional starting time to filter on */
    startTimeMillis?: Maybe<Scalars['Long']>;
    /** An optional ending time to filter on */
    endTimeMillis?: Maybe<Scalars['Long']>;
};

/** Metadata about a lineage relationship between two entities */
export type LineageRelationship = {
    __typename?: 'LineageRelationship';
    /** The type of the relationship */
    type: Scalars['String'];
    /** Entity that is related via lineage */
    entity?: Maybe<Entity>;
    /** Degree of relationship (number of hops to get to entity) */
    degree: Scalars['Int'];
    /** Timestamp for when this lineage relationship was created. Could be null. */
    createdOn?: Maybe<Scalars['Long']>;
    /** The actor who created this lineage relationship. Could be null. */
    createdActor?: Maybe<Entity>;
    /** Timestamp for when this lineage relationship was last updated. Could be null. */
    updatedOn?: Maybe<Scalars['Long']>;
    /** The actor who last updated this lineage relationship. Could be null. */
    updatedActor?: Maybe<Entity>;
    /** Whether this edge is a manual edge. Could be null. */
    isManual?: Maybe<Scalars['Boolean']>;
};

/** Parameters required to specify the page to land once clicked */
export type LinkParams = {
    __typename?: 'LinkParams';
    /** Context to define the search page */
    searchParams?: Maybe<SearchParams>;
    /** Context to define the entity profile page */
    entityProfileParams?: Maybe<EntityProfileParams>;
};

/** Input arguments for listing access tokens */
export type ListAccessTokenInput = {
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
    /** Facet filters to apply to search results */
    filters?: Maybe<Array<FacetFilterInput>>;
};

/** Results returned when listing access tokens */
export type ListAccessTokenResult = {
    __typename?: 'ListAccessTokenResult';
    /** The starting offset of the result set */
    start: Scalars['Int'];
    /** The number of results to be returned */
    count: Scalars['Int'];
    /** The total number of results in the result set */
    total: Scalars['Int'];
    /** The token metadata themselves */
    tokens: Array<AccessTokenMetadata>;
};

/** Input required when listing DataHub Domains */
export type ListDomainsInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Domains to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing DataHub Domains */
export type ListDomainsResult = {
    __typename?: 'ListDomainsResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Domains in the returned result set */
    count: Scalars['Int'];
    /** The total number of Domains in the result set */
    total: Scalars['Int'];
    /** The Domains themselves */
    domains: Array<Domain>;
};

/** Input provided when listing DataHub Global Views */
export type ListGlobalViewsInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Views to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** Input required when listing DataHub Groups */
export type ListGroupsInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Policies to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing DataHub Groups */
export type ListGroupsResult = {
    __typename?: 'ListGroupsResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Policies in the returned result set */
    count: Scalars['Int'];
    /** The total number of Policies in the result set */
    total: Scalars['Int'];
    /** The groups themselves */
    groups: Array<CorpGroup>;
};

/** Input arguments for listing Ingestion Sources */
export type ListIngestionSourcesInput = {
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
    /** An optional search query */
    query?: Maybe<Scalars['String']>;
};

/** Results returned when listing ingestion sources */
export type ListIngestionSourcesResult = {
    __typename?: 'ListIngestionSourcesResult';
    /** The starting offset of the result set */
    start: Scalars['Int'];
    /** The number of results to be returned */
    count: Scalars['Int'];
    /** The total number of results in the result set */
    total: Scalars['Int'];
    /** The Ingestion Sources themselves */
    ingestionSources: Array<IngestionSource>;
};

/** Input provided when listing DataHub Views */
export type ListMyViewsInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Views to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
    /** Optional - List the type of View to filter for. */
    viewType?: Maybe<DataHubViewType>;
};

/** Input required when listing DataHub Access Policies */
export type ListPoliciesInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Policies to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing DataHub Access Policies */
export type ListPoliciesResult = {
    __typename?: 'ListPoliciesResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Policies in the returned result set */
    count: Scalars['Int'];
    /** The total number of Policies in the result set */
    total: Scalars['Int'];
    /** The Policies themselves */
    policies: Array<Policy>;
};

/** Input provided when listing existing posts */
export type ListPostsInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Roles to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing Posts */
export type ListPostsResult = {
    __typename?: 'ListPostsResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Roles in the returned result set */
    count: Scalars['Int'];
    /** The total number of Roles in the result set */
    total: Scalars['Int'];
    /** The Posts themselves */
    posts: Array<Post>;
};

/** Input required for listing query entities */
export type ListQueriesInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Queries to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** A raw search query */
    query?: Maybe<Scalars['String']>;
    /** An optional source for the query */
    source?: Maybe<QuerySource>;
    /** An optional Urn for the parent dataset that the query is associated with. */
    datasetUrn?: Maybe<Scalars['String']>;
};

/** Results when listing entity queries */
export type ListQueriesResult = {
    __typename?: 'ListQueriesResult';
    /** The starting offset of the result set */
    start: Scalars['Int'];
    /** The number of results to be returned */
    count: Scalars['Int'];
    /** The total number of results in the result set */
    total: Scalars['Int'];
    /** The Queries themselves */
    queries: Array<QueryEntity>;
};

/** Input arguments for fetching UI recommendations */
export type ListRecommendationsInput = {
    /** Urn of the actor requesting recommendations */
    userUrn: Scalars['String'];
    /** Context provider by the caller requesting recommendations */
    requestContext?: Maybe<RecommendationRequestContext>;
    /** Max number of modules to return */
    limit?: Maybe<Scalars['Int']>;
};

/** Results returned by the ListRecommendations query */
export type ListRecommendationsResult = {
    __typename?: 'ListRecommendationsResult';
    /** List of modules to show */
    modules: Array<RecommendationModule>;
};

/** Input provided when listing existing roles */
export type ListRolesInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Roles to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing DataHub Roles */
export type ListRolesResult = {
    __typename?: 'ListRolesResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Roles in the returned result set */
    count: Scalars['Int'];
    /** The total number of Roles in the result set */
    total: Scalars['Int'];
    /** The Roles themselves */
    roles: Array<DataHubRole>;
};

/** Input for listing DataHub Secrets */
export type ListSecretsInput = {
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
    /** An optional search query */
    query?: Maybe<Scalars['String']>;
};

/** Input for listing DataHub Secrets */
export type ListSecretsResult = {
    __typename?: 'ListSecretsResult';
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
    /** The total number of results in the result set */
    total?: Maybe<Scalars['Int']>;
    /** The secrets themselves */
    secrets: Array<Secret>;
};

/** Input required when listing DataHub Tests */
export type ListTestsInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Domains to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional query string to match on */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing DataHub Tests */
export type ListTestsResult = {
    __typename?: 'ListTestsResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Tests in the returned result set */
    count: Scalars['Int'];
    /** The total number of Tests in the result set */
    total: Scalars['Int'];
    /** The Tests themselves */
    tests: Array<Test>;
};

/** Input required when listing DataHub Users */
export type ListUsersInput = {
    /** The starting offset of the result set returned */
    start?: Maybe<Scalars['Int']>;
    /** The maximum number of Policies to be returned in the result set */
    count?: Maybe<Scalars['Int']>;
    /** Optional search query */
    query?: Maybe<Scalars['String']>;
};

/** The result obtained when listing DataHub Users */
export type ListUsersResult = {
    __typename?: 'ListUsersResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Policies in the returned result set */
    count: Scalars['Int'];
    /** The total number of Policies in the result set */
    total: Scalars['Int'];
    /** The users themselves */
    users: Array<CorpUser>;
};

/** The result obtained when listing DataHub Views */
export type ListViewsResult = {
    __typename?: 'ListViewsResult';
    /** The starting offset of the result set returned */
    start: Scalars['Int'];
    /** The number of Views in the returned result set */
    count: Scalars['Int'];
    /** The total number of Views in the result set */
    total: Scalars['Int'];
    /** The Views themselves */
    views: Array<DataHubView>;
};

/** A Logical Operator, AND or OR. */
export enum LogicalOperator {
    /** An AND operator. */
    And = 'AND',
    /** An OR operator. */
    Or = 'OR',
}

/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeature = EntityWithRelationships &
    Entity & {
        __typename?: 'MLFeature';
        /** The primary key of the ML Feature */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** The display name for the ML Feature */
        name: Scalars['String'];
        /** MLFeature featureNamespace */
        featureNamespace: Scalars['String'];
        /** The description about the ML Feature */
        description?: Maybe<Scalars['String']>;
        /** MLFeature data type */
        dataType?: Maybe<MlFeatureDataType>;
        /** Ownership metadata of the MLFeature */
        ownership?: Maybe<Ownership>;
        /**
         * ModelProperties metadata of the MLFeature
         * @deprecated Field no longer supported
         */
        featureProperties?: Maybe<MlFeatureProperties>;
        /** ModelProperties metadata of the MLFeature */
        properties?: Maybe<MlFeatureProperties>;
        /** References to internal resources related to the MLFeature */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** Status metadata of the MLFeature */
        status?: Maybe<Status>;
        /** Deprecation */
        deprecation?: Maybe<Deprecation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** Tags applied to entity */
        tags?: Maybe<GlobalTags>;
        /** The structured glossary terms associated with the entity */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the entity */
        domain?: Maybe<DomainAssociation>;
        /** An additional set of of read write properties */
        editableProperties?: Maybe<MlFeatureEditableProperties>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeatureRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeatureLineageArgs = {
    input: LineageInput;
};

/** The data type associated with an individual Machine Learning Feature */
export enum MlFeatureDataType {
    Useless = 'USELESS',
    Nominal = 'NOMINAL',
    Ordinal = 'ORDINAL',
    Binary = 'BINARY',
    Count = 'COUNT',
    Time = 'TIME',
    Interval = 'INTERVAL',
    Image = 'IMAGE',
    Video = 'VIDEO',
    Audio = 'AUDIO',
    Text = 'TEXT',
    Map = 'MAP',
    Sequence = 'SEQUENCE',
    Set = 'SET',
    Continuous = 'CONTINUOUS',
    Byte = 'BYTE',
    Unknown = 'UNKNOWN',
}

export type MlFeatureEditableProperties = {
    __typename?: 'MLFeatureEditableProperties';
    /** The edited description */
    description?: Maybe<Scalars['String']>;
};

export type MlFeatureProperties = {
    __typename?: 'MLFeatureProperties';
    description?: Maybe<Scalars['String']>;
    dataType?: Maybe<MlFeatureDataType>;
    version?: Maybe<VersionTag>;
    sources?: Maybe<Array<Maybe<Dataset>>>;
};

/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTable = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'MLFeatureTable';
        /** The primary key of the ML Feature Table */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** The display name */
        name: Scalars['String'];
        /** Standardized platform urn where the MLFeatureTable is defined */
        platform: DataPlatform;
        /** MLFeatureTable description */
        description?: Maybe<Scalars['String']>;
        /** Ownership metadata of the MLFeatureTable */
        ownership?: Maybe<Ownership>;
        /** Additional read only properties associated the the ML Feature Table */
        properties?: Maybe<MlFeatureTableProperties>;
        /**
         * Deprecated, use properties field instead
         * ModelProperties metadata of the MLFeature
         * @deprecated Field no longer supported
         */
        featureTableProperties?: Maybe<MlFeatureTableProperties>;
        /** References to internal resources related to the MLFeature */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** Status metadata of the MLFeatureTable */
        status?: Maybe<Status>;
        /** Deprecation */
        deprecation?: Maybe<Deprecation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the ML Feature Table. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /** Tags applied to entity */
        tags?: Maybe<GlobalTags>;
        /** The structured glossary terms associated with the entity */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the entity */
        domain?: Maybe<DomainAssociation>;
        /** An additional set of of read write properties */
        editableProperties?: Maybe<MlFeatureTableEditableProperties>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTableRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTableLineageArgs = {
    input: LineageInput;
};

export type MlFeatureTableEditableProperties = {
    __typename?: 'MLFeatureTableEditableProperties';
    /** The edited description */
    description?: Maybe<Scalars['String']>;
};

export type MlFeatureTableProperties = {
    __typename?: 'MLFeatureTableProperties';
    description?: Maybe<Scalars['String']>;
    mlFeatures?: Maybe<Array<Maybe<MlFeature>>>;
    mlPrimaryKeys?: Maybe<Array<Maybe<MlPrimaryKey>>>;
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
};

export type MlHyperParam = {
    __typename?: 'MLHyperParam';
    name?: Maybe<Scalars['String']>;
    description?: Maybe<Scalars['String']>;
    value?: Maybe<Scalars['String']>;
    createdAt?: Maybe<Scalars['Long']>;
};

export type MlMetric = {
    __typename?: 'MLMetric';
    name?: Maybe<Scalars['String']>;
    description?: Maybe<Scalars['String']>;
    value?: Maybe<Scalars['String']>;
    createdAt?: Maybe<Scalars['Long']>;
};

/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModel = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'MLModel';
        /** The primary key of the ML model */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** ML model display name */
        name: Scalars['String'];
        /** Standardized platform urn where the MLModel is defined */
        platform: DataPlatform;
        /** Fabric type where mlmodel belongs to or where it was generated */
        origin: FabricType;
        /** Human readable description for mlmodel */
        description?: Maybe<Scalars['String']>;
        /**
         * Deprecated, use tags field instead
         * The standard tags for the ML Model
         * @deprecated Field no longer supported
         */
        globalTags?: Maybe<GlobalTags>;
        /** The standard tags for the ML Model */
        tags?: Maybe<GlobalTags>;
        /** Ownership metadata of the mlmodel */
        ownership?: Maybe<Ownership>;
        /** Additional read only information about the ML Model */
        properties?: Maybe<MlModelProperties>;
        /** Intended use of the mlmodel */
        intendedUse?: Maybe<IntendedUse>;
        /** Factors metadata of the mlmodel */
        factorPrompts?: Maybe<MlModelFactorPrompts>;
        /** Metrics metadata of the mlmodel */
        metrics?: Maybe<Metrics>;
        /** Evaluation Data of the mlmodel */
        evaluationData?: Maybe<Array<BaseData>>;
        /** Training Data of the mlmodel */
        trainingData?: Maybe<Array<BaseData>>;
        /** Quantitative Analyses of the mlmodel */
        quantitativeAnalyses?: Maybe<QuantitativeAnalyses>;
        /** Ethical Considerations of the mlmodel */
        ethicalConsiderations?: Maybe<EthicalConsiderations>;
        /** Caveats and Recommendations of the mlmodel */
        caveatsAndRecommendations?: Maybe<CaveatsAndRecommendations>;
        /** References to internal resources related to the mlmodel */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** Source Code */
        sourceCode?: Maybe<SourceCode>;
        /** Status metadata of the mlmodel */
        status?: Maybe<Status>;
        /** Cost Aspect of the mlmodel */
        cost?: Maybe<Cost>;
        /** Deprecation */
        deprecation?: Maybe<Deprecation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the ML Model. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /** The structured glossary terms associated with the entity */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the entity */
        domain?: Maybe<DomainAssociation>;
        /** An additional set of of read write properties */
        editableProperties?: Maybe<MlModelEditableProperties>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModelRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModelLineageArgs = {
    input: LineageInput;
};

export type MlModelEditableProperties = {
    __typename?: 'MLModelEditableProperties';
    /** The edited description */
    description?: Maybe<Scalars['String']>;
};

export type MlModelFactorPrompts = {
    __typename?: 'MLModelFactorPrompts';
    /** What are foreseeable salient factors for which MLModel performance may vary, and how were these determined */
    relevantFactors?: Maybe<Array<MlModelFactors>>;
    /** Which factors are being reported, and why were these chosen */
    evaluationFactors?: Maybe<Array<MlModelFactors>>;
};

export type MlModelFactors = {
    __typename?: 'MLModelFactors';
    /** Distinct categories with similar characteristics that are present in the evaluation data instances */
    groups?: Maybe<Array<Scalars['String']>>;
    /** Instrumentation used for MLModel */
    instrumentation?: Maybe<Array<Scalars['String']>>;
    /** Environment in which the MLModel is deployed */
    environment?: Maybe<Array<Scalars['String']>>;
};

/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroup = EntityWithRelationships &
    Entity &
    BrowsableEntity & {
        __typename?: 'MLModelGroup';
        /** The primary key of the ML Model Group */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** The display name for the Entity */
        name: Scalars['String'];
        /** Standardized platform urn where the MLModelGroup is defined */
        platform: DataPlatform;
        /** Fabric type where MLModelGroup belongs to or where it was generated */
        origin: FabricType;
        /** Human readable description for MLModelGroup */
        description?: Maybe<Scalars['String']>;
        /** Additional read only properties about the ML Model Group */
        properties?: Maybe<MlModelGroupProperties>;
        /** Ownership metadata of the MLModelGroup */
        ownership?: Maybe<Ownership>;
        /** Status metadata of the MLModelGroup */
        status?: Maybe<Status>;
        /** Deprecation */
        deprecation?: Maybe<Deprecation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** The browse paths corresponding to the ML Model Group. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /** Tags applied to entity */
        tags?: Maybe<GlobalTags>;
        /** The structured glossary terms associated with the entity */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the entity */
        domain?: Maybe<DomainAssociation>;
        /** An additional set of of read write properties */
        editableProperties?: Maybe<MlModelGroupEditableProperties>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroupRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroupLineageArgs = {
    input: LineageInput;
};

export type MlModelGroupEditableProperties = {
    __typename?: 'MLModelGroupEditableProperties';
    /** The edited description */
    description?: Maybe<Scalars['String']>;
};

export type MlModelGroupProperties = {
    __typename?: 'MLModelGroupProperties';
    description?: Maybe<Scalars['String']>;
    createdAt?: Maybe<Scalars['Long']>;
    version?: Maybe<VersionTag>;
};

export type MlModelProperties = {
    __typename?: 'MLModelProperties';
    description?: Maybe<Scalars['String']>;
    date?: Maybe<Scalars['Long']>;
    version?: Maybe<Scalars['String']>;
    type?: Maybe<Scalars['String']>;
    hyperParameters?: Maybe<HyperParameterMap>;
    hyperParams?: Maybe<Array<Maybe<MlHyperParam>>>;
    trainingMetrics?: Maybe<Array<Maybe<MlMetric>>>;
    mlFeatures?: Maybe<Array<Scalars['String']>>;
    tags?: Maybe<Array<Scalars['String']>>;
    groups?: Maybe<Array<Maybe<MlModelGroup>>>;
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    externalUrl?: Maybe<Scalars['String']>;
};

/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKey = EntityWithRelationships &
    Entity & {
        __typename?: 'MLPrimaryKey';
        /** The primary key of the ML Primary Key */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The timestamp for the last time this entity was ingested */
        lastIngested?: Maybe<Scalars['Long']>;
        /** The display name */
        name: Scalars['String'];
        /** MLPrimaryKey featureNamespace */
        featureNamespace: Scalars['String'];
        /** MLPrimaryKey description */
        description?: Maybe<Scalars['String']>;
        /** MLPrimaryKey data type */
        dataType?: Maybe<MlFeatureDataType>;
        /** Additional read only properties of the ML Primary Key */
        properties?: Maybe<MlPrimaryKeyProperties>;
        /**
         * Deprecated, use properties field instead
         * MLPrimaryKeyProperties
         * @deprecated Field no longer supported
         */
        primaryKeyProperties?: Maybe<MlPrimaryKeyProperties>;
        /** Ownership metadata of the MLPrimaryKey */
        ownership?: Maybe<Ownership>;
        /** References to internal resources related to the MLPrimaryKey */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** Status metadata of the MLPrimaryKey */
        status?: Maybe<Status>;
        /** Deprecation */
        deprecation?: Maybe<Deprecation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Granular API for querying edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Edges extending from this entity grouped by direction in the lineage graph */
        lineage?: Maybe<EntityLineageResult>;
        /** Tags applied to entity */
        tags?: Maybe<GlobalTags>;
        /** The structured glossary terms associated with the entity */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** The Domain associated with the entity */
        domain?: Maybe<DomainAssociation>;
        /** An additional set of of read write properties */
        editableProperties?: Maybe<MlPrimaryKeyEditableProperties>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKeyRelationshipsArgs = {
    input: RelationshipsInput;
};

/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKeyLineageArgs = {
    input: LineageInput;
};

export type MlPrimaryKeyEditableProperties = {
    __typename?: 'MLPrimaryKeyEditableProperties';
    /** The edited description */
    description?: Maybe<Scalars['String']>;
};

export type MlPrimaryKeyProperties = {
    __typename?: 'MLPrimaryKeyProperties';
    description?: Maybe<Scalars['String']>;
    dataType?: Maybe<MlFeatureDataType>;
    version?: Maybe<VersionTag>;
    sources?: Maybe<Array<Maybe<Dataset>>>;
};

/** Configurations related to managed, UI based ingestion */
export type ManagedIngestionConfig = {
    __typename?: 'ManagedIngestionConfig';
    /** Whether ingestion screen is enabled in the UI */
    enabled: Scalars['Boolean'];
};

/** An overview of the field that was matched in the entity search document */
export type MatchedField = {
    __typename?: 'MatchedField';
    /** Name of the field that matched */
    name: Scalars['String'];
    /** Value of the field that matched */
    value: Scalars['String'];
};

/** Media content */
export type Media = {
    __typename?: 'Media';
    /** The type of media */
    type: MediaType;
    /** The location of the media (a URL) */
    location: Scalars['String'];
};

/** The type of media */
export enum MediaType {
    /** An image */
    Image = 'IMAGE',
}

/** Input to fetch metadata analytics charts */
export type MetadataAnalyticsInput = {
    /** Entity type to fetch analytics for (If empty, queries across all entities) */
    entityType?: Maybe<EntityType>;
    /** Urn of the domain to fetch analytics for (If empty or GLOBAL, queries across all domains) */
    domain?: Maybe<Scalars['String']>;
    /** Search query to filter down result (If empty, does not apply any search query) */
    query?: Maybe<Scalars['String']>;
};

export type Metrics = {
    __typename?: 'Metrics';
    /** Measures of ML Model performance */
    performanceMeasures?: Maybe<Array<Scalars['String']>>;
    /** Decision Thresholds used if any */
    decisionThreshold?: Maybe<Array<Scalars['String']>>;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type Mutation = {
    __typename?: 'Mutation';
    /** Accept role using invite token */
    acceptRole?: Maybe<Scalars['Boolean']>;
    /** Add members to a group */
    addGroupMembers?: Maybe<Scalars['Boolean']>;
    /** Add a link, or institutional memory, from a particular Entity */
    addLink?: Maybe<Scalars['Boolean']>;
    /** Add an owner to a particular Entity */
    addOwner?: Maybe<Scalars['Boolean']>;
    /** Add multiple owners to a particular Entity */
    addOwners?: Maybe<Scalars['Boolean']>;
    /** Add multiple related Terms to a Glossary Term to establish relationships */
    addRelatedTerms?: Maybe<Scalars['Boolean']>;
    /** Add a tag to a particular Entity or subresource */
    addTag?: Maybe<Scalars['Boolean']>;
    /** Add multiple tags to a particular Entity or subresource */
    addTags?: Maybe<Scalars['Boolean']>;
    /** Add a glossary term to a particular Entity or subresource */
    addTerm?: Maybe<Scalars['Boolean']>;
    /** Add multiple glossary terms to a particular Entity or subresource */
    addTerms?: Maybe<Scalars['Boolean']>;
    /** Add owners to multiple Entities */
    batchAddOwners?: Maybe<Scalars['Boolean']>;
    /** Add tags to multiple Entities or subresources */
    batchAddTags?: Maybe<Scalars['Boolean']>;
    /** Add glossary terms to multiple Entities or subresource */
    batchAddTerms?: Maybe<Scalars['Boolean']>;
    /** Batch assign roles to users */
    batchAssignRole?: Maybe<Scalars['Boolean']>;
    /** Remove owners from multiple Entities */
    batchRemoveOwners?: Maybe<Scalars['Boolean']>;
    /** Remove tags from multiple Entities or subresource */
    batchRemoveTags?: Maybe<Scalars['Boolean']>;
    /** Remove glossary terms from multiple Entities or subresource */
    batchRemoveTerms?: Maybe<Scalars['Boolean']>;
    /** Set domain for multiple Entities */
    batchSetDomain?: Maybe<Scalars['Boolean']>;
    /** Updates the deprecation status for a batch of assets. */
    batchUpdateDeprecation?: Maybe<Scalars['Boolean']>;
    /** Updates the soft deleted status for a batch of assets */
    batchUpdateSoftDeleted?: Maybe<Scalars['Boolean']>;
    /** Batch update the state for a set of steps. */
    batchUpdateStepStates: BatchUpdateStepStatesResult;
    /** Cancel a running execution request, provided the urn of the original execution request */
    cancelIngestionExecutionRequest?: Maybe<Scalars['String']>;
    /** Generates an access token for DataHub APIs for a particular user & of a particular type */
    createAccessToken?: Maybe<AccessToken>;
    /**
     * Create a new Domain. Returns the urn of the newly created Domain. Requires the 'Create Domains' or 'Manage Domains' Platform Privilege. If a Domain with the provided ID already exists,
     * it will be overwritten.
     */
    createDomain?: Maybe<Scalars['String']>;
    /** Create a new GlossaryNode. Returns the urn of the newly created GlossaryNode. If a node with the provided ID already exists, it will be overwritten. */
    createGlossaryNode?: Maybe<Scalars['String']>;
    /** Create a new GlossaryTerm. Returns the urn of the newly created GlossaryTerm. If a term with the provided ID already exists, it will be overwritten. */
    createGlossaryTerm?: Maybe<Scalars['String']>;
    /** Create a new group. Returns the urn of the newly created group. Requires the Manage Users & Groups Platform Privilege */
    createGroup?: Maybe<Scalars['String']>;
    /**
     * Create a request to execute an ingestion job
     * input: Input required for creating an ingestion execution request
     */
    createIngestionExecutionRequest?: Maybe<Scalars['String']>;
    /** Create a new ingestion source */
    createIngestionSource?: Maybe<Scalars['String']>;
    /** Create invite token */
    createInviteToken?: Maybe<InviteToken>;
    /** Generates a token that can be shared with existing native users to reset their credentials. */
    createNativeUserResetToken?: Maybe<ResetToken>;
    /** Create a policy and returns the resulting urn */
    createPolicy?: Maybe<Scalars['String']>;
    /** Create a post */
    createPost?: Maybe<Scalars['Boolean']>;
    /** Create a new Query */
    createQuery?: Maybe<QueryEntity>;
    /** Create a new Secret */
    createSecret?: Maybe<Scalars['String']>;
    /**
     * Create a new tag. Requires the 'Manage Tags' or 'Create Tags' Platform Privilege. If a Tag with the provided ID already exists,
     * it will be overwritten.
     */
    createTag?: Maybe<Scalars['String']>;
    /** Create a new test */
    createTest?: Maybe<Scalars['String']>;
    /**
     * Create a request to execute a test ingestion connection job
     * input: Input required for creating a test connection request
     */
    createTestConnectionRequest?: Maybe<Scalars['String']>;
    /** Create a new DataHub View (Saved Filter) */
    createView?: Maybe<DataHubView>;
    /** Remove an assertion associated with an entity. Requires the 'Edit Assertions' privilege on the entity. */
    deleteAssertion?: Maybe<Scalars['Boolean']>;
    /** Delete a Domain */
    deleteDomain?: Maybe<Scalars['Boolean']>;
    /** Remove a glossary entity (GlossaryTerm or GlossaryNode). Return boolean whether it was successful or not. */
    deleteGlossaryEntity?: Maybe<Scalars['Boolean']>;
    /** Delete an existing ingestion source */
    deleteIngestionSource?: Maybe<Scalars['String']>;
    /** Remove an existing policy and returns the policy urn */
    deletePolicy?: Maybe<Scalars['String']>;
    /** Delete a post */
    deletePost?: Maybe<Scalars['Boolean']>;
    /** Delete a Query by urn. This requires the 'Edit Queries' Metadata Privilege. */
    deleteQuery?: Maybe<Scalars['Boolean']>;
    /** Delete a Secret */
    deleteSecret?: Maybe<Scalars['String']>;
    /** Delete a Tag */
    deleteTag?: Maybe<Scalars['Boolean']>;
    /** Delete an existing test - note that this will NOT delete dangling pointers until the next execution of the test. */
    deleteTest?: Maybe<Scalars['Boolean']>;
    /** Delete a DataHub View (Saved Filter) */
    deleteView?: Maybe<Scalars['Boolean']>;
    /** Remove a group. Requires Manage Users & Groups Platform Privilege */
    removeGroup?: Maybe<Scalars['Boolean']>;
    /** Remove members from a group */
    removeGroupMembers?: Maybe<Scalars['Boolean']>;
    /** Remove a link, or institutional memory, from a particular Entity */
    removeLink?: Maybe<Scalars['Boolean']>;
    /** Remove an owner from a particular Entity */
    removeOwner?: Maybe<Scalars['Boolean']>;
    /** Remove multiple related Terms for a Glossary Term */
    removeRelatedTerms?: Maybe<Scalars['Boolean']>;
    /** Remove a tag from a particular Entity or subresource */
    removeTag?: Maybe<Scalars['Boolean']>;
    /** Remove a glossary term from a particular Entity or subresource */
    removeTerm?: Maybe<Scalars['Boolean']>;
    /** Remove a user. Requires Manage Users & Groups Platform Privilege */
    removeUser?: Maybe<Scalars['Boolean']>;
    /** Report a new operation for an asset */
    reportOperation?: Maybe<Scalars['String']>;
    /** Revokes access tokens. */
    revokeAccessToken: Scalars['Boolean'];
    /** Rollback a specific ingestion execution run based on its runId */
    rollbackIngestion?: Maybe<Scalars['String']>;
    /** Sets the Domain for a Dataset, Chart, Dashboard, Data Flow (Pipeline), or Data Job (Task). Returns true if the Domain was successfully added, or already exists. Requires the Edit Domains privilege for the Entity. */
    setDomain?: Maybe<Scalars['Boolean']>;
    /** Set the hex color associated with an existing Tag */
    setTagColor?: Maybe<Scalars['Boolean']>;
    /** Sets the Domain for a Dataset, Chart, Dashboard, Data Flow (Pipeline), or Data Job (Task). Returns true if the Domain was successfully removed, or was already removed. Requires the Edit Domains privilege for an asset. */
    unsetDomain?: Maybe<Scalars['Boolean']>;
    /** Update the metadata about a particular Chart */
    updateChart?: Maybe<Chart>;
    /** Update a particular Corp Group's editable properties */
    updateCorpGroupProperties?: Maybe<CorpGroup>;
    /** Update a particular Corp User's editable properties */
    updateCorpUserProperties?: Maybe<CorpUser>;
    /** Update the View-related settings for a user. */
    updateCorpUserViewsSettings?: Maybe<Scalars['Boolean']>;
    /** Update the metadata about a particular Dashboard */
    updateDashboard?: Maybe<Dashboard>;
    /** Update the metadata about a particular Data Flow (Pipeline) */
    updateDataFlow?: Maybe<DataFlow>;
    /** Update the metadata about a particular Data Job (Task) */
    updateDataJob?: Maybe<DataJob>;
    /** Update the metadata about a particular Dataset */
    updateDataset?: Maybe<Dataset>;
    /** Update the metadata about a batch of Datasets */
    updateDatasets?: Maybe<Array<Maybe<Dataset>>>;
    /** Sets the Deprecation status for a Metadata Entity. Requires the Edit Deprecation status privilege for an entity. */
    updateDeprecation?: Maybe<Scalars['Boolean']>;
    /** Incubating. Updates the description of a resource. Currently only supports Dataset Schema Fields, Containers */
    updateDescription?: Maybe<Scalars['Boolean']>;
    /** Update the Embed information for a Dataset, Dashboard, or Chart. */
    updateEmbed?: Maybe<Scalars['Boolean']>;
    /**
     * Update the global settings related to the Views feature.
     * Requires the 'Manage Global Views' Platform Privilege.
     */
    updateGlobalViewsSettings: Scalars['Boolean'];
    /** Update an existing ingestion source */
    updateIngestionSource?: Maybe<Scalars['String']>;
    /** Update lineage for an entity */
    updateLineage?: Maybe<Scalars['Boolean']>;
    /** Updates the name of the entity. */
    updateName?: Maybe<Scalars['Boolean']>;
    /** Update the metadata about a particular Notebook */
    updateNotebook?: Maybe<Notebook>;
    /** Updates the parent node of a resource. Currently only GlossaryNodes and GlossaryTerms have parentNodes. */
    updateParentNode?: Maybe<Scalars['Boolean']>;
    /** Update an existing policy and returns the resulting urn */
    updatePolicy?: Maybe<Scalars['String']>;
    /** Update an existing Query */
    updateQuery?: Maybe<QueryEntity>;
    /** Update the information about a particular Entity Tag */
    updateTag?: Maybe<Tag>;
    /** Update an existing test */
    updateTest?: Maybe<Scalars['String']>;
    /** Update a user setting */
    updateUserSetting?: Maybe<Scalars['Boolean']>;
    /** Change the status of a user. Requires Manage Users & Groups Platform Privilege */
    updateUserStatus?: Maybe<Scalars['String']>;
    /** Delete a DataHub View (Saved Filter) */
    updateView?: Maybe<DataHubView>;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAcceptRoleArgs = {
    input: AcceptRoleInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddGroupMembersArgs = {
    input: AddGroupMembersInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddLinkArgs = {
    input: AddLinkInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddOwnerArgs = {
    input: AddOwnerInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddOwnersArgs = {
    input: AddOwnersInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddRelatedTermsArgs = {
    input: RelatedTermsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddTagArgs = {
    input: TagAssociationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddTagsArgs = {
    input: AddTagsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddTermArgs = {
    input: TermAssociationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationAddTermsArgs = {
    input: AddTermsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchAddOwnersArgs = {
    input: BatchAddOwnersInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchAddTagsArgs = {
    input: BatchAddTagsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchAddTermsArgs = {
    input: BatchAddTermsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchAssignRoleArgs = {
    input: BatchAssignRoleInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchRemoveOwnersArgs = {
    input: BatchRemoveOwnersInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchRemoveTagsArgs = {
    input: BatchRemoveTagsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchRemoveTermsArgs = {
    input: BatchRemoveTermsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchSetDomainArgs = {
    input: BatchSetDomainInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchUpdateDeprecationArgs = {
    input: BatchUpdateDeprecationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchUpdateSoftDeletedArgs = {
    input: BatchUpdateSoftDeletedInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchUpdateStepStatesArgs = {
    input: BatchUpdateStepStatesInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCancelIngestionExecutionRequestArgs = {
    input: CancelIngestionExecutionRequestInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateAccessTokenArgs = {
    input: CreateAccessTokenInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateDomainArgs = {
    input: CreateDomainInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateGlossaryNodeArgs = {
    input: CreateGlossaryEntityInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateGlossaryTermArgs = {
    input: CreateGlossaryEntityInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateGroupArgs = {
    input: CreateGroupInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateIngestionExecutionRequestArgs = {
    input: CreateIngestionExecutionRequestInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateIngestionSourceArgs = {
    input: UpdateIngestionSourceInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateInviteTokenArgs = {
    input: CreateInviteTokenInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateNativeUserResetTokenArgs = {
    input: CreateNativeUserResetTokenInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreatePolicyArgs = {
    input: PolicyUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreatePostArgs = {
    input: CreatePostInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateQueryArgs = {
    input: CreateQueryInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateSecretArgs = {
    input: CreateSecretInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateTagArgs = {
    input: CreateTagInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateTestArgs = {
    input: CreateTestInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateTestConnectionRequestArgs = {
    input: CreateTestConnectionRequestInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateViewArgs = {
    input: CreateViewInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteAssertionArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteDomainArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteGlossaryEntityArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteIngestionSourceArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeletePolicyArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeletePostArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteQueryArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteSecretArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteTagArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteTestArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteViewArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveGroupArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveGroupMembersArgs = {
    input: RemoveGroupMembersInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveLinkArgs = {
    input: RemoveLinkInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveOwnerArgs = {
    input: RemoveOwnerInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveRelatedTermsArgs = {
    input: RelatedTermsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveTagArgs = {
    input: TagAssociationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveTermArgs = {
    input: TermAssociationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveUserArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationReportOperationArgs = {
    input: ReportOperationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRevokeAccessTokenArgs = {
    tokenId: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRollbackIngestionArgs = {
    input: RollbackIngestionInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationSetDomainArgs = {
    entityUrn: Scalars['String'];
    domainUrn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationSetTagColorArgs = {
    urn: Scalars['String'];
    colorHex: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUnsetDomainArgs = {
    entityUrn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateChartArgs = {
    urn: Scalars['String'];
    input: ChartUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateCorpGroupPropertiesArgs = {
    urn: Scalars['String'];
    input: CorpGroupUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateCorpUserPropertiesArgs = {
    urn: Scalars['String'];
    input: CorpUserUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateCorpUserViewsSettingsArgs = {
    input: UpdateCorpUserViewsSettingsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDashboardArgs = {
    urn: Scalars['String'];
    input: DashboardUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDataFlowArgs = {
    urn: Scalars['String'];
    input: DataFlowUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDataJobArgs = {
    urn: Scalars['String'];
    input: DataJobUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDatasetArgs = {
    urn: Scalars['String'];
    input: DatasetUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDatasetsArgs = {
    input: Array<BatchDatasetUpdateInput>;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDeprecationArgs = {
    input: UpdateDeprecationInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDescriptionArgs = {
    input: DescriptionUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateEmbedArgs = {
    input: UpdateEmbedInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateGlobalViewsSettingsArgs = {
    input: UpdateGlobalViewsSettingsInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateIngestionSourceArgs = {
    urn: Scalars['String'];
    input: UpdateIngestionSourceInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateLineageArgs = {
    input: UpdateLineageInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateNameArgs = {
    input: UpdateNameInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateNotebookArgs = {
    urn: Scalars['String'];
    input: NotebookUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateParentNodeArgs = {
    input: UpdateParentNodeInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdatePolicyArgs = {
    urn: Scalars['String'];
    input: PolicyUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateQueryArgs = {
    urn: Scalars['String'];
    input: UpdateQueryInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateTagArgs = {
    urn: Scalars['String'];
    input: TagUpdateInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateTestArgs = {
    urn: Scalars['String'];
    input: UpdateTestInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateUserSettingArgs = {
    input: UpdateUserSettingInput;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateUserStatusArgs = {
    urn: Scalars['String'];
    status: CorpUserStatus;
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateViewArgs = {
    urn: Scalars['String'];
    input: UpdateViewInput;
};

/** For consumption by UI only */
export type NamedBar = {
    __typename?: 'NamedBar';
    name: Scalars['String'];
    segments: Array<BarSegment>;
};

/** For consumption by UI only */
export type NamedLine = {
    __typename?: 'NamedLine';
    name: Scalars['String'];
    data: Array<NumericDataPoint>;
};

/** A Notebook Metadata Entity */
export type Notebook = Entity &
    BrowsableEntity & {
        __typename?: 'Notebook';
        /** The primary key of the Notebook */
        urn: Scalars['String'];
        /** A standard Entity Type */
        type: EntityType;
        /** The Notebook tool name */
        tool: Scalars['String'];
        /** An id unique within the Notebook tool */
        notebookId: Scalars['String'];
        /** Additional read only information about the Notebook */
        info?: Maybe<NotebookInfo>;
        /** Additional read write properties about the Notebook */
        editableProperties?: Maybe<NotebookEditableProperties>;
        /** Ownership metadata of the Notebook */
        ownership?: Maybe<Ownership>;
        /** Status metadata of the Notebook */
        status?: Maybe<Status>;
        /** The content of this Notebook */
        content: NotebookContent;
        /** The tags associated with the Notebook */
        tags?: Maybe<GlobalTags>;
        /** References to internal resources related to the Notebook */
        institutionalMemory?: Maybe<InstitutionalMemory>;
        /** The Domain associated with the Notebook */
        domain?: Maybe<DomainAssociation>;
        /** The specific instance of the data platform that this entity belongs to */
        dataPlatformInstance?: Maybe<DataPlatformInstance>;
        /** Edges extending from this entity */
        relationships?: Maybe<EntityRelationshipsResult>;
        /** Sub Types that this entity implements */
        subTypes?: Maybe<SubTypes>;
        /** The structured glossary terms associated with the notebook */
        glossaryTerms?: Maybe<GlossaryTerms>;
        /** Standardized platform. */
        platform: DataPlatform;
        /** The browse paths corresponding to the Notebook. If no Browse Paths have been generated before, this will be null. */
        browsePaths?: Maybe<Array<BrowsePath>>;
        /** Whether or not this entity exists on DataHub */
        exists?: Maybe<Scalars['Boolean']>;
    };

/** A Notebook Metadata Entity */
export type NotebookRelationshipsArgs = {
    input: RelationshipsInput;
};

/** The Union of every NotebookCell */
export type NotebookCell = {
    __typename?: 'NotebookCell';
    /** The chart cell content. The will be non-null only when all other cell field is null. */
    chartCell?: Maybe<ChartCell>;
    /** The text cell content. The will be non-null only when all other cell field is null. */
    textCell?: Maybe<TextCell>;
    /** The query cell content. The will be non-null only when all other cell field is null. */
    queryChell?: Maybe<QueryCell>;
    /** The type of this Notebook cell */
    type: NotebookCellType;
};

/** The type for a NotebookCell */
export enum NotebookCellType {
    /** TEXT Notebook cell type. The cell context is text only. */
    TextCell = 'TEXT_CELL',
    /** QUERY Notebook cell type. The cell context is query only. */
    QueryCell = 'QUERY_CELL',
    /** CHART Notebook cell type. The cell content is chart only. */
    ChartCell = 'CHART_CELL',
}

/** The actual content in a Notebook */
export type NotebookContent = {
    __typename?: 'NotebookContent';
    /** The content of a Notebook which is composed by a list of NotebookCell */
    cells: Array<NotebookCell>;
};

/**
 * Notebook properties that are editable via the UI This represents logical metadata,
 * as opposed to technical metadata
 */
export type NotebookEditableProperties = {
    __typename?: 'NotebookEditableProperties';
    /** Description of the Notebook */
    description?: Maybe<Scalars['String']>;
};

/** Update to writable Notebook fields */
export type NotebookEditablePropertiesUpdate = {
    /** Writable description aka documentation for a Notebook */
    description: Scalars['String'];
};

/** Additional read only information about a Notebook */
export type NotebookInfo = {
    __typename?: 'NotebookInfo';
    /** Display of the Notebook */
    title?: Maybe<Scalars['String']>;
    /** Description of the Notebook */
    description?: Maybe<Scalars['String']>;
    /** Native platform URL of the Notebook */
    externalUrl?: Maybe<Scalars['String']>;
    /** A list of platform specific metadata tuples */
    customProperties?: Maybe<Array<CustomPropertiesEntry>>;
    /** Captures information about who created/last modified/deleted this Notebook and when */
    changeAuditStamps?: Maybe<ChangeAuditStamps>;
};

/** Arguments provided to update a Notebook Entity */
export type NotebookUpdateInput = {
    /** Update to ownership */
    ownership?: Maybe<OwnershipUpdate>;
    /** Update to tags */
    tags?: Maybe<GlobalTagsUpdate>;
    /** Update to editable properties */
    editableProperties?: Maybe<NotebookEditablePropertiesUpdate>;
};

/** For consumption by UI only */
export type NumericDataPoint = {
    __typename?: 'NumericDataPoint';
    x: Scalars['String'];
    y: Scalars['Int'];
};

/** Operational info for an entity. */
export type Operation = TimeSeriesAspect & {
    __typename?: 'Operation';
    /** The time at which the operation was reported */
    timestampMillis: Scalars['Long'];
    /** Actor who issued this operation. */
    actor?: Maybe<Scalars['String']>;
    /** Operation type of change. */
    operationType: OperationType;
    /** A custom operation type */
    customOperationType?: Maybe<Scalars['String']>;
    /** Source of the operation */
    sourceType?: Maybe<OperationSourceType>;
    /** How many rows were affected by this operation. */
    numAffectedRows?: Maybe<Scalars['Long']>;
    /** Which other datasets were affected by this operation. */
    affectedDatasets?: Maybe<Array<Scalars['String']>>;
    /** When time at which the asset was actually updated */
    lastUpdatedTimestamp: Scalars['Long'];
    /** Optional partition identifier */
    partition?: Maybe<Scalars['String']>;
    /** Custom operation properties */
    customProperties?: Maybe<Array<StringMapEntry>>;
};

/** Enum to define the source/reporter type for an Operation. */
export enum OperationSourceType {
    /** A data process reported the operation. */
    DataProcess = 'DATA_PROCESS',
    /** A data platform reported the operation. */
    DataPlatform = 'DATA_PLATFORM',
}

/** Enum to define the operation type when an entity changes. */
export enum OperationType {
    /** When data is inserted. */
    Insert = 'INSERT',
    /** When data is updated. */
    Update = 'UPDATE',
    /** When data is deleted. */
    Delete = 'DELETE',
    /** When table is created. */
    Create = 'CREATE',
    /** When table is altered */
    Alter = 'ALTER',
    /** When table is dropped */
    Drop = 'DROP',
    /** Unknown operation */
    Unknown = 'UNKNOWN',
    /** Custom */
    Custom = 'CUSTOM',
}

/** Carries information about where an entity originated from. */
export type Origin = {
    __typename?: 'Origin';
    /** Where an entity originated from. Either NATIVE or EXTERNAL */
    type: OriginType;
    /** Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the identity provider. */
    externalType?: Maybe<Scalars['String']>;
};

/** Enum to define where an entity originated from. */
export enum OriginType {
    /** The entity is native to DataHub. */
    Native = 'NATIVE',
    /** The entity is external to DataHub. */
    External = 'EXTERNAL',
    /** The entity is of unknown origin. */
    Unknown = 'UNKNOWN',
}

/** An owner of a Metadata Entity */
export type Owner = {
    __typename?: 'Owner';
    /** Owner object */
    owner: OwnerType;
    /** The type of the ownership */
    type: OwnershipType;
    /** Source information for the ownership */
    source?: Maybe<OwnershipSource>;
    /** Reference back to the owned urn for tracking purposes e.g. when sibling nodes are merged together */
    associatedUrn: Scalars['String'];
};

/** Entities that are able to own other entities */
export enum OwnerEntityType {
    /** A corp user owner */
    CorpUser = 'CORP_USER',
    /** A corp group owner */
    CorpGroup = 'CORP_GROUP',
}

/** Input provided when adding an owner to an asset */
export type OwnerInput = {
    /** The primary key of the Owner to add or remove */
    ownerUrn: Scalars['String'];
    /** The owner type, either a user or group */
    ownerEntityType: OwnerEntityType;
    /** The ownership type for the new owner. If none is provided, then a new NONE will be added. */
    type?: Maybe<OwnershipType>;
};

/** An owner of a Metadata Entity, either a user or group */
export type OwnerType = CorpUser | CorpGroup;

/**
 * An owner to add to a Metadata Entity
 * TODO Add a USER or GROUP actor enum
 */
export type OwnerUpdate = {
    /** The owner URN, either a corpGroup or corpuser */
    owner: Scalars['String'];
    /** The owner type */
    type: OwnershipType;
};

/** Ownership information about a Metadata Entity */
export type Ownership = {
    __typename?: 'Ownership';
    /** List of owners of the entity */
    owners?: Maybe<Array<Owner>>;
    /** Audit stamp containing who last modified the record and when */
    lastModified: AuditStamp;
};

/** Information about the source of Ownership metadata about a Metadata Entity */
export type OwnershipSource = {
    __typename?: 'OwnershipSource';
    /** The type of the source */
    type: OwnershipSourceType;
    /** An optional reference URL for the source */
    url?: Maybe<Scalars['String']>;
};

/** The origin of Ownership metadata associated with a Metadata Entity */
export enum OwnershipSourceType {
    /** Auditing system or audit logs */
    Audit = 'AUDIT',
    /** Database, eg GRANTS table */
    Database = 'DATABASE',
    /** File system, eg file or directory owner */
    FileSystem = 'FILE_SYSTEM',
    /** Issue tracking system, eg Jira */
    IssueTrackingSystem = 'ISSUE_TRACKING_SYSTEM',
    /** Manually provided by a user */
    Manual = 'MANUAL',
    /** Other ownership like service, eg Nuage, ACL service etc */
    Service = 'SERVICE',
    /** SCM system, eg GIT, SVN */
    SourceControl = 'SOURCE_CONTROL',
    /** Other sources */
    Other = 'OTHER',
}

/**
 * The type of the ownership relationship between a Person and a Metadata Entity
 * Note that this field will soon become deprecated due to low usage
 */
export enum OwnershipType {
    /** A person or group who is responsible for technical aspects of the asset. */
    TechnicalOwner = 'TECHNICAL_OWNER',
    /** A person or group who is responsible for logical, or business related, aspects of the asset. */
    BusinessOwner = 'BUSINESS_OWNER',
    /** A steward, expert, or delegate responsible for the asset. */
    DataSteward = 'DATA_STEWARD',
    /** No specific type associated with the owner. */
    None = 'NONE',
    /**
     * A person or group that owns the data.
     * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
     */
    Dataowner = 'DATAOWNER',
    /**
     * A person or group that is in charge of developing the code
     * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
     */
    Developer = 'DEVELOPER',
    /**
     * A person or a group that overseas the operation, eg a DBA or SRE
     * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
     */
    Delegate = 'DELEGATE',
    /**
     * A person, group, or service that produces or generates the data
     * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
     */
    Producer = 'PRODUCER',
    /**
     * A person or a group that has direct business interest
     * Deprecated! Use BUSINESS_OWNER instead.
     */
    Stakeholder = 'STAKEHOLDER',
    /**
     * A person, group, or service that consumes the data
     * Deprecated! This ownership type is no longer supported.
     */
    Consumer = 'CONSUMER',
}

/** An update for the ownership information for a Metadata Entity */
export type OwnershipUpdate = {
    /** The updated list of owners */
    owners: Array<OwnerUpdate>;
};

/** All of the parent containers for a given entity. Returns parents with direct parent first followed by the parent's parent etc. */
export type ParentContainersResult = {
    __typename?: 'ParentContainersResult';
    /** The number of containers bubbling up for this entity */
    count: Scalars['Int'];
    /** A list of parent containers in order from direct parent, to parent's parent etc. If there are no containers, return an emty list */
    containers: Array<Container>;
};

/** All of the parent nodes for GlossaryTerms and GlossaryNodes */
export type ParentNodesResult = {
    __typename?: 'ParentNodesResult';
    /** The number of parent nodes bubbling up for this entity */
    count: Scalars['Int'];
    /** A list of parent nodes in order from direct parent, to parent's parent etc. If there are no nodes, return an empty list */
    nodes: Array<GlossaryNode>;
};

/** Information about the partition being profiled */
export type PartitionSpec = {
    __typename?: 'PartitionSpec';
    /** The partition type */
    type: PartitionType;
    /** The partition identifier */
    partition: Scalars['String'];
    /** The optional time window partition information */
    timePartition?: Maybe<TimeWindow>;
};

export enum PartitionType {
    FullTable = 'FULL_TABLE',
    Query = 'QUERY',
    Partition = 'PARTITION',
}

/**
 * Deprecated, do not use this type
 * The logical type associated with an individual Dataset
 */
export enum PlatformNativeType {
    /** Table */
    Table = 'TABLE',
    /** View */
    View = 'VIEW',
    /** Directory in file system */
    Directory = 'DIRECTORY',
    /** Stream */
    Stream = 'STREAM',
    /** Bucket in key value store */
    Bucket = 'BUCKET',
}

/** The platform privileges that the currently authenticated user has */
export type PlatformPrivileges = {
    __typename?: 'PlatformPrivileges';
    /** Whether the user should be able to view analytics */
    viewAnalytics: Scalars['Boolean'];
    /** Whether the user should be able to manage policies */
    managePolicies: Scalars['Boolean'];
    /** Whether the user should be able to manage users & groups */
    manageIdentities: Scalars['Boolean'];
    /** Whether the user should be able to generate personal access tokens */
    generatePersonalAccessTokens: Scalars['Boolean'];
    /** Whether the user should be able to create new Domains */
    createDomains: Scalars['Boolean'];
    /** Whether the user should be able to manage Domains */
    manageDomains: Scalars['Boolean'];
    /** Whether the user is able to manage UI-based ingestion */
    manageIngestion: Scalars['Boolean'];
    /** Whether the user is able to manage UI-based secrets */
    manageSecrets: Scalars['Boolean'];
    /** Whether the user should be able to manage tokens on behalf of other users. */
    manageTokens: Scalars['Boolean'];
    /** Whether the user is able to manage Tests */
    manageTests: Scalars['Boolean'];
    /** Whether the user should be able to manage Glossaries */
    manageGlossaries: Scalars['Boolean'];
    /** Whether the user is able to manage user credentials */
    manageUserCredentials: Scalars['Boolean'];
    /** Whether the user should be able to create new Tags */
    createTags: Scalars['Boolean'];
    /** Whether the user should be able to create and delete all Tags */
    manageTags: Scalars['Boolean'];
    /** Whether the user should be able to create, update, and delete global views. */
    manageGlobalViews: Scalars['Boolean'];
};

/** A type of Schema, either a table schema or a key value schema */
export type PlatformSchema = TableSchema | KeyValueSchema;

/** The category of a specific Data Platform */
export enum PlatformType {
    /** Value for a file system */
    FileSystem = 'FILE_SYSTEM',
    /** Value for a key value store */
    KeyValueStore = 'KEY_VALUE_STORE',
    /** Value for a message broker */
    MessageBroker = 'MESSAGE_BROKER',
    /** Value for an object store */
    ObjectStore = 'OBJECT_STORE',
    /** Value for an OLAP datastore */
    OlapDatastore = 'OLAP_DATASTORE',
    /** Value for a query engine */
    QueryEngine = 'QUERY_ENGINE',
    /** Value for a relational database */
    RelationalDb = 'RELATIONAL_DB',
    /** Value for a search engine */
    SearchEngine = 'SEARCH_ENGINE',
    /** Value for other platforms */
    Others = 'OTHERS',
}

/** Configurations related to the Policies Feature */
export type PoliciesConfig = {
    __typename?: 'PoliciesConfig';
    /** Whether the policies feature is enabled and should be displayed in the UI */
    enabled: Scalars['Boolean'];
    /** A list of platform privileges to display in the Policy Builder experience */
    platformPrivileges: Array<Privilege>;
    /** A list of resource privileges to display in the Policy Builder experience */
    resourcePrivileges: Array<ResourcePrivileges>;
};

/**
 * DEPRECATED
 * TODO: Eventually get rid of this in favor of DataHub Policy
 * An DataHub Platform Access Policy Access Policies determine who can perform what actions against which resources on the platform
 */
export type Policy = {
    __typename?: 'Policy';
    /** The primary key of the Policy */
    urn: Scalars['String'];
    /** The type of the Policy */
    type: PolicyType;
    /** The name of the Policy */
    name: Scalars['String'];
    /** The present state of the Policy */
    state: PolicyState;
    /** The description of the Policy */
    description?: Maybe<Scalars['String']>;
    /** The resources that the Policy privileges apply to */
    resources?: Maybe<ResourceFilter>;
    /** The privileges that the Policy grants */
    privileges: Array<Scalars['String']>;
    /** The actors that the Policy grants privileges to */
    actors: ActorFilter;
    /** Whether the Policy is editable, ie system policies, or not */
    editable: Scalars['Boolean'];
};

/** Match condition */
export enum PolicyMatchCondition {
    /** Whether the field matches the value */
    Equals = 'EQUALS',
}

/** Criterion to define relationship between field and values */
export type PolicyMatchCriterion = {
    __typename?: 'PolicyMatchCriterion';
    /**
     * The name of the field that the criterion refers to
     * e.g. entity_type, entity_urn, domain
     */
    field: Scalars['String'];
    /** Values. Matches criterion if any one of the values matches condition (OR-relationship) */
    values: Array<PolicyMatchCriterionValue>;
    /** The name of the field that the criterion refers to */
    condition: PolicyMatchCondition;
};

/** Criterion to define relationship between field and values */
export type PolicyMatchCriterionInput = {
    /**
     * The name of the field that the criterion refers to
     * e.g. entity_type, entity_urn, domain
     */
    field: Scalars['String'];
    /** Values. Matches criterion if any one of the values matches condition (OR-relationship) */
    values: Array<Scalars['String']>;
    /** The name of the field that the criterion refers to */
    condition: PolicyMatchCondition;
};

/** Value in PolicyMatchCriterion with hydrated entity if value is urn */
export type PolicyMatchCriterionValue = {
    __typename?: 'PolicyMatchCriterionValue';
    /** The value of the field to match */
    value: Scalars['String'];
    /** Hydrated entities of the above values. Only set if the value is an urn */
    entity?: Maybe<Entity>;
};

/** Filter object that encodes a complex filter logic with OR + AND */
export type PolicyMatchFilter = {
    __typename?: 'PolicyMatchFilter';
    /** List of criteria to apply */
    criteria?: Maybe<Array<PolicyMatchCriterion>>;
};

/** Filter object that encodes a complex filter logic with OR + AND */
export type PolicyMatchFilterInput = {
    /** List of criteria to apply */
    criteria?: Maybe<Array<PolicyMatchCriterionInput>>;
};

/** The state of an Access Policy */
export enum PolicyState {
    /**
     * A Policy that has not been officially created, but in progress
     * Currently unused
     */
    Draft = 'DRAFT',
    /** A Policy that is active and being enforced */
    Active = 'ACTIVE',
    /** A Policy that is not active or being enforced */
    Inactive = 'INACTIVE',
}

/** The type of the Access Policy */
export enum PolicyType {
    /** An access policy that grants privileges pertaining to Metadata Entities */
    Metadata = 'METADATA',
    /** An access policy that grants top level administrative privileges pertaining to the DataHub Platform itself */
    Platform = 'PLATFORM',
}

/** Input provided when creating or updating an Access Policy */
export type PolicyUpdateInput = {
    /** The Policy Type */
    type: PolicyType;
    /** The Policy name */
    name: Scalars['String'];
    /** The Policy state */
    state: PolicyState;
    /** A Policy description */
    description?: Maybe<Scalars['String']>;
    /** The set of resources that the Policy privileges apply to */
    resources?: Maybe<ResourceFilterInput>;
    /** The set of privileges that the Policy grants */
    privileges: Array<Scalars['String']>;
    /** The set of actors that the Policy privileges are granted to */
    actors: ActorFilterInput;
};

/** Input provided when creating a Post */
export type Post = Entity & {
    __typename?: 'Post';
    /** The primary key of the Post */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** Granular API for querying edges extending from the Post */
    relationships?: Maybe<EntityRelationshipsResult>;
    /** The type of post */
    postType: PostType;
    /** The content of the post */
    content: PostContent;
    /** When the post was last modified */
    lastModified: AuditStamp;
};

/** Input provided when creating a Post */
export type PostRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Post content */
export type PostContent = {
    __typename?: 'PostContent';
    /** The type of post content */
    contentType: PostContentType;
    /** The title of the post */
    title: Scalars['String'];
    /** Optional content of the post */
    description?: Maybe<Scalars['String']>;
    /** Optional link that the post is associated with */
    link?: Maybe<Scalars['String']>;
    /** Optional media contained in the post */
    media?: Maybe<Media>;
};

/** The type of post */
export enum PostContentType {
    /** Text content */
    Text = 'TEXT',
    /** Link content */
    Link = 'LINK',
}

/** The type of post */
export enum PostType {
    /** Posts on the home page */
    HomePageAnnouncement = 'HOME_PAGE_ANNOUNCEMENT',
}

/** An individual DataHub Access Privilege */
export type Privilege = {
    __typename?: 'Privilege';
    /** Standardized privilege type, serving as a unique identifier for a privilege eg EDIT_ENTITY */
    type: Scalars['String'];
    /** The name to appear when displaying the privilege, eg Edit Entity */
    displayName?: Maybe<Scalars['String']>;
    /** A description of the privilege to display */
    description?: Maybe<Scalars['String']>;
};

/** Object that encodes the privileges the actor has for a given resource */
export type Privileges = {
    __typename?: 'Privileges';
    /** Granted Privileges */
    privileges: Array<Scalars['String']>;
};

export type QuantitativeAnalyses = {
    __typename?: 'QuantitativeAnalyses';
    /** Link to a dashboard with results showing how the model performed with respect to each factor */
    unitaryResults?: Maybe<ResultsType>;
    /** Link to a dashboard with results showing how the model performed with respect to the intersection of evaluated factors */
    intersectionalResults?: Maybe<ResultsType>;
};

/** Configuration for the queries tab */
export type QueriesTabConfig = {
    __typename?: 'QueriesTabConfig';
    /** Number of queries to show in the queries tab */
    queriesTabResultSize?: Maybe<Scalars['Int']>;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type Query = {
    __typename?: 'Query';
    /**
     * Fetch configurations
     * Used by DataHub UI
     */
    appConfig?: Maybe<AppConfig>;
    /** Fetch an Assertion by primary key (urn) */
    assertion?: Maybe<Assertion>;
    /** Autocomplete a search query against a specific DataHub Entity Type */
    autoComplete?: Maybe<AutoCompleteResults>;
    /** Autocomplete a search query against a specific set of DataHub Entity Types */
    autoCompleteForMultiple?: Maybe<AutoCompleteMultipleResults>;
    /** Batch fetch the state for a set of steps. */
    batchGetStepStates: BatchGetStepStatesResult;
    /**
     * Hierarchically browse a specific type of DataHub Entity by path
     * Used by explore in the UI
     */
    browse?: Maybe<BrowseResults>;
    /** Retrieve the browse paths corresponding to an entity */
    browsePaths?: Maybe<Array<BrowsePath>>;
    /** Fetch a Chart by primary key (urn) */
    chart?: Maybe<Chart>;
    /** Fetch an Entity Container by primary key (urn) */
    container?: Maybe<Container>;
    /** Fetch a CorpGroup, representing a DataHub platform group by primary key (urn) */
    corpGroup?: Maybe<CorpGroup>;
    /** Fetch a CorpUser, representing a DataHub platform user, by primary key (urn) */
    corpUser?: Maybe<CorpUser>;
    /** Fetch a Dashboard by primary key (urn) */
    dashboard?: Maybe<Dashboard>;
    /** Fetch a Data Flow (or Data Pipeline) by primary key (urn) */
    dataFlow?: Maybe<DataFlow>;
    /** Fetch a Data Job (or Data Task) by primary key (urn) */
    dataJob?: Maybe<DataJob>;
    /** Fetch a Data Platform by primary key (urn) */
    dataPlatform?: Maybe<DataPlatform>;
    /** Fetch a Dataset by primary key (urn) */
    dataset?: Maybe<Dataset>;
    /** Fetch a Domain by primary key (urn) */
    domain?: Maybe<Domain>;
    /** Gets entities based on their urns */
    entities?: Maybe<Array<Maybe<Entity>>>;
    /** Gets an entity based on its urn */
    entity?: Maybe<Entity>;
    /** Get whether or not not an entity exists */
    entityExists?: Maybe<Scalars['Boolean']>;
    /**
     * Get an execution request
     * urn: The primary key associated with the execution request.
     */
    executionRequest?: Maybe<ExecutionRequest>;
    /**
     * Generates an access token for DataHub APIs for a particular user & of a particular type
     * Deprecated, use createAccessToken instead
     */
    getAccessToken?: Maybe<AccessToken>;
    /** Retrieves a set of server driven Analytics Charts to render in the UI */
    getAnalyticsCharts: Array<AnalyticsChartGroup>;
    /** Fetches the number of entities ingested by type */
    getEntityCounts?: Maybe<EntityCountResults>;
    /** Get all granted privileges for the given actor and resource */
    getGrantedPrivileges?: Maybe<Privileges>;
    /** Retrieves a set of server driven Analytics Highlight Cards to render in the UI */
    getHighlights: Array<Highlight>;
    /** Get invite token */
    getInviteToken?: Maybe<InviteToken>;
    /** Retrieves a set of charts regarding the ingested metadata */
    getMetadataAnalyticsCharts: Array<AnalyticsChartGroup>;
    /** Get quick filters to display in auto-complete */
    getQuickFilters?: Maybe<GetQuickFiltersResult>;
    /** Get all GlossaryNodes without a parentNode */
    getRootGlossaryNodes?: Maybe<GetRootGlossaryNodesResult>;
    /** Get all GlossaryTerms without a parentNode */
    getRootGlossaryTerms?: Maybe<GetRootGlossaryTermsResult>;
    /** Returns the most recent changes made to each column in a dataset at each dataset version. */
    getSchemaBlame?: Maybe<GetSchemaBlameResult>;
    /** Returns the list of schema versions for a dataset. */
    getSchemaVersionList?: Maybe<GetSchemaVersionListResult>;
    /**
     * Fetch the values of a set of secrets. The caller must have the MANAGE_SECRETS
     * privilege to use.
     */
    getSecretValues?: Maybe<Array<SecretValue>>;
    /**
     * Fetch the Global Settings related to the Views feature.
     * Requires the 'Manage Global Views' Platform Privilege.
     */
    globalViewsSettings?: Maybe<GlobalViewsSettings>;
    /** Fetch a Glossary Node by primary key (urn) */
    glossaryNode?: Maybe<GlossaryNode>;
    /** Fetch a Glossary Term by primary key (urn) */
    glossaryTerm?: Maybe<GlossaryTerm>;
    /**
     * Fetch a specific ingestion source
     * urn: The primary key associated with the ingestion source.
     */
    ingestionSource?: Maybe<IngestionSource>;
    /**
     * Deprecated, use appConfig Query instead
     * Whether the analytics feature is enabled in the UI
     * @deprecated Field no longer supported
     */
    isAnalyticsEnabled: Scalars['Boolean'];
    /** List access tokens stored in DataHub. */
    listAccessTokens: ListAccessTokenResult;
    /** List all DataHub Domains */
    listDomains?: Maybe<ListDomainsResult>;
    /** List Global DataHub Views */
    listGlobalViews?: Maybe<ListViewsResult>;
    /** List all DataHub Groups */
    listGroups?: Maybe<ListGroupsResult>;
    /** List all ingestion sources */
    listIngestionSources?: Maybe<ListIngestionSourcesResult>;
    /** List DataHub Views owned by the current user */
    listMyViews?: Maybe<ListViewsResult>;
    /** List all DataHub Access Policies */
    listPolicies?: Maybe<ListPoliciesResult>;
    /** List all Posts */
    listPosts?: Maybe<ListPostsResult>;
    /** List Dataset Queries */
    listQueries?: Maybe<ListQueriesResult>;
    /** Fetch recommendations for a particular scenario */
    listRecommendations?: Maybe<ListRecommendationsResult>;
    /** List all DataHub Roles */
    listRoles?: Maybe<ListRolesResult>;
    /** List all secrets stored in DataHub (no values) */
    listSecrets?: Maybe<ListSecretsResult>;
    /** List all DataHub Tests */
    listTests?: Maybe<ListTestsResult>;
    /** List all DataHub Users */
    listUsers?: Maybe<ListUsersResult>;
    /** Fetch details associated with the authenticated user, provided via an auth cookie or header */
    me?: Maybe<AuthenticatedUser>;
    /** Incubating: Fetch a ML Feature by primary key (urn) */
    mlFeature?: Maybe<MlFeature>;
    /** Incubating: Fetch a ML Feature Table by primary key (urn) */
    mlFeatureTable?: Maybe<MlFeatureTable>;
    /** Incubating: Fetch an ML Model by primary key (urn) */
    mlModel?: Maybe<MlModel>;
    /** Incubating: Fetch an ML Model Group by primary key (urn) */
    mlModelGroup?: Maybe<MlModelGroup>;
    /** Incubating: Fetch a ML Primary Key by primary key (urn) */
    mlPrimaryKey?: Maybe<MlPrimaryKey>;
    /** Fetch a Notebook by primary key (urn) */
    notebook?: Maybe<Notebook>;
    /** Search DataHub entities by providing a pointer reference for scrolling through results. */
    scrollAcrossEntities?: Maybe<ScrollResults>;
    /** Search across the results of a graph query on a node, uses scroll API */
    scrollAcrossLineage?: Maybe<ScrollAcrossLineageResults>;
    /** Full text search against a specific DataHub Entity Type */
    search?: Maybe<SearchResults>;
    /** Search DataHub entities */
    searchAcrossEntities?: Maybe<SearchResults>;
    /** Search across the results of a graph query on a node */
    searchAcrossLineage?: Maybe<SearchAcrossLineageResults>;
    /** Fetch a Tag by primary key (urn) */
    tag?: Maybe<Tag>;
    /** Fetch a Test by primary key (urn) */
    test?: Maybe<Test>;
    /** Fetch a Dataset by primary key (urn) at a point in time based on aspect versions (versionStamp) */
    versionedDataset?: Maybe<VersionedDataset>;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryAssertionArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryAutoCompleteArgs = {
    input: AutoCompleteInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryAutoCompleteForMultipleArgs = {
    input: AutoCompleteMultipleInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryBatchGetStepStatesArgs = {
    input: BatchGetStepStatesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryBrowseArgs = {
    input: BrowseInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryBrowsePathsArgs = {
    input: BrowsePathsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryChartArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryContainerArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryCorpGroupArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryCorpUserArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDashboardArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDataFlowArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDataJobArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDataPlatformArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDatasetArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDomainArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryEntitiesArgs = {
    urns: Array<Scalars['String']>;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryEntityArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryEntityExistsArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryExecutionRequestArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetAccessTokenArgs = {
    input: GetAccessTokenInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetEntityCountsArgs = {
    input?: Maybe<EntityCountInput>;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetGrantedPrivilegesArgs = {
    input: GetGrantedPrivilegesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetInviteTokenArgs = {
    input: GetInviteTokenInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetMetadataAnalyticsChartsArgs = {
    input: MetadataAnalyticsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetQuickFiltersArgs = {
    input: GetQuickFiltersInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetRootGlossaryNodesArgs = {
    input: GetRootGlossaryEntitiesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetRootGlossaryTermsArgs = {
    input: GetRootGlossaryEntitiesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetSchemaBlameArgs = {
    input: GetSchemaBlameInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetSchemaVersionListArgs = {
    input: GetSchemaVersionListInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetSecretValuesArgs = {
    input: GetSecretValuesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGlossaryNodeArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGlossaryTermArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryIngestionSourceArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListAccessTokensArgs = {
    input: ListAccessTokenInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListDomainsArgs = {
    input: ListDomainsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListGlobalViewsArgs = {
    input: ListGlobalViewsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListGroupsArgs = {
    input: ListGroupsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListIngestionSourcesArgs = {
    input: ListIngestionSourcesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListMyViewsArgs = {
    input: ListMyViewsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListPoliciesArgs = {
    input: ListPoliciesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListPostsArgs = {
    input: ListPostsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListQueriesArgs = {
    input: ListQueriesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListRecommendationsArgs = {
    input: ListRecommendationsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListRolesArgs = {
    input: ListRolesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListSecretsArgs = {
    input: ListSecretsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListTestsArgs = {
    input: ListTestsInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListUsersArgs = {
    input: ListUsersInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryMlFeatureArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryMlFeatureTableArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryMlModelArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryMlModelGroupArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryMlPrimaryKeyArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryNotebookArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryScrollAcrossEntitiesArgs = {
    input: ScrollAcrossEntitiesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryScrollAcrossLineageArgs = {
    input: ScrollAcrossLineageInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QuerySearchArgs = {
    input: SearchInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QuerySearchAcrossEntitiesArgs = {
    input: SearchAcrossEntitiesInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QuerySearchAcrossLineageArgs = {
    input: SearchAcrossLineageInput;
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryTagArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryTestArgs = {
    urn: Scalars['String'];
};

/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryVersionedDatasetArgs = {
    urn: Scalars['String'];
    versionStamp?: Maybe<Scalars['String']>;
};

/** A Notebook cell which contains Query as content */
export type QueryCell = {
    __typename?: 'QueryCell';
    /** Title of the cell */
    cellTitle: Scalars['String'];
    /** Unique id for the cell. */
    cellId: Scalars['String'];
    /** Captures information about who created/last modified/deleted this TextCell and when */
    changeAuditStamps?: Maybe<ChangeAuditStamps>;
    /** Raw query to explain some specific logic in a Notebook */
    rawQuery: Scalars['String'];
    /** Captures information about who last executed this query cell and when */
    lastExecuted?: Maybe<AuditStamp>;
};

/** An individual Query */
export type QueryEntity = Entity & {
    __typename?: 'QueryEntity';
    /** A primary key associated with the Query */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** Properties about the Query */
    properties?: Maybe<QueryProperties>;
    /** Subjects for the query */
    subjects?: Maybe<Array<QuerySubject>>;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/** An individual Query */
export type QueryEntityRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A query language / dialect. */
export enum QueryLanguage {
    /** Standard ANSI SQL */
    Sql = 'SQL',
}

/** Properties about an individual Query */
export type QueryProperties = {
    __typename?: 'QueryProperties';
    /** The Query statement itself */
    statement: QueryStatement;
    /** The source of the Query */
    source: QuerySource;
    /** The name of the Query */
    name?: Maybe<Scalars['String']>;
    /** The description of the Query */
    description?: Maybe<Scalars['String']>;
    /** An Audit Stamp corresponding to the creation of this resource */
    created: AuditStamp;
    /** An Audit Stamp corresponding to the update of this resource */
    lastModified: AuditStamp;
};

/** The source of the query */
export enum QuerySource {
    /** The query was provided manually, e.g. from the UI. */
    Manual = 'MANUAL',
}

/** An individual Query Statement */
export type QueryStatement = {
    __typename?: 'QueryStatement';
    /** The query statement value */
    value: Scalars['String'];
    /** The language for the Query Statement */
    language: QueryLanguage;
};

/** Input required for creating a Query Statement */
export type QueryStatementInput = {
    /** The query text */
    value: Scalars['String'];
    /** The query language */
    language: QueryLanguage;
};

/** The subject for a Query */
export type QuerySubject = {
    __typename?: 'QuerySubject';
    /** The dataset which is the subject of the Query */
    dataset: Dataset;
};

/** A quick filter in search and auto-complete */
export type QuickFilter = {
    __typename?: 'QuickFilter';
    /** Name of field to filter by */
    field: Scalars['String'];
    /** Value to filter on */
    value: Scalars['String'];
    /** Entity that the value maps to if any */
    entity?: Maybe<Entity>;
};

/** Payload representing data about a single aspect */
export type RawAspect = {
    __typename?: 'RawAspect';
    /** The name of the aspect */
    aspectName: Scalars['String'];
    /** JSON string containing the aspect's payload */
    payload?: Maybe<Scalars['String']>;
    /** Details for the frontend on how the raw aspect should be rendered */
    renderSpec?: Maybe<AspectRenderSpec>;
};

/** Content to display within each recommendation module */
export type RecommendationContent = {
    __typename?: 'RecommendationContent';
    /** String representation of content */
    value: Scalars['String'];
    /** Entity being recommended. Empty if the content being recommended is not an entity */
    entity?: Maybe<Entity>;
    /** Additional context required to generate the the recommendation */
    params?: Maybe<RecommendationParams>;
};

export type RecommendationModule = {
    __typename?: 'RecommendationModule';
    /** Title of the module to display */
    title: Scalars['String'];
    /** Unique id of the module being recommended */
    moduleId: Scalars['String'];
    /** Type of rendering that defines how the module should be rendered */
    renderType: RecommendationRenderType;
    /** List of content to display inside the module */
    content: Array<RecommendationContent>;
};

/** Parameters required to render a recommendation of a given type */
export type RecommendationParams = {
    __typename?: 'RecommendationParams';
    /** Context to define the search recommendations */
    searchParams?: Maybe<SearchParams>;
    /** Context to define the entity profile page */
    entityProfileParams?: Maybe<EntityProfileParams>;
    /** Context about the recommendation */
    contentParams?: Maybe<ContentParams>;
};

/**
 * Enum that defines how the modules should be rendered.
 * There should be two frontend implementation of large and small modules per type.
 */
export enum RecommendationRenderType {
    /** Simple list of entities */
    EntityNameList = 'ENTITY_NAME_LIST',
    /** List of platforms */
    PlatformSearchList = 'PLATFORM_SEARCH_LIST',
    /** Tag search list */
    TagSearchList = 'TAG_SEARCH_LIST',
    /** A list of recommended search queries */
    SearchQueryList = 'SEARCH_QUERY_LIST',
    /** Glossary Term search list */
    GlossaryTermSearchList = 'GLOSSARY_TERM_SEARCH_LIST',
    /** Domain Search List */
    DomainSearchList = 'DOMAIN_SEARCH_LIST',
}

/**
 * Context that defines the page requesting recommendations
 * i.e. for search pages, the query/filters. for entity pages, the entity urn and tab
 */
export type RecommendationRequestContext = {
    /** Scenario in which the recommendations will be displayed */
    scenario: ScenarioType;
    /** Additional context for defining the search page requesting recommendations */
    searchRequestContext?: Maybe<SearchRequestContext>;
    /** Additional context for defining the entity page requesting recommendations */
    entityRequestContext?: Maybe<EntityRequestContext>;
};

/** Input provided when adding Terms to an asset */
export type RelatedTermsInput = {
    /** The Glossary Term urn to add or remove this relationship to/from */
    urn: Scalars['String'];
    /** The primary key of the Glossary Term to add or remove */
    termUrns: Array<Scalars['String']>;
    /** The type of relationship we're adding or removing to/from for a Glossary Term */
    relationshipType: TermRelationshipType;
};

/** Direction between a source and destination node */
export enum RelationshipDirection {
    /** A directed edge pointing at the source Entity */
    Incoming = 'INCOMING',
    /** A directed edge pointing at the destination Entity */
    Outgoing = 'OUTGOING',
}

/** Input for the list relationships field of an Entity */
export type RelationshipsInput = {
    /** The types of relationships to query, representing an OR */
    types: Array<Scalars['String']>;
    /** The direction of the relationship, either incoming or outgoing from the source entity */
    direction: RelationshipDirection;
    /** The starting offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of results to be returned */
    count?: Maybe<Scalars['Int']>;
};

/** Input required to remove members from an external DataHub group */
export type RemoveGroupMembersInput = {
    /** The group to remove members from */
    groupUrn: Scalars['String'];
    /** The members to remove from the group */
    userUrns: Array<Scalars['String']>;
};

/** Input provided when removing the association between a Metadata Entity and a Link */
export type RemoveLinkInput = {
    /** The url of the link to add or remove, which uniquely identifies the Link */
    linkUrl: Scalars['String'];
    /** The urn of the resource or entity to attach the link to, for example a dataset urn */
    resourceUrn: Scalars['String'];
};

/** Input required to remove members from a native DataHub group */
export type RemoveNativeGroupMembersInput = {
    /** The group to remove members from */
    groupUrn: Scalars['String'];
    /** The members to remove from the group */
    userUrns: Array<Scalars['String']>;
};

/** Input provided when removing the association between a Metadata Entity and an user or group owner */
export type RemoveOwnerInput = {
    /** The primary key of the Owner to add or remove */
    ownerUrn: Scalars['String'];
    /** The urn of the resource or entity to attach or remove the owner from, for example a dataset urn */
    resourceUrn: Scalars['String'];
};

/** Input provided to report an asset operation */
export type ReportOperationInput = {
    /** The urn of the asset (e.g. dataset) to report the operation for */
    urn: Scalars['String'];
    /** The type of operation that was performed. Required */
    operationType: OperationType;
    /** A custom type of operation. Required if operation type is CUSTOM. */
    customOperationType?: Maybe<Scalars['String']>;
    /** The source or reporter of the operation */
    sourceType: OperationSourceType;
    /** A list of key-value parameters to include */
    customProperties?: Maybe<Array<StringMapEntryInput>>;
    /** An optional partition identifier */
    partition?: Maybe<Scalars['String']>;
    /** Optional: The number of affected rows */
    numAffectedRows?: Maybe<Scalars['Long']>;
    /**
     * Optional: Provide a timestamp associated with the operation. If not provided, one will be generated for you based
     * on the current time.
     */
    timestampMillis?: Maybe<Scalars['Long']>;
};

/** Token that allows native users to reset their credentials */
export type ResetToken = {
    __typename?: 'ResetToken';
    /** The reset token */
    resetToken: Scalars['String'];
};

/** The resources that a DataHub Access Policy applies to */
export type ResourceFilter = {
    __typename?: 'ResourceFilter';
    /** The type of the resource the policy should apply to Not required because in the future we want to support filtering by type OR by domain */
    type?: Maybe<Scalars['String']>;
    /** A list of specific resource urns to apply the filter to */
    resources?: Maybe<Array<Scalars['String']>>;
    /** Whether of not to apply the filter to all resources of the type */
    allResources?: Maybe<Scalars['Boolean']>;
    /** Whether of not to apply the filter to all resources of the type */
    filter?: Maybe<PolicyMatchFilter>;
};

/** Input required when creating or updating an Access Policies Determines which resources the Policy applies to */
export type ResourceFilterInput = {
    /**
     * The type of the resource the policy should apply to
     * Not required because in the future we want to support filtering by type OR by domain
     */
    type?: Maybe<Scalars['String']>;
    /** A list of specific resource urns to apply the filter to */
    resources?: Maybe<Array<Scalars['String']>>;
    /** Whether of not to apply the filter to all resources of the type */
    allResources?: Maybe<Scalars['Boolean']>;
    /** Whether of not to apply the filter to all resources of the type */
    filter?: Maybe<PolicyMatchFilterInput>;
};

/**
 * A privilege associated with a particular resource type
 * A resource is most commonly a DataHub Metadata Entity
 */
export type ResourcePrivileges = {
    __typename?: 'ResourcePrivileges';
    /** Resource type associated with the Access Privilege, eg dataset */
    resourceType: Scalars['String'];
    /** The name to used for displaying the resourceType */
    resourceTypeDisplayName?: Maybe<Scalars['String']>;
    /** An optional entity type to use when performing search and navigation to the entity */
    entityType?: Maybe<EntityType>;
    /** A list of privileges that are supported against this resource */
    privileges: Array<Privilege>;
};

/** Reference to a resource to apply an action to */
export type ResourceRefInput = {
    /** The urn of the resource being referenced */
    resourceUrn: Scalars['String'];
    /** An optional type of a sub resource to attach the Tag to */
    subResourceType?: Maybe<SubResourceType>;
    /** An optional sub resource identifier to attach the Tag to */
    subResource?: Maybe<Scalars['String']>;
};

/** Spec to identify resource */
export type ResourceSpec = {
    /** Resource type */
    resourceType: EntityType;
    /** Resource urn */
    resourceUrn: Scalars['String'];
};

export type ResultsType = StringBox;

/** Input for rolling back an ingestion execution */
export type RollbackIngestionInput = {
    /** An ingestion run ID */
    runId: Scalars['String'];
};

/** For consumption by UI only */
export type Row = {
    __typename?: 'Row';
    values: Array<Scalars['String']>;
    cells?: Maybe<Array<Cell>>;
};

/** Type of the scenario requesting recommendation */
export enum ScenarioType {
    /** Recommendations to show on the users home page */
    Home = 'HOME',
    /** Recommendations to show on the search results page */
    SearchResults = 'SEARCH_RESULTS',
    /** Recommendations to show on an Entity Profile page */
    EntityProfile = 'ENTITY_PROFILE',
    /** Recommendations to show on the search bar when clicked */
    SearchBar = 'SEARCH_BAR',
}

/**
 * Deprecated, use SchemaMetadata instead
 * Metadata about a Dataset schema
 */
export type Schema = {
    __typename?: 'Schema';
    /** Dataset this schema metadata is associated with */
    datasetUrn?: Maybe<Scalars['String']>;
    /** Schema name */
    name: Scalars['String'];
    /** Platform this schema metadata is associated with */
    platformUrn: Scalars['String'];
    /** The version of the GMS Schema metadata */
    version: Scalars['Long'];
    /** The cluster this schema metadata is derived from */
    cluster?: Maybe<Scalars['String']>;
    /** The SHA1 hash of the schema content */
    hash: Scalars['String'];
    /** The native schema in the datasets platform, schemaless if it was not provided */
    platformSchema?: Maybe<PlatformSchema>;
    /** Client provided a list of fields from value schema */
    fields: Array<SchemaField>;
    /** Client provided list of fields that define primary keys to access record */
    primaryKeys?: Maybe<Array<Scalars['String']>>;
    /** Client provided list of foreign key constraints */
    foreignKeys?: Maybe<Array<Maybe<ForeignKeyConstraint>>>;
    /** The time at which the schema metadata information was created */
    createdAt?: Maybe<Scalars['Long']>;
    /** The time at which the schema metadata information was last ingested */
    lastObserved?: Maybe<Scalars['Long']>;
};

/** Information about an individual field in a Dataset schema */
export type SchemaField = {
    __typename?: 'SchemaField';
    /** Flattened name of the field computed from jsonPath field */
    fieldPath: Scalars['String'];
    /** Flattened name of a field in JSON Path notation */
    jsonPath?: Maybe<Scalars['String']>;
    /** Human readable label for the field. Not supplied by all data sources */
    label?: Maybe<Scalars['String']>;
    /** Indicates if this field is optional or nullable */
    nullable: Scalars['Boolean'];
    /** Description of the field */
    description?: Maybe<Scalars['String']>;
    /** Platform independent field type of the field */
    type: SchemaFieldDataType;
    /** The native type of the field in the datasets platform as declared by platform schema */
    nativeDataType?: Maybe<Scalars['String']>;
    /** Whether the field references its own type recursively */
    recursive: Scalars['Boolean'];
    /**
     * Deprecated, use tags field instead
     * Tags associated with the field
     * @deprecated Field no longer supported
     */
    globalTags?: Maybe<GlobalTags>;
    /** Tags associated with the field */
    tags?: Maybe<GlobalTags>;
    /** Glossary terms associated with the field */
    glossaryTerms?: Maybe<GlossaryTerms>;
    /** Whether the field is part of a key schema */
    isPartOfKey?: Maybe<Scalars['Boolean']>;
};

/** Blame for a single field */
export type SchemaFieldBlame = {
    __typename?: 'SchemaFieldBlame';
    /** Flattened name of a schema field */
    fieldPath: Scalars['String'];
    /** Attributes identifying a field change */
    schemaFieldChange: SchemaFieldChange;
};

/** Attributes identifying a field change */
export type SchemaFieldChange = {
    __typename?: 'SchemaFieldChange';
    /** The time at which the schema was updated */
    timestampMillis: Scalars['Long'];
    /** The last semantic version that this schema was changed in */
    lastSemanticVersion: Scalars['String'];
    /** Version stamp of the change */
    versionStamp: Scalars['String'];
    /** The type of the change */
    changeType: ChangeOperationType;
    /** Last column update, such as Added/Modified/Removed in v1.2.3. */
    lastSchemaFieldChange?: Maybe<Scalars['String']>;
};

/** The type associated with a single Dataset schema field */
export enum SchemaFieldDataType {
    /** A boolean type */
    Boolean = 'BOOLEAN',
    /** A fixed bytestring type */
    Fixed = 'FIXED',
    /** A string type */
    String = 'STRING',
    /** A string of bytes */
    Bytes = 'BYTES',
    /** A number, including integers, floats, and doubles */
    Number = 'NUMBER',
    /** A datestrings type */
    Date = 'DATE',
    /** A timestamp type */
    Time = 'TIME',
    /** An enum type */
    Enum = 'ENUM',
    /** A NULL type */
    Null = 'NULL',
    /** A map collection type */
    Map = 'MAP',
    /** An array collection type */
    Array = 'ARRAY',
    /** An union type */
    Union = 'UNION',
    /** An complex struct type */
    Struct = 'STRUCT',
}

/**
 * Standalone schema field entity. Differs from the SchemaField struct because it is not directly nested inside a
 * schema field
 */
export type SchemaFieldEntity = Entity & {
    __typename?: 'SchemaFieldEntity';
    /** Primary key of the schema field */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /** Field path identifying the field in its dataset */
    fieldPath: Scalars['String'];
    /** The field's parent. */
    parent: Entity;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/**
 * Standalone schema field entity. Differs from the SchemaField struct because it is not directly nested inside a
 * schema field
 */
export type SchemaFieldEntityRelationshipsArgs = {
    input: RelationshipsInput;
};

/** A Dataset schema field (i.e. column) */
export type SchemaFieldRef = {
    __typename?: 'SchemaFieldRef';
    /** A schema field urn */
    urn: Scalars['String'];
    /** A schema field path */
    path: Scalars['String'];
};

/** Metadata about a Dataset schema */
export type SchemaMetadata = Aspect & {
    __typename?: 'SchemaMetadata';
    /**
     * The logical version of the schema metadata, where zero represents the latest version
     * with otherwise monotonic ordering starting at one
     */
    aspectVersion?: Maybe<Scalars['Long']>;
    /** Dataset this schema metadata is associated with */
    datasetUrn?: Maybe<Scalars['String']>;
    /** Schema name */
    name: Scalars['String'];
    /** Platform this schema metadata is associated with */
    platformUrn: Scalars['String'];
    /** The version of the GMS Schema metadata */
    version: Scalars['Long'];
    /** The cluster this schema metadata is derived from */
    cluster?: Maybe<Scalars['String']>;
    /** The SHA1 hash of the schema content */
    hash: Scalars['String'];
    /** The native schema in the datasets platform, schemaless if it was not provided */
    platformSchema?: Maybe<PlatformSchema>;
    /** Client provided a list of fields from value schema */
    fields: Array<SchemaField>;
    /** Client provided list of fields that define primary keys to access record */
    primaryKeys?: Maybe<Array<Scalars['String']>>;
    /** Client provided list of foreign key constraints */
    foreignKeys?: Maybe<Array<Maybe<ForeignKeyConstraint>>>;
    /** The time at which the schema metadata information was created */
    createdAt?: Maybe<Scalars['Long']>;
};

/** Input arguments for a full text search query across entities, specifying a starting pointer. Allows paging beyond 10k results */
export type ScrollAcrossEntitiesInput = {
    /** Entity types to be searched. If this is not provided, all entities will be searched. */
    types?: Maybe<Array<EntityType>>;
    /** The query string */
    query: Scalars['String'];
    /** The starting point of paginated results, an opaque ID the backend understands as a pointer */
    scrollId?: Maybe<Scalars['String']>;
    /** The amount of time to keep the point in time snapshot alive, takes a time unit based string ex: 5m or 30s */
    keepAlive?: Maybe<Scalars['String']>;
    /** The number of elements included in the results */
    count?: Maybe<Scalars['Int']>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
    /** Optional - A View to apply when generating results */
    viewUrn?: Maybe<Scalars['String']>;
    /** Flags controlling search options */
    searchFlags?: Maybe<SearchFlags>;
};

/** Input arguments for a search query over the results of a multi-hop graph query, uses scroll API */
export type ScrollAcrossLineageInput = {
    /** Urn of the source node */
    urn?: Maybe<Scalars['String']>;
    /** The direction of the relationship, either incoming or outgoing from the source entity */
    direction: LineageDirection;
    /** Entity types to be searched. If this is not provided, all entities will be searched. */
    types?: Maybe<Array<EntityType>>;
    /** The query string */
    query?: Maybe<Scalars['String']>;
    /** The starting point of paginated results, an opaque ID the backend understands as a pointer */
    scrollId?: Maybe<Scalars['String']>;
    /** The amount of time to keep the point in time snapshot alive, takes a time unit based string ex: 5m or 30s */
    keepAlive?: Maybe<Scalars['String']>;
    /** The number of elements included in the results */
    count?: Maybe<Scalars['Int']>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
    /** An optional starting time to filter on */
    startTimeMillis?: Maybe<Scalars['Long']>;
    /** An optional ending time to filter on */
    endTimeMillis?: Maybe<Scalars['Long']>;
    /** Flags controlling search options */
    searchFlags?: Maybe<SearchFlags>;
};

/** Results returned by issueing a search across relationships query using scroll API */
export type ScrollAcrossLineageResults = {
    __typename?: 'ScrollAcrossLineageResults';
    /** Opaque ID to pass to the next request to the server */
    nextScrollId?: Maybe<Scalars['String']>;
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities */
    searchResults: Array<SearchAcrossLineageResult>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
};

/** Results returned by issuing a search query */
export type ScrollResults = {
    __typename?: 'ScrollResults';
    /** Opaque ID to pass to the next request to the server */
    nextScrollId?: Maybe<Scalars['String']>;
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities for a scroll request */
    searchResults: Array<SearchResult>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
};

/** Input arguments for a full text search query across entities */
export type SearchAcrossEntitiesInput = {
    /** Entity types to be searched. If this is not provided, all entities will be searched. */
    types?: Maybe<Array<EntityType>>;
    /** The query string */
    query: Scalars['String'];
    /** The starting point of paginated results */
    start?: Maybe<Scalars['Int']>;
    /** The number of elements included in the results */
    count?: Maybe<Scalars['Int']>;
    /**
     * Deprecated in favor of the more expressive orFilters field
     * Facet filters to apply to search results. These will be 'AND'-ed together.
     */
    filters?: Maybe<Array<FacetFilterInput>>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
    /** Optional - A View to apply when generating results */
    viewUrn?: Maybe<Scalars['String']>;
    /** Flags controlling search options */
    searchFlags?: Maybe<SearchFlags>;
};

/** Input arguments for a search query over the results of a multi-hop graph query */
export type SearchAcrossLineageInput = {
    /** Urn of the source node */
    urn?: Maybe<Scalars['String']>;
    /** The direction of the relationship, either incoming or outgoing from the source entity */
    direction: LineageDirection;
    /** Entity types to be searched. If this is not provided, all entities will be searched. */
    types?: Maybe<Array<EntityType>>;
    /** The query string */
    query?: Maybe<Scalars['String']>;
    /** The starting point of paginated results */
    start?: Maybe<Scalars['Int']>;
    /** The number of elements included in the results */
    count?: Maybe<Scalars['Int']>;
    /**
     * Deprecated in favor of the more expressive orFilters field
     * Facet filters to apply to search results. These will be 'AND'-ed together.
     */
    filters?: Maybe<Array<FacetFilterInput>>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
    /** An optional starting time to filter on */
    startTimeMillis?: Maybe<Scalars['Long']>;
    /** An optional ending time to filter on */
    endTimeMillis?: Maybe<Scalars['Long']>;
    /** Flags controlling search options */
    searchFlags?: Maybe<SearchFlags>;
};

/** Individual search result from a search across relationships query (has added metadata about the path) */
export type SearchAcrossLineageResult = {
    __typename?: 'SearchAcrossLineageResult';
    /** The resolved DataHub Metadata Entity matching the search query */
    entity: Entity;
    /** Insights about why the search result was matched */
    insights?: Maybe<Array<SearchInsight>>;
    /** Matched field hint */
    matchedFields: Array<MatchedField>;
    /** Optional list of entities between the source and destination node */
    paths?: Maybe<Array<Maybe<EntityPath>>>;
    /** Degree of relationship (number of hops to get to entity) */
    degree: Scalars['Int'];
};

/** Results returned by issueing a search across relationships query */
export type SearchAcrossLineageResults = {
    __typename?: 'SearchAcrossLineageResults';
    /** The offset of the result set */
    start: Scalars['Int'];
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities */
    searchResults: Array<SearchAcrossLineageResult>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
    /** Optional freshness characteristics of this query (cached, staleness etc.) */
    freshness?: Maybe<FreshnessStats>;
};

/** Set of flags to control search behavior */
export type SearchFlags = {
    /** Whether to skip cache */
    skipCache?: Maybe<Scalars['Boolean']>;
    /** The maximum number of values in an facet aggregation */
    maxAggValues?: Maybe<Scalars['Int']>;
    /** Structured or unstructured fulltext query */
    fulltext?: Maybe<Scalars['Boolean']>;
    /** Whether to skip highlighting */
    skipHighlighting?: Maybe<Scalars['Boolean']>;
    /** Whether to skip aggregates/facets */
    skipAggregates?: Maybe<Scalars['Boolean']>;
};

/** Input arguments for a full text search query */
export type SearchInput = {
    /** The Metadata Entity type to be searched against */
    type: EntityType;
    /** The raw query string */
    query: Scalars['String'];
    /** The offset of the result set */
    start?: Maybe<Scalars['Int']>;
    /** The number of entities to include in result set */
    count?: Maybe<Scalars['Int']>;
    /**
     * Deprecated in favor of the more expressive orFilters field
     * Facet filters to apply to search results. These will be 'AND'-ed together.
     */
    filters?: Maybe<Array<FacetFilterInput>>;
    /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
    orFilters?: Maybe<Array<AndFilterInput>>;
    /** Flags controlling search options */
    searchFlags?: Maybe<SearchFlags>;
};

/** Insights about why a search result was returned or ranked in the way that it was */
export type SearchInsight = {
    __typename?: 'SearchInsight';
    /** The insight to display */
    text: Scalars['String'];
    /** An optional emoji to display in front of the text */
    icon?: Maybe<Scalars['String']>;
};

/** Context to define the search recommendations */
export type SearchParams = {
    __typename?: 'SearchParams';
    /** Entity types to be searched. If this is not provided, all entities will be searched. */
    types?: Maybe<Array<EntityType>>;
    /** Search query */
    query: Scalars['String'];
    /** Filters */
    filters?: Maybe<Array<FacetFilter>>;
};

/** Context that defines a search page requesting recommendatinos */
export type SearchRequestContext = {
    /** Search query */
    query: Scalars['String'];
    /** Faceted filters applied to search results */
    filters?: Maybe<Array<FacetFilterInput>>;
};

/** An individual search result hit */
export type SearchResult = {
    __typename?: 'SearchResult';
    /** The resolved DataHub Metadata Entity matching the search query */
    entity: Entity;
    /** Insights about why the search result was matched */
    insights?: Maybe<Array<SearchInsight>>;
    /** Matched field hint */
    matchedFields: Array<MatchedField>;
};

/** Results returned by issuing a search query */
export type SearchResults = {
    __typename?: 'SearchResults';
    /** The offset of the result set */
    start: Scalars['Int'];
    /** The number of entities included in the result set */
    count: Scalars['Int'];
    /** The total number of search results matching the query and filters */
    total: Scalars['Int'];
    /** The search result entities */
    searchResults: Array<SearchResult>;
    /** Candidate facet aggregations used for search filtering */
    facets?: Maybe<Array<FacetMetadata>>;
};

/** A referencible secret stored in DataHub's system. Notice that we do not return the actual secret value. */
export type Secret = {
    __typename?: 'Secret';
    /** The urn of the secret */
    urn: Scalars['String'];
    /** The name of the secret */
    name: Scalars['String'];
    /** An optional description for the secret */
    description?: Maybe<Scalars['String']>;
};

/** A plaintext secret value */
export type SecretValue = {
    __typename?: 'SecretValue';
    /** The name of the secret */
    name: Scalars['String'];
    /** The plaintext value of the secret. */
    value: Scalars['String'];
};

/** Properties identify a semantic version */
export type SemanticVersionStruct = {
    __typename?: 'SemanticVersionStruct';
    /** Semantic version of the change */
    semanticVersion?: Maybe<Scalars['String']>;
    /** Semantic version timestamp */
    semanticVersionTimestamp?: Maybe<Scalars['Long']>;
    /** Version stamp of the change */
    versionStamp?: Maybe<Scalars['String']>;
};

/** Metadata about the entity's siblings */
export type SiblingProperties = {
    __typename?: 'SiblingProperties';
    /** If this entity is the primary sibling among the sibling set */
    isPrimary?: Maybe<Scalars['Boolean']>;
    /** The sibling entities */
    siblings?: Maybe<Array<Maybe<Entity>>>;
};

export type SourceCode = {
    __typename?: 'SourceCode';
    /** Source Code along with types */
    sourceCode?: Maybe<Array<SourceCodeUrl>>;
};

export type SourceCodeUrl = {
    __typename?: 'SourceCodeUrl';
    /** Source Code Url Types */
    type: SourceCodeUrlType;
    /** Source Code Url */
    sourceCodeUrl: Scalars['String'];
};

export enum SourceCodeUrlType {
    /** MLModel Source Code */
    MlModelSourceCode = 'ML_MODEL_SOURCE_CODE',
    /** Training Pipeline Source Code */
    TrainingPipelineSourceCode = 'TRAINING_PIPELINE_SOURCE_CODE',
    /** Evaluation Pipeline Source Code */
    EvaluationPipelineSourceCode = 'EVALUATION_PIPELINE_SOURCE_CODE',
}

/** The status of a particular Metadata Entity */
export type Status = {
    __typename?: 'Status';
    /** Whether the entity is removed or not */
    removed: Scalars['Boolean'];
};

/** The input required to update the state of a step */
export type StepStateInput = {
    /** The globally unique id for the step */
    id: Scalars['String'];
    /** The new properties for the step */
    properties: Array<Maybe<StringMapEntryInput>>;
};

/** A single step state */
export type StepStateResult = {
    __typename?: 'StepStateResult';
    /** Unique id of the step */
    id: Scalars['String'];
    /** The properties for the step state */
    properties: Array<StringMapEntry>;
};

export type StringBox = {
    __typename?: 'StringBox';
    stringValue: Scalars['String'];
};

/** An entry in a string string map represented as a tuple */
export type StringMapEntry = {
    __typename?: 'StringMapEntry';
    /** The key of the map entry */
    key: Scalars['String'];
    /** The value fo the map entry */
    value?: Maybe<Scalars['String']>;
};

/** String map entry input */
export type StringMapEntryInput = {
    /** The key of the map entry */
    key: Scalars['String'];
    /** The value fo the map entry */
    value?: Maybe<Scalars['String']>;
};

/** A flexible carrier for structured results of an execution request. */
export type StructuredReport = {
    __typename?: 'StructuredReport';
    /** The type of the structured report. (e.g. INGESTION_REPORT, TEST_CONNECTION_REPORT, etc.) */
    type: Scalars['String'];
    /** The serialized value of the structured report */
    serializedValue: Scalars['String'];
    /** The content-type of the serialized value (e.g. application/json, application/json;gzip etc.) */
    contentType: Scalars['String'];
};

/** A type of Metadata Entity sub resource */
export enum SubResourceType {
    /** A Dataset field or column */
    DatasetField = 'DATASET_FIELD',
}

export type SubTypes = {
    __typename?: 'SubTypes';
    /** The sub-types that this entity implements. e.g. Datasets that are views will implement the "view" subtype */
    typeNames?: Maybe<Array<Scalars['String']>>;
};

export type SystemFreshness = {
    __typename?: 'SystemFreshness';
    /** Name of the system */
    systemName: Scalars['String'];
    /**
     * The latest timestamp in millis of the system that was used to respond to this query
     * In case a cache was consulted, this reflects the freshness of the cache
     * In case an index was consulted, this reflects the freshness of the index
     */
    freshnessMillis: Scalars['Long'];
};

/** For consumption by UI only */
export type TableChart = {
    __typename?: 'TableChart';
    title: Scalars['String'];
    columns: Array<Scalars['String']>;
    rows: Array<Row>;
};

/** Information about a raw Table Schema */
export type TableSchema = {
    __typename?: 'TableSchema';
    /** Raw table schema */
    schema: Scalars['String'];
};

/** A Tag Entity, which can be associated with other Metadata Entities and subresources */
export type Tag = Entity & {
    __typename?: 'Tag';
    /** The primary key of the TAG */
    urn: Scalars['String'];
    /** A standard Entity Type */
    type: EntityType;
    /**
     * A unique identifier for the Tag. Deprecated - Use properties.name field instead.
     * @deprecated Field no longer supported
     */
    name: Scalars['String'];
    /** Additional properties about the Tag */
    properties?: Maybe<TagProperties>;
    /**
     * Additional read write properties about the Tag
     * Deprecated! Use 'properties' field instead.
     * @deprecated Field no longer supported
     */
    editableProperties?: Maybe<EditableTagProperties>;
    /** Ownership metadata of the dataset */
    ownership?: Maybe<Ownership>;
    /** Granular API for querying edges extending from this entity */
    relationships?: Maybe<EntityRelationshipsResult>;
    /**
     * Deprecated, use properties.description field instead
     * @deprecated Field no longer supported
     */
    description?: Maybe<Scalars['String']>;
};

/** A Tag Entity, which can be associated with other Metadata Entities and subresources */
export type TagRelationshipsArgs = {
    input: RelationshipsInput;
};

/**
 * An edge between a Metadata Entity and a Tag Modeled as a struct to permit
 * additional attributes
 * TODO Consider whether this query should be serviced by the relationships field
 */
export type TagAssociation = {
    __typename?: 'TagAssociation';
    /** The tag itself */
    tag: Tag;
    /** Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together */
    associatedUrn: Scalars['String'];
};

/** Input provided when updating the association between a Metadata Entity and a Tag */
export type TagAssociationInput = {
    /** The primary key of the Tag to add or remove */
    tagUrn: Scalars['String'];
    /** The target Metadata Entity to add or remove the Tag to */
    resourceUrn: Scalars['String'];
    /** An optional type of a sub resource to attach the Tag to */
    subResourceType?: Maybe<SubResourceType>;
    /** An optional sub resource identifier to attach the Tag to */
    subResource?: Maybe<Scalars['String']>;
};

/**
 * Deprecated, use addTag or removeTag mutation instead
 * A tag update to be applied
 */
export type TagAssociationUpdate = {
    /** The tag being applied */
    tag: TagUpdateInput;
};

/** Properties for a DataHub Tag */
export type TagProperties = {
    __typename?: 'TagProperties';
    /** A display name for the Tag */
    name: Scalars['String'];
    /** A description of the Tag */
    description?: Maybe<Scalars['String']>;
    /** An optional RGB hex code for a Tag color, e.g. #FFFFFF */
    colorHex?: Maybe<Scalars['String']>;
};

/**
 * Deprecated, use addTag or removeTag mutations instead
 * An update for a particular Tag entity
 */
export type TagUpdateInput = {
    /** The primary key of the Tag */
    urn: Scalars['String'];
    /** The display name of a Tag */
    name: Scalars['String'];
    /** Description of the tag */
    description?: Maybe<Scalars['String']>;
    /** Ownership metadata of the tag */
    ownership?: Maybe<OwnershipUpdate>;
};

/** Configurations related to tracking users in the app */
export type TelemetryConfig = {
    __typename?: 'TelemetryConfig';
    /** Env variable for whether or not third party logging should be enabled for this instance */
    enableThirdPartyLogging?: Maybe<Scalars['Boolean']>;
};

/** Input provided when updating the association between a Metadata Entity and a Glossary Term */
export type TermAssociationInput = {
    /** The primary key of the Glossary Term to add or remove */
    termUrn: Scalars['String'];
    /** The target Metadata Entity to add or remove the Glossary Term from */
    resourceUrn: Scalars['String'];
    /** An optional type of a sub resource to attach the Glossary Term to */
    subResourceType?: Maybe<SubResourceType>;
    /** An optional sub resource identifier to attach the Glossary Term to */
    subResource?: Maybe<Scalars['String']>;
};

/** A type of Metadata Entity sub resource */
export enum TermRelationshipType {
    /** When a Term inherits from, or has an 'Is A' relationship with another Term */
    IsA = 'isA',
    /** When a Term contains, or has a 'Has A' relationship with another Term */
    HasA = 'hasA',
}

/** A metadata entity representing a DataHub Test */
export type Test = Entity & {
    __typename?: 'Test';
    /** The primary key of the Test itself */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** The name of the Test */
    name: Scalars['String'];
    /** The category of the Test (user defined) */
    category: Scalars['String'];
    /** Description of the test */
    description?: Maybe<Scalars['String']>;
    /** Definition for the test */
    definition: TestDefinition;
    /** Unused for tests */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/** A metadata entity representing a DataHub Test */
export type TestRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Definition of the test */
export type TestDefinition = {
    __typename?: 'TestDefinition';
    /** JSON-based def for the test */
    json?: Maybe<Scalars['String']>;
};

export type TestDefinitionInput = {
    /** The string representation of the Test */
    json?: Maybe<Scalars['String']>;
};

/** The result of running a test */
export type TestResult = {
    __typename?: 'TestResult';
    /** The test itself, or null if the test has been deleted */
    test?: Maybe<Test>;
    /** The final result, e.g. either SUCCESS or FAILURE. */
    type: TestResultType;
};

/** The result type of a test that has been run */
export enum TestResultType {
    /** The test succeeded. */
    Success = 'SUCCESS',
    /** The test failed. */
    Failure = 'FAILURE',
}

/** A set of test results */
export type TestResults = {
    __typename?: 'TestResults';
    /** The tests passing */
    passing: Array<TestResult>;
    /** The tests failing */
    failing: Array<TestResult>;
};

/** Configurations related to DataHub Tests feature */
export type TestsConfig = {
    __typename?: 'TestsConfig';
    /** Whether Tests feature is enabled */
    enabled: Scalars['Boolean'];
};

/** A Notebook cell which contains text as content */
export type TextCell = {
    __typename?: 'TextCell';
    /** Title of the cell */
    cellTitle: Scalars['String'];
    /** Unique id for the cell. */
    cellId: Scalars['String'];
    /** Captures information about who created/last modified/deleted this TextCell and when */
    changeAuditStamps?: Maybe<ChangeAuditStamps>;
    /** The actual text in a TextCell in a Notebook */
    text: Scalars['String'];
};

/** A time range used in fetching Usage statistics */
export enum TimeRange {
    /** Last day */
    Day = 'DAY',
    /** Last week */
    Week = 'WEEK',
    /** Last month */
    Month = 'MONTH',
    /** Last quarter */
    Quarter = 'QUARTER',
    /** Last year */
    Year = 'YEAR',
    /** All time */
    All = 'ALL',
}

/** A time series aspect, or a group of related metadata associated with an Entity and corresponding to a particular timestamp */
export type TimeSeriesAspect = {
    /** The timestamp associated with the time series aspect in milliseconds */
    timestampMillis: Scalars['Long'];
};

/** For consumption by UI only */
export type TimeSeriesChart = {
    __typename?: 'TimeSeriesChart';
    title: Scalars['String'];
    lines: Array<NamedLine>;
    dateRange: DateRange;
    interval: DateInterval;
};

/** A time window with a finite start and end time */
export type TimeWindow = {
    __typename?: 'TimeWindow';
    /** The start time of the time window */
    startTimeMillis: Scalars['Long'];
    /** The end time of the time window */
    durationMillis: Scalars['Long'];
};

/** Input required to update a users settings. */
export type UpdateCorpUserViewsSettingsInput = {
    /**
     * The URN of the View that serves as this user's personal default.
     * If not provided, any existing default view will be removed.
     */
    defaultView?: Maybe<Scalars['String']>;
};

/** Input provided when setting the Deprecation status for an Entity. */
export type UpdateDeprecationInput = {
    /** The urn of the Entity to set deprecation for. */
    urn: Scalars['String'];
    /** Whether the Entity is marked as deprecated. */
    deprecated: Scalars['Boolean'];
    /** Optional - The time user plan to decommission this entity */
    decommissionTime?: Maybe<Scalars['Long']>;
    /** Optional - Additional information about the entity deprecation plan */
    note?: Maybe<Scalars['String']>;
};

/** Input required to set or clear information related to rendering a Data Asset inside of DataHub. */
export type UpdateEmbedInput = {
    /** The URN associated with the Data Asset to update. Only dataset, dashboard, and chart urns are currently supported. */
    urn: Scalars['String'];
    /** Set or clear a URL used to render an embedded asset. */
    renderUrl?: Maybe<Scalars['String']>;
};

/** Input required to update Global View Settings. */
export type UpdateGlobalViewsSettingsInput = {
    /**
     * The URN of the View that serves as the Global, or organization-wide, default.
     * If this field is not provided, the existing Global Default will be cleared.
     */
    defaultView?: Maybe<Scalars['String']>;
};

/** Input parameters for creating / updating an Ingestion Source */
export type UpdateIngestionSourceConfigInput = {
    /** A JSON-encoded recipe */
    recipe: Scalars['String'];
    /** The version of DataHub Ingestion Framework to use when executing the recipe. */
    version?: Maybe<Scalars['String']>;
    /** The id of the executor to use for executing the recipe */
    executorId: Scalars['String'];
    /** Whether or not to run ingestion in debug mode */
    debugMode?: Maybe<Scalars['Boolean']>;
};

/** Input arguments for creating / updating an Ingestion Source */
export type UpdateIngestionSourceInput = {
    /** A name associated with the ingestion source */
    name: Scalars['String'];
    /** The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe. */
    type: Scalars['String'];
    /** An optional description associated with the ingestion source */
    description?: Maybe<Scalars['String']>;
    /** An optional schedule for the ingestion source. If not provided, the source is only available for run on-demand. */
    schedule?: Maybe<UpdateIngestionSourceScheduleInput>;
    /** A set of type-specific ingestion source configurations */
    config: UpdateIngestionSourceConfigInput;
};

/** Input arguments for creating / updating the schedule of an Ingestion Source */
export type UpdateIngestionSourceScheduleInput = {
    /** The cron-formatted interval describing when the job should be executed */
    interval: Scalars['String'];
    /** The name of the timezone in which the cron interval should be scheduled (e.g. America/Los Angeles) */
    timezone: Scalars['String'];
};

/** Input required in order to upsert lineage edges */
export type UpdateLineageInput = {
    /** New lineage edges to upsert */
    edgesToAdd: Array<Maybe<LineageEdge>>;
    /**
     * Lineage edges to remove. Takes precedence over edgesToAdd - so edges existing both edgesToAdd
     * and edgesToRemove will be removed.
     */
    edgesToRemove: Array<Maybe<LineageEdge>>;
};

/** Input provided for filling in a post content */
export type UpdateMediaInput = {
    /** The type of media */
    type: MediaType;
    /** The location of the media (a URL) */
    location: Scalars['String'];
};

/** Input for updating the name of an entity */
export type UpdateNameInput = {
    /** The new name */
    name: Scalars['String'];
    /** The primary key of the resource to update the name for */
    urn: Scalars['String'];
};

/** Input for updating the parent node of a resource. Currently only GlossaryNodes and GlossaryTerms have parentNodes. */
export type UpdateParentNodeInput = {
    /** The new parent node urn. If parentNode is null, this will remove the parent from this entity */
    parentNode?: Maybe<Scalars['String']>;
    /** The primary key of the resource to update the parent node for */
    resourceUrn: Scalars['String'];
};

/** Input provided for filling in a post content */
export type UpdatePostContentInput = {
    /** The type of post content */
    contentType: PostContentType;
    /** The title of the post */
    title: Scalars['String'];
    /** Optional content of the post */
    description?: Maybe<Scalars['String']>;
    /** Optional link that the post is associated with */
    link?: Maybe<Scalars['String']>;
    /** Optional media contained in the post */
    media?: Maybe<UpdateMediaInput>;
};

/** Input required for updating an existing Query. Requires the 'Edit Queries' privilege for all query subjects. */
export type UpdateQueryInput = {
    /** Properties about the Query */
    properties?: Maybe<UpdateQueryPropertiesInput>;
    /** Subjects for the query */
    subjects?: Maybe<Array<UpdateQuerySubjectInput>>;
};

/** Input properties required for creating a Query. Any non-null fields will be updated if provided. */
export type UpdateQueryPropertiesInput = {
    /** An optional display name for the Query */
    name?: Maybe<Scalars['String']>;
    /** An optional description for the Query */
    description?: Maybe<Scalars['String']>;
    /** The Query contents */
    statement?: Maybe<QueryStatementInput>;
};

/** Input required for creating a Query. For now, only datasets are supported. */
export type UpdateQuerySubjectInput = {
    /** The urn of the dataset that is the subject of the query */
    datasetUrn: Scalars['String'];
};

/** Result returned when fetching step state */
export type UpdateStepStateResult = {
    __typename?: 'UpdateStepStateResult';
    /** Id of the step */
    id: Scalars['String'];
    /** Whether the update succeeded. */
    succeeded: Scalars['Boolean'];
};

export type UpdateTestInput = {
    /** The name of the Test */
    name: Scalars['String'];
    /** The category of the Test (user defined) */
    category: Scalars['String'];
    /** Description of the test */
    description?: Maybe<Scalars['String']>;
    /** The test definition */
    definition: TestDefinitionInput;
};

/** Input for updating a user setting */
export type UpdateUserSettingInput = {
    /** The name of the setting */
    name: UserSetting;
    /** The new value of the setting */
    value: Scalars['Boolean'];
};

/** Input provided when updating a DataHub View */
export type UpdateViewInput = {
    /** The name of the View */
    name?: Maybe<Scalars['String']>;
    /** An optional description of the View */
    description?: Maybe<Scalars['String']>;
    /** The view definition itself */
    definition?: Maybe<DataHubViewDefinitionInput>;
};

/** Deprecated, use relationships query instead */
export type UpstreamEntityRelationships = {
    __typename?: 'UpstreamEntityRelationships';
    entities?: Maybe<Array<Maybe<EntityRelationshipLegacy>>>;
};

/** An aggregation of Dataset usage statistics */
export type UsageAggregation = {
    __typename?: 'UsageAggregation';
    /** The time window start time */
    bucket?: Maybe<Scalars['Long']>;
    /** The time window span */
    duration?: Maybe<WindowDuration>;
    /** The resource urn associated with the usage information, eg a Dataset urn */
    resource?: Maybe<Scalars['String']>;
    /** The rolled up usage metrics */
    metrics?: Maybe<UsageAggregationMetrics>;
};

/** Rolled up metrics about Dataset usage over time */
export type UsageAggregationMetrics = {
    __typename?: 'UsageAggregationMetrics';
    /** The unique number of users who have queried the dataset within the time range */
    uniqueUserCount?: Maybe<Scalars['Int']>;
    /** Usage statistics within the time range by user */
    users?: Maybe<Array<Maybe<UserUsageCounts>>>;
    /** The total number of queries issued against the dataset within the time range */
    totalSqlQueries?: Maybe<Scalars['Int']>;
    /** A set of common queries issued against the dataset within the time range */
    topSqlQueries?: Maybe<Array<Maybe<Scalars['String']>>>;
    /** Per field usage statistics within the time range */
    fields?: Maybe<Array<Maybe<FieldUsageCounts>>>;
};

/** The result of a Dataset usage query */
export type UsageQueryResult = {
    __typename?: 'UsageQueryResult';
    /** A set of relevant time windows for use in displaying usage statistics */
    buckets?: Maybe<Array<Maybe<UsageAggregation>>>;
    /** A set of rolled up aggregations about the Dataset usage */
    aggregations?: Maybe<UsageQueryResultAggregations>;
};

/** A set of rolled up aggregations about the Dataset usage */
export type UsageQueryResultAggregations = {
    __typename?: 'UsageQueryResultAggregations';
    /** The count of unique Dataset users within the queried time range */
    uniqueUserCount?: Maybe<Scalars['Int']>;
    /** The specific per user usage counts within the queried time range */
    users?: Maybe<Array<Maybe<UserUsageCounts>>>;
    /** The specific per field usage counts within the queried time range */
    fields?: Maybe<Array<Maybe<FieldUsageCounts>>>;
    /**
     * The total number of queries executed within the queried time range
     * Note that this field will likely be deprecated in favor of a totalQueries field
     */
    totalSqlQueries?: Maybe<Scalars['Int']>;
};

/** An individual setting type for a Corp User. */
export enum UserSetting {
    /** Show simplified homepage */
    ShowSimplifiedHomepage = 'SHOW_SIMPLIFIED_HOMEPAGE',
}

/** Information about individual user usage of a Dataset */
export type UserUsageCounts = {
    __typename?: 'UserUsageCounts';
    /** The user of the Dataset */
    user?: Maybe<CorpUser>;
    /** The number of queries issued by the user */
    count?: Maybe<Scalars['Int']>;
    /**
     * The extracted user email
     * Note that this field will soon be deprecated and merged with user
     */
    userEmail?: Maybe<Scalars['String']>;
};

/** The technical version associated with a given Metadata Entity */
export type VersionTag = {
    __typename?: 'VersionTag';
    versionTag?: Maybe<Scalars['String']>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type VersionedDataset = Entity & {
    __typename?: 'VersionedDataset';
    /** The primary key of the Dataset */
    urn: Scalars['String'];
    /** The standard Entity Type */
    type: EntityType;
    /** Standardized platform urn where the dataset is defined */
    platform: DataPlatform;
    /** The parent container in which the entity resides */
    container?: Maybe<Container>;
    /** Recursively get the lineage of containers for this entity */
    parentContainers?: Maybe<ParentContainersResult>;
    /**
     * Unique guid for dataset
     * No longer to be used as the Dataset display name. Use properties.name instead
     */
    name: Scalars['String'];
    /** An additional set of read only properties */
    properties?: Maybe<DatasetProperties>;
    /** An additional set of of read write properties */
    editableProperties?: Maybe<DatasetEditableProperties>;
    /** Ownership metadata of the dataset */
    ownership?: Maybe<Ownership>;
    /** The deprecation status of the dataset */
    deprecation?: Maybe<Deprecation>;
    /** References to internal resources related to the dataset */
    institutionalMemory?: Maybe<InstitutionalMemory>;
    /** Editable schema metadata of the dataset */
    editableSchemaMetadata?: Maybe<EditableSchemaMetadata>;
    /** Status of the Dataset */
    status?: Maybe<Status>;
    /** Tags used for searching dataset */
    tags?: Maybe<GlobalTags>;
    /** The structured glossary terms associated with the dataset */
    glossaryTerms?: Maybe<GlossaryTerms>;
    /** The Domain associated with the Dataset */
    domain?: Maybe<DomainAssociation>;
    /** Experimental! The resolved health status of the Dataset */
    health?: Maybe<Array<Health>>;
    /** Schema metadata of the dataset */
    schema?: Maybe<Schema>;
    /** Sub Types that this entity implements */
    subTypes?: Maybe<SubTypes>;
    /** View related properties. Only relevant if subtypes field contains view. */
    viewProperties?: Maybe<ViewProperties>;
    /**
     * Deprecated, see the properties field instead
     * Environment in which the dataset belongs to or where it was generated
     * Note that this field will soon be deprecated in favor of a more standardized concept of Environment
     * @deprecated Field no longer supported
     */
    origin: FabricType;
    /**
     * No-op, has to be included due to model
     * @deprecated Field no longer supported
     */
    relationships?: Maybe<EntityRelationshipsResult>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type VersionedDatasetRelationshipsArgs = {
    input: RelationshipsInput;
};

/** Properties about a Dataset of type view */
export type ViewProperties = {
    __typename?: 'ViewProperties';
    /** Whether the view is materialized or not */
    materialized: Scalars['Boolean'];
    /** The logic associated with the view, most commonly a SQL statement */
    logic: Scalars['String'];
    /** The language in which the view logic is written, for example SQL */
    language: Scalars['String'];
};

/** Configurations related to DataHub Views feature */
export type ViewsConfig = {
    __typename?: 'ViewsConfig';
    /** Whether Views feature is enabled */
    enabled: Scalars['Boolean'];
};

/** Configurations related to visual appearance of the app */
export type VisualConfig = {
    __typename?: 'VisualConfig';
    /** Custom logo url for the homepage & top banner */
    logoUrl?: Maybe<Scalars['String']>;
    /** Custom favicon url for the homepage & top banner */
    faviconUrl?: Maybe<Scalars['String']>;
    /** Configuration for the queries tab */
    queriesTab?: Maybe<QueriesTabConfig>;
};

/** The duration of a fixed window of time */
export enum WindowDuration {
    /** A one day window */
    Day = 'DAY',
    /** A one week window */
    Week = 'WEEK',
    /** A one month window */
    Month = 'MONTH',
    /** A one year window */
    Year = 'YEAR',
}
