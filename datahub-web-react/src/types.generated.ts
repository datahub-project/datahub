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

export type Access = {
  __typename?: 'Access';
  roles?: Maybe<Array<RoleAssociation>>;
};

/** The access level for a Metadata Entity, either public or private */
export enum AccessLevel {
  /** Restricted to a subset of viewers */
  Private = 'PRIVATE',
  /** Publicly available */
  Public = 'PUBLIC'
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
  /** No expiry */
  NoExpiry = 'NO_EXPIRY',
  /** 1 day */
  OneDay = 'ONE_DAY',
  /** 1 hour */
  OneHour = 'ONE_HOUR',
  /** 1 month */
  OneMonth = 'ONE_MONTH',
  /** 1 week */
  OneWeek = 'ONE_WEEK',
  /** 1 year */
  OneYear = 'ONE_YEAR',
  /** 6 months */
  SixMonths = 'SIX_MONTHS',
  /** 3 months */
  ThreeMonths = 'THREE_MONTHS'
}

export type AccessTokenMetadata = Entity & {
  __typename?: 'AccessTokenMetadata';
  /** The actor associated with the Access Token. */
  actorUrn: Scalars['String'];
  /** The time when token was generated at. */
  createdAt: Scalars['Long'];
  /** The description of the token if defined. */
  description?: Maybe<Scalars['String']>;
  /** Time when token will be expired. */
  expiresAt?: Maybe<Scalars['Long']>;
  /** The unique identifier of the token. */
  id: Scalars['String'];
  /** The name of the token, if it exists. */
  name: Scalars['String'];
  /** The owner (CorpUser) who created the Access Token. */
  owner?: Maybe<CorpUser>;
  /** The actor who created the Access Token. */
  ownerUrn: Scalars['String'];
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the access token */
  urn: Scalars['String'];
};


export type AccessTokenMetadataRelationshipsArgs = {
  input: RelationshipsInput;
};

/** A type of DataHub Access Token. */
export enum AccessTokenType {
  /** Generates a personal access token */
  Personal = 'PERSONAL',
  /** Generates a service account access token */
  ServiceAccount = 'SERVICE_ACCOUNT'
}

export type ActiveIncidentHealthDetails = {
  __typename?: 'ActiveIncidentHealthDetails';
  /** The number of active incidents */
  count: Scalars['Int'];
  /** The timestamp when the last incident was updated */
  lastActivityAt?: Maybe<Scalars['Long']>;
  /** The title of the latest incident */
  latestIncidentTitle?: Maybe<Scalars['String']>;
  /** The latest incident */
  latestIncidentUrn?: Maybe<Scalars['String']>;
};

export type Actor = {
  __typename?: 'Actor';
  /** List of groups for which the role is provisioned */
  groups?: Maybe<Array<RoleGroup>>;
  /** List of users for which the role is provisioned */
  users?: Maybe<Array<RoleUser>>;
};

/** The actors that a DataHub Access Policy applies to */
export type ActorFilter = {
  __typename?: 'ActorFilter';
  /** Whether the filter should apply to all groups */
  allGroups: Scalars['Boolean'];
  /** Whether the filter should apply to all users */
  allUsers: Scalars['Boolean'];
  /** A set of groups explicitly excluded from this policy, even if matched by groups/allGroups */
  excludedGroups?: Maybe<Array<Scalars['String']>>;
  /**
   * Ownership types explicitly excluded from this policy. Owners whose type is in this list will not match,
   * even if they match via resourceOwnersTypes. Takes precedence over resourceOwnersTypes.
   */
  excludedResourceOwnersTypes?: Maybe<Array<Scalars['String']>>;
  /** A set of users explicitly excluded from this policy, even if matched by users/allUsers */
  excludedUsers?: Maybe<Array<Scalars['String']>>;
  /** A disjunctive set of groups to apply the policy to */
  groups?: Maybe<Array<Scalars['String']>>;
  /** The list of excluded groups on the Policy, resolved. */
  resolvedExcludedGroups?: Maybe<Array<CorpGroup>>;
  /** The list of excluded users on the Policy, resolved. */
  resolvedExcludedUsers?: Maybe<Array<CorpUser>>;
  /** The list of groups on the Policy, resolved. */
  resolvedGroups?: Maybe<Array<CorpGroup>>;
  /** Set of OwnershipTypes to apply the policy to (if resourceOwners field is set to True), resolved. */
  resolvedOwnershipTypes?: Maybe<Array<OwnershipTypeEntity>>;
  /** The list of roles on the Policy, resolved. */
  resolvedRoles?: Maybe<Array<DataHubRole>>;
  /** The list of users on the Policy, resolved. */
  resolvedUsers?: Maybe<Array<CorpUser>>;
  /**
   * Whether the filter should return TRUE for owners of a particular resource
   * Only applies to policies of type METADATA, which have a resource associated with them
   */
  resourceOwners: Scalars['Boolean'];
  /** Set of OwnershipTypes to apply the policy to (if resourceOwners field is set to True) */
  resourceOwnersTypes?: Maybe<Array<Scalars['String']>>;
  /** A disjunctive set of roles to apply the policy to */
  roles?: Maybe<Array<Scalars['String']>>;
  /** A disjunctive set of users to apply the policy to */
  users?: Maybe<Array<Scalars['String']>>;
};

/** Input required when creating or updating an Access Policies Determines which actors the Policy applies to */
export type ActorFilterInput = {
  /** Whether the filter should apply to all groups */
  allGroups: Scalars['Boolean'];
  /** Whether the filter should apply to all users */
  allUsers: Scalars['Boolean'];
  /** A set of groups explicitly excluded from this policy, even if matched by groups/allGroups */
  excludedGroups?: Maybe<Array<Scalars['String']>>;
  /** Ownership types explicitly excluded from this policy. Takes precedence over resourceOwnersTypes. */
  excludedResourceOwnersTypes?: Maybe<Array<Scalars['String']>>;
  /** A set of users explicitly excluded from this policy, even if matched by users/allUsers */
  excludedUsers?: Maybe<Array<Scalars['String']>>;
  /** A disjunctive set of groups to apply the policy to */
  groups?: Maybe<Array<Scalars['String']>>;
  /**
   * Whether the filter should return TRUE for owners of a particular resource
   * Only applies to policies of type METADATA, which have a resource associated with them
   */
  resourceOwners: Scalars['Boolean'];
  /** Set of OwnershipTypes to apply the policy to (if resourceOwners field is set to True) */
  resourceOwnersTypes?: Maybe<Array<Scalars['String']>>;
  /** A disjunctive set of users to apply the policy to */
  users?: Maybe<Array<Scalars['String']>>;
};

/**
 * Input required to attach Business Attribute
 * If businessAttributeUrn is null, then it will remove the business attribute from the resource
 */
export type AddBusinessAttributeInput = {
  /** The urn of the business attribute to add */
  businessAttributeUrn: Scalars['String'];
  /** resource urns to add the business attribute to */
  resourceUrn: Array<ResourceRefInput>;
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
  /** A label to attach to the link */
  label: Scalars['String'];
  /** The url of the link to add or remove */
  linkUrl: Scalars['String'];
  /** The urn of the resource or entity to attach the link to, for example a dataset urn */
  resourceUrn: Scalars['String'];
  /** Optional settings input for this link */
  settings?: Maybe<LinkSettingsInput>;
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
  /** The owner type, either a user or group */
  ownerEntityType: OwnerEntityType;
  /** The primary key of the Owner to add or remove */
  ownerUrn: Scalars['String'];
  /** The urn of the ownership type entity. */
  ownershipTypeUrn?: Maybe<Scalars['String']>;
  /** The urn of the resource or entity to attach or remove the owner from, for example a dataset urn */
  resourceUrn: Scalars['String'];
  /**
   * The ownership type for the new owner. If none is provided, then a new NONE will be added.
   * Deprecated - Use ownershipTypeUrn field instead.
   */
  type?: Maybe<OwnershipType>;
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
  /** The target Metadata Entity to add or remove the Tag to */
  resourceUrn: Scalars['String'];
  /** An optional sub resource identifier to attach the Tag to */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to attach the Tag to */
  subResourceType?: Maybe<SubResourceType>;
  /** The primary key of the Tags */
  tagUrns: Array<Scalars['String']>;
};

/** Input provided when adding Terms to an asset */
export type AddTermsInput = {
  /** The target Metadata Entity to add or remove the Glossary Term from */
  resourceUrn: Scalars['String'];
  /** An optional sub resource identifier to attach the Glossary Term to */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to attach the Glossary Term to */
  subResourceType?: Maybe<SubResourceType>;
  /** The primary key of the Glossary Term to add or remove */
  termUrns: Array<Scalars['String']>;
};

/** Input arguments for a full text search query across entities to get aggregations */
export type AggregateAcrossEntitiesInput = {
  /**
   * The list of facets to get aggregations for. If list is empty or null, get aggregations for all facets
   * Sub-aggregations can be specified with the unicode character ␞ (U+241E) as a delimiter between the subtypes.
   * e.g. _entityType␞owners
   */
  facets?: Maybe<Array<Maybe<Scalars['String']>>>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The query string */
  query: Scalars['String'];
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** Entity types to be searched. If this is not provided, all entities will be searched. */
  types?: Maybe<Array<EntityType>>;
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

/** Results returned from aggregateAcrossEntities */
export type AggregateResults = {
  __typename?: 'AggregateResults';
  /** Candidate facet aggregations used for search filtering */
  facets?: Maybe<Array<FacetMetadata>>;
};

/** Information about the aggregation that can be used for filtering, included the field value and number of results */
export type AggregationMetadata = {
  __typename?: 'AggregationMetadata';
  /** The number of search results containing the value */
  count: Scalars['Long'];
  /** Optional display name to show in the UI for this filter value */
  displayName?: Maybe<Scalars['String']>;
  /** Entity corresponding to the facet field */
  entity?: Maybe<Entity>;
  /** A particular value of a facet field */
  value: Scalars['String'];
};

/** An entry for an allowed value for a structured property */
export type AllowedValue = {
  __typename?: 'AllowedValue';
  /** The description of this allowed value */
  description?: Maybe<Scalars['String']>;
  /** The allowed value */
  value: PropertyValue;
};

/** An input entry for an allowed value for a structured property */
export type AllowedValueInput = {
  /** The description of this allowed value */
  description?: Maybe<Scalars['String']>;
  /**
   * The allowed number value if the value is of type number.
   * Either this or stringValue is required.
   */
  numberValue?: Maybe<Scalars['Float']>;
  /**
   * The allowed string value if the value is of type string
   * Either this or numberValue is required.
   */
  stringValue?: Maybe<Scalars['String']>;
};

/** For consumption by UI only */
export type AnalyticsChart = BarChart | TableChart | TimeSeriesChart;

/** For consumption by UI only */
export type AnalyticsChartGroup = {
  __typename?: 'AnalyticsChartGroup';
  charts: Array<AnalyticsChart>;
  groupId: Scalars['String'];
  title: Scalars['String'];
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
  /** Configurations related to the Analytics Feature */
  analyticsConfig: AnalyticsConfig;
  /** App version */
  appVersion?: Maybe<Scalars['String']>;
  /** Auth-related configurations */
  authConfig: AuthConfig;
  /** Configuration related to the DataHub Chrome Extension */
  chromeExtensionConfig: ChromeExtensionConfig;
  /** Feature flags telling the UI whether a feature is enabled or not */
  featureFlags: FeatureFlagsConfig;
  /** Configuration related to the home page */
  homePageConfig: HomePageConfig;
  /** Configurations related to the User & Group management */
  identityManagementConfig: IdentityManagementConfig;
  /** Configurations related to Lineage */
  lineageConfig: LineageConfig;
  /** Configurations related to UI-based ingestion */
  managedIngestionConfig: ManagedIngestionConfig;
  /** Configurations related to the Policies Feature */
  policiesConfig: PoliciesConfig;
  /** Configurations related to the Search bar */
  searchBarConfig: SearchBarConfig;
  /** Configurations related to the Search card */
  searchCardConfig: SearchCardConfig;
  /** Configurations related the Search Flags */
  searchFlagsConfig: SearchFlagsConfig;
  /** Semantic search and embedding configuration */
  semanticSearchConfig?: Maybe<SemanticSearchConfig>;
  /** Configurations related to tracking users in the app */
  telemetryConfig: TelemetryConfig;
  /** Configurations related to DataHub tests */
  testsConfig: TestsConfig;
  /** Configurations related to DataHub Views */
  viewsConfig: ViewsConfig;
  /** Configurations related to visual appearance, allows styling the UI without rebuilding the bundle */
  visualConfig: VisualConfig;
};

/**
 * An Application, or a grouping of Entities for a single business purpose. Compared with Data Products, Applications represent a grouping of tables that exist to serve a specific
 * purpose. However, unlike Data Products, they don't represent groups that are tailored to be consumed for any particular purpose. Often, the assets in Applications power specific
 * outcomes, for example a Pricing Application.
 */
export type Application = Entity & {
  __typename?: 'Application';
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The Domain associated with the Application */
  domain?: Maybe<DomainAssociation>;
  /** The forms associated with the Application */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the Application */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the Application */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the Application */
  ownership?: Maybe<Ownership>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Properties about an Application */
  properties?: Maybe<ApplicationProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags used for searching Application */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Application */
  urn: Scalars['String'];
};


/**
 * An Application, or a grouping of Entities for a single business purpose. Compared with Data Products, Applications represent a grouping of tables that exist to serve a specific
 * purpose. However, unlike Data Products, they don't represent groups that are tailored to be consumed for any particular purpose. Often, the assets in Applications power specific
 * outcomes, for example a Pricing Application.
 */
export type ApplicationAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/**
 * An Application, or a grouping of Entities for a single business purpose. Compared with Data Products, Applications represent a grouping of tables that exist to serve a specific
 * purpose. However, unlike Data Products, they don't represent groups that are tailored to be consumed for any particular purpose. Often, the assets in Applications power specific
 * outcomes, for example a Pricing Application.
 */
export type ApplicationRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/**
 * An Application, or a grouping of Entities for a single business purpose. Compared with Data Products, Applications represent a grouping of tables that exist to serve a specific
 * purpose. However, unlike Data Products, they don't represent groups that are tailored to be consumed for any particular purpose. Often, the assets in Applications power specific
 * outcomes, for example a Pricing Application.
 */
export type ApplicationRelationshipsArgs = {
  input: RelationshipsInput;
};

export type ApplicationAssociation = {
  __typename?: 'ApplicationAssociation';
  /** The application related to the assocaited urn */
  application: Application;
  /** Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
};

/** Configuration for the application sidebar section */
export type ApplicationConfig = {
  __typename?: 'ApplicationConfig';
  /** Whether to show the application in the navigation sidebar */
  showApplicationInNavigation?: Maybe<Scalars['Boolean']>;
  /** Whether to show the application sidebar section even when empty */
  showSidebarSectionWhenEmpty?: Maybe<Scalars['Boolean']>;
};

/** Input required to fetch the entities inside of a Application. */
export type ApplicationEntitiesInput = {
  /** The number of entities to include in result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Optional query filter for particular entities inside the Application */
  query?: Maybe<Scalars['String']>;
  /** The offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Properties about an Application */
export type ApplicationProperties = {
  __typename?: 'ApplicationProperties';
  /** Custom properties of the Application */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Description of the Application */
  description?: Maybe<Scalars['String']>;
  /** External URL for the Appliation (most likely GitHub repo where Application may be managed as code) */
  externalUrl?: Maybe<Scalars['String']>;
  /** Display name of the Application */
  name: Scalars['String'];
  /** Number of children entities inside of the Application. This number includes soft deleted entities. */
  numAssets?: Maybe<Scalars['Int']>;
};

export type ArrayPrimaryKeyInput = {
  arrayField: Scalars['String'];
  keys: Array<Scalars['String']>;
};

/** A versioned aspect, or single group of related metadata, associated with an Entity and having a unique version */
export type Aspect = {
  /** The version of the aspect, where zero represents the latest version */
  version?: Maybe<Scalars['Long']>;
};

/** Params to configure what list of aspects should be fetched by the aspects property */
export type AspectParams = {
  /**
   * Fetch using aspect names
   * If absent, returns all aspects matching other inputs
   */
  aspectNames?: Maybe<Array<Scalars['String']>>;
  /** Only fetch auto render aspects */
  autoRenderOnly?: Maybe<Scalars['Boolean']>;
};

/** Details for the frontend on how the raw aspect should be rendered */
export type AspectRenderSpec = {
  __typename?: 'AspectRenderSpec';
  /** Name to refer to the aspect type by for the UI. Powered by the renderSpec annotation on the aspect model */
  displayName?: Maybe<Scalars['String']>;
  /** Format the aspect should be displayed in for the UI. Powered by the renderSpec annotation on the aspect model */
  displayType?: Maybe<Scalars['String']>;
  /** Field in the aspect payload to index into for rendering. */
  key?: Maybe<Scalars['String']>;
};

/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type Assertion = Entity & EntityWithRelationships & {
  __typename?: 'Assertion';
  /** The actions associated with the Assertion */
  actions?: Maybe<AssertionActions>;
  /**
   * Experimental API.
   * For fetching extra aspects that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Details about assertion */
  info?: Maybe<AssertionInfo>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Standardized platform urn where the assertion is evaluated */
  platform: DataPlatform;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /**
   * Lifecycle events detailing individual runs of this assertion. If startTimeMillis & endTimeMillis are not provided, the most
   * recent events will be returned.
   */
  runEvents?: Maybe<AssertionRunEventsResult>;
  /** Status metadata of the assertion */
  status?: Maybe<Status>;
  /** The standard tags for the Assertion */
  tags?: Maybe<GlobalTags>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the Assertion */
  urn: Scalars['String'];
};


/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionLineageArgs = {
  input: LineageInput;
};


/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionRelationshipsArgs = {
  input: RelationshipsInput;
};


/** An assertion represents a programmatic validation, check, or test performed periodically against another Entity. */
export type AssertionRunEventsArgs = {
  endTimeMillis?: Maybe<Scalars['Long']>;
  filter?: Maybe<FilterInput>;
  limit?: Maybe<Scalars['Int']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
  status?: Maybe<AssertionRunStatus>;
};

/** An action associated with an assertion */
export type AssertionAction = {
  __typename?: 'AssertionAction';
  /** The type of the actions */
  type: AssertionActionType;
};

/** The type of the Action */
export enum AssertionActionType {
  /** Raise an incident. */
  RaiseIncident = 'RAISE_INCIDENT',
  /** Resolve open incidents related to the assertion. */
  ResolveIncident = 'RESOLVE_INCIDENT'
}

/** Some actions associated with an assertion */
export type AssertionActions = {
  __typename?: 'AssertionActions';
  /** Actions to be executed on failed assertion run. */
  onFailure: Array<AssertionAction>;
  /** Actions to be executed on successful assertion run. */
  onSuccess: Array<AssertionAction>;
};

export type AssertionHealthStatusByType = {
  __typename?: 'AssertionHealthStatusByType';
  /** The timestamp when the last assertion of this type group with the given status ran */
  lastStatusResultAt?: Maybe<Scalars['Long']>;
  /** The status of the assertions in the given type group */
  status: HealthStatus;
  /** The number of assertions in the given type group that have the given status (PASS, WARN, FAIL) */
  statusCount: Scalars['Int'];
  /** The number of assertions in the given type group */
  total: Scalars['Int'];
  /** The type group of assertions */
  type: AssertionType;
};

/** Type of assertion. Assertion types can evolve to span Datasets, Flows (Pipelines), Models, Features etc. */
export type AssertionInfo = {
  __typename?: 'AssertionInfo';
  /** Information about Custom assertion */
  customAssertion?: Maybe<CustomAssertionInfo>;
  /** Dataset-specific assertion information */
  datasetAssertion?: Maybe<DatasetAssertionInfo>;
  /** An optional human-readable description of the assertion */
  description?: Maybe<Scalars['String']>;
  /** URL where assertion details are available */
  externalUrl?: Maybe<Scalars['String']>;
  /** Information about a Field Assertion */
  fieldAssertion?: Maybe<FieldAssertionInfo>;
  /** Information about an Freshness Assertion */
  freshnessAssertion?: Maybe<FreshnessAssertionInfo>;
  /** The time that the status last changed and the actor who changed it */
  lastUpdated?: Maybe<AuditStamp>;
  /** Schema assertion, e.g. defining the expected structure for an asset. */
  schemaAssertion?: Maybe<SchemaAssertionInfo>;
  /** The source or origin of the Assertion definition. */
  source?: Maybe<AssertionSource>;
  /** Information about a SQL Assertion */
  sqlAssertion?: Maybe<SqlAssertionInfo>;
  /** Top-level type of the assertion. */
  type: AssertionType;
  /** Information about an Volume Assertion */
  volumeAssertion?: Maybe<VolumeAssertionInfo>;
};

/** The result of evaluating an assertion. */
export type AssertionResult = {
  __typename?: 'AssertionResult';
  /** Observed aggregate value for evaluated batch */
  actualAggValue?: Maybe<Scalars['Float']>;
  /** Error details, if type is ERROR */
  error?: Maybe<AssertionResultError>;
  /** URL where full results are available */
  externalUrl?: Maybe<Scalars['String']>;
  /** Number of rows with missing value for evaluated batch */
  missingCount?: Maybe<Scalars['Long']>;
  /** Native results / properties of evaluation */
  nativeResults?: Maybe<Array<StringMapEntry>>;
  /** Number of rows for evaluated batch */
  rowCount?: Maybe<Scalars['Long']>;
  /** The final result, e.g. either SUCCESS or FAILURE. */
  type: AssertionResultType;
  /** Number of rows with unexpected value for evaluated batch */
  unexpectedCount?: Maybe<Scalars['Long']>;
};

/** An error encountered when evaluating an AssertionResult */
export type AssertionResultError = {
  __typename?: 'AssertionResultError';
  /** Additional metadata depending on the type of error */
  properties?: Maybe<Array<StringMapEntry>>;
  /** The type of error encountered */
  type: AssertionResultErrorType;
};

/** Input for reporting an Error during Assertion Run */
export type AssertionResultErrorInput = {
  /** The error message with details of error encountered */
  message: Scalars['String'];
  /** The type of error encountered */
  type: AssertionResultErrorType;
};

/** The type of error encountered when evaluating an AssertionResult */
export enum AssertionResultErrorType {
  /** Error while executing a custom SQL assertion */
  CustomSqlError = 'CUSTOM_SQL_ERROR',
  /** Error while executing a field assertion */
  FieldAssertionError = 'FIELD_ASSERTION_ERROR',
  /** Insufficient data to evaluate assertion */
  InsufficientData = 'INSUFFICIENT_DATA',
  /** Invalid parameters were detected */
  InvalidParameters = 'INVALID_PARAMETERS',
  /** Event type not supported by the specified source */
  InvalidSourceType = 'INVALID_SOURCE_TYPE',
  /** Source is unreachable */
  SourceConnectionError = 'SOURCE_CONNECTION_ERROR',
  /** Source query failed to execute */
  SourceQueryFailed = 'SOURCE_QUERY_FAILED',
  /** Unknown error */
  UnknownError = 'UNKNOWN_ERROR',
  /** Platform not supported */
  UnsupportedPlatform = 'UNSUPPORTED_PLATFORM'
}

/** Input for reporting result of the assertion */
export type AssertionResultInput = {
  /** Error details, if type is ERROR */
  error?: Maybe<AssertionResultErrorInput>;
  /** Native platform URL of the Assertion Run Event */
  externalUrl?: Maybe<Scalars['String']>;
  /**
   * Additional metadata representing about the native results of the assertion.
   * These will be displayed alongside the result.
   * It should be used to capture additional context that is useful for the user.
   */
  properties?: Maybe<Array<StringMapEntryInput>>;
  /**
   * Optional: Provide a timestamp associated with the run event. If not provided, one will be generated for you based
   * on the current time.
   */
  timestampMillis?: Maybe<Scalars['Long']>;
  /** The final result of assertion, e.g. either SUCCESS or FAILURE. */
  type: AssertionResultType;
};

/** The result type of an assertion, success or failure. */
export enum AssertionResultType {
  /** The assertion errored. */
  Error = 'ERROR',
  /** The assertion failed. */
  Failure = 'FAILURE',
  /** The assertion has not yet been fully evaluated. */
  Init = 'INIT',
  /** The assertion succeeded. */
  Success = 'SUCCESS'
}

/** An event representing an event in the assertion evaluation lifecycle. */
export type AssertionRunEvent = TimeSeriesAspect & {
  __typename?: 'AssertionRunEvent';
  /** Urn of entity on which the assertion is applicable */
  asserteeUrn: Scalars['String'];
  /** Urn of assertion which is evaluated */
  assertionUrn: Scalars['String'];
  /** Specification of the batch which this run is evaluating */
  batchSpec?: Maybe<BatchSpec>;
  /** The time at which the run event was last observed by the DataHub system - ie, when it was reported by external systems */
  lastObservedMillis?: Maybe<Scalars['Long']>;
  /** Information about the partition that was evaluated */
  partitionSpec?: Maybe<PartitionSpec>;
  /** Results of assertion, present if the status is COMPLETE */
  result?: Maybe<AssertionResult>;
  /** Native (platform-specific) identifier for this run */
  runId: Scalars['String'];
  /** Runtime parameters of evaluation */
  runtimeContext?: Maybe<Array<StringMapEntry>>;
  /** The status of the assertion run as per this timeseries event */
  status: AssertionRunStatus;
  /** The time at which the assertion was evaluated */
  timestampMillis: Scalars['Long'];
};

/** Result returned when fetching run events for an assertion. */
export type AssertionRunEventsResult = {
  __typename?: 'AssertionRunEventsResult';
  /** The number of errored run events */
  errored: Scalars['Int'];
  /** The number of failed run events */
  failed: Scalars['Int'];
  /** The run events themselves */
  runEvents: Array<AssertionRunEvent>;
  /** The number of succeeded run events */
  succeeded: Scalars['Int'];
  /** The total number of run events returned */
  total: Scalars['Int'];
};

/** The state of an assertion run, as defined within an Assertion Run Event. */
export enum AssertionRunStatus {
  /** An assertion run has completed. */
  Complete = 'COMPLETE'
}

/** The source of an Assertion */
export type AssertionSource = {
  __typename?: 'AssertionSource';
  /** The time at which the assertion was initially created and the actor who created it */
  created?: Maybe<AuditStamp>;
  /** The source type */
  type: AssertionSourceType;
};

/** The source of an assertion */
export enum AssertionSourceType {
  /** The assertion was defined and managed externally of DataHub. */
  External = 'EXTERNAL',
  /** The assertion was inferred, e.g. from offline AI / ML models. */
  Inferred = 'INFERRED',
  /** The assertion was defined natively on DataHub by a user. */
  Native = 'NATIVE'
}

/** An "aggregation" function that can be applied to column values of a Dataset to create the input to an Assertion Operator. */
export enum AssertionStdAggregation {
  /** Assertion is applied on all columns */
  Columns = 'COLUMNS',
  /** Assertion is applied on number of columns */
  ColumnCount = 'COLUMN_COUNT',
  /** Assertion is applied on individual column value */
  Identity = 'IDENTITY',
  /** Assertion is applied on column std deviation */
  Max = 'MAX',
  /** Assertion is applied on column mean */
  Mean = 'MEAN',
  /** Assertion is applied on column median */
  Median = 'MEDIAN',
  /** Assertion is applied on column min */
  Min = 'MIN',
  /** Assertion is applied on number of null values in column */
  NullCount = 'NULL_COUNT',
  /** Assertion is applied on proportion of null values in column */
  NullProportion = 'NULL_PROPORTION',
  /** Assertion is applied on number of rows */
  RowCount = 'ROW_COUNT',
  /** Assertion is applied on column std deviation */
  Stddev = 'STDDEV',
  /** Assertion is applied on column sum */
  Sum = 'SUM',
  /** Assertion is applied on number of distinct values in column */
  UniqueCount = 'UNIQUE_COUNT',
  /** Assertion is applied on proportion of distinct values in column */
  UniquePropotion = 'UNIQUE_PROPOTION',
  /** Other */
  Native = '_NATIVE_'
}

/** A standard operator or condition that constitutes an assertion definition */
export enum AssertionStdOperator {
  /** Value being asserted is between min_value and max_value */
  Between = 'BETWEEN',
  /** Value being asserted contains value */
  Contain = 'CONTAIN',
  /** Value being asserted ends with value */
  EndWith = 'END_WITH',
  /** Value being asserted is equal to value */
  EqualTo = 'EQUAL_TO',
  /** Value being asserted is greater than min_value */
  GreaterThan = 'GREATER_THAN',
  /** Value being asserted is greater than or equal to min_value */
  GreaterThanOrEqualTo = 'GREATER_THAN_OR_EQUAL_TO',
  /** Value being asserted is one of the array values */
  In = 'IN',
  /** Value being asserted is false */
  IsFalse = 'IS_FALSE',
  /** Value being asserted is true */
  IsTrue = 'IS_TRUE',
  /** Value being asserted is less than max_value */
  LessThan = 'LESS_THAN',
  /** Value being asserted is less than or equal to max_value */
  LessThanOrEqualTo = 'LESS_THAN_OR_EQUAL_TO',
  /** Value being asserted is not equal to value */
  NotEqualTo = 'NOT_EQUAL_TO',
  /** Value being asserted is not in one of the array values. */
  NotIn = 'NOT_IN',
  /** Value being asserted is not null */
  NotNull = 'NOT_NULL',
  /** Value being asserted is null */
  Null = 'NULL',
  /** Value being asserted matches the regex value. */
  RegexMatch = 'REGEX_MATCH',
  /** Value being asserted starts with value */
  StartWith = 'START_WITH',
  /** Other */
  Native = '_NATIVE_'
}

/** Parameter for AssertionStdOperator. */
export type AssertionStdParameter = {
  __typename?: 'AssertionStdParameter';
  /** The type of the parameter */
  type: AssertionStdParameterType;
  /** The parameter value */
  value: Scalars['String'];
};

/** The type of an AssertionStdParameter */
export enum AssertionStdParameterType {
  /** A list of values. When used, the value should be formatted as a serialized JSON array. */
  List = 'LIST',
  /** A numeric value */
  Number = 'NUMBER',
  /** A set of values. When used, the value should be formatted as a serialized JSON array. */
  Set = 'SET',
  /** A string value */
  String = 'STRING',
  /** A value of unknown type */
  Unknown = 'UNKNOWN'
}

/** Parameters for AssertionStdOperators */
export type AssertionStdParameters = {
  __typename?: 'AssertionStdParameters';
  /** The maxValue parameter of an assertion */
  maxValue?: Maybe<AssertionStdParameter>;
  /** The minValue parameter of an assertion */
  minValue?: Maybe<AssertionStdParameter>;
  /** The value parameter of an assertion */
  value?: Maybe<AssertionStdParameter>;
};

/** The top-level assertion type. */
export enum AssertionType {
  /** A custom assertion. */
  Custom = 'CUSTOM',
  /** A single-dataset assertion. */
  Dataset = 'DATASET',
  /** A schema or structural assertion. */
  DataSchema = 'DATA_SCHEMA',
  /** A structured assertion targeting a specific column or field of the Dataset. */
  Field = 'FIELD',
  /** An assertion which indicates when a particular operation should occur to an asset. */
  Freshness = 'FRESHNESS',
  /** A raw SQL-statement based assertion. */
  Sql = 'SQL',
  /** An assertion which indicates how much data should be available for a particular asset. */
  Volume = 'VOLUME'
}

/** An enum to represent a type of change in an assertion value, metric, or measurement. */
export enum AssertionValueChangeType {
  /** A change that is defined in absolute terms. */
  Absolute = 'ABSOLUTE',
  /**
   * A change that is defined in relative terms using percentage change
   * from the original value.
   */
  Percentage = 'PERCENTAGE'
}

/** The params required if the module is type ASSET_COLLECTION */
export type AssetCollectionModuleParams = {
  __typename?: 'AssetCollectionModuleParams';
  /** The list of asset urns for the asset collection module */
  assetUrns: Array<Scalars['String']>;
  /**
   * Optional dynamic filter
   *
   * The stringified json representing the logical predicate built in the UI to select assets.
   * This predicate is turned into orFilters to send through graphql since graphql doesn't support
   * arbitrary nesting. This string is used to restore the UI for this logical predicate.
   */
  dynamicFilterJson?: Maybe<Scalars['String']>;
};

/** Input for the params required if the module is type ASSET_COLLECTION */
export type AssetCollectionModuleParamsInput = {
  /** The list of asset urns for the asset collection module */
  assetUrns: Array<Scalars['String']>;
  /**
   * Optional dynamic filters
   *
   * The stringified json representing the logical predicate built in the UI to select assets.
   * This predicate is turned into orFilters to send through graphql since graphql doesn't support
   * arbitrary nesting. This string is used to restore the UI for this logical predicate.
   */
  dynamicFilterJson?: Maybe<Scalars['String']>;
};

/** Settings associated with this asset */
export type AssetSettings = {
  __typename?: 'AssetSettings';
  /** Information related to the asset summary for this asset */
  assetSummary?: Maybe<AssetSummarySettings>;
};

/** Information regarding asset stats */
export type AssetStatsResult = {
  __typename?: 'AssetStatsResult';
  /** The oldest dataset profile in our index */
  oldestDatasetProfileTime?: Maybe<Scalars['Long']>;
  /** The oldest dataset usage in our index */
  oldestDatasetUsageTime?: Maybe<Scalars['Long']>;
  /** The oldest dataset operation in our index */
  oldestOperationTime?: Maybe<Scalars['Long']>;
};

/** Information related to the asset summary for this asset */
export type AssetSummarySettings = {
  __typename?: 'AssetSummarySettings';
  /** The list of templates applied to this asset in order. Right now we only expect one. */
  templates?: Maybe<Array<AssetSummarySettingsTemplate>>;
};

/** Object containing the template and any additional info for asset summary settings */
export type AssetSummarySettingsTemplate = {
  __typename?: 'AssetSummarySettingsTemplate';
  /** The page template entity */
  template?: Maybe<DataHubPageTemplate>;
};

/** A time stamp along with an optional actor */
export type AuditStamp = {
  __typename?: 'AuditStamp';
  /** Who performed the audited action */
  actor?: Maybe<Scalars['String']>;
  /** When the audited action took place */
  time: Scalars['Long'];
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
  /** An optional entity field name to autocomplete on */
  field?: Maybe<Scalars['String']>;
  /** Faceted filters applied to autocomplete results */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** The maximum number of autocomplete results to be returned */
  limit?: Maybe<Scalars['Int']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The raw query string */
  query: Scalars['String'];
  /** Entity type to be autocompleted against */
  type?: Maybe<EntityType>;
};

/** Input for performing an auto completion query against a a set of Metadata Entities */
export type AutoCompleteMultipleInput = {
  /** An optional field to autocomplete against */
  field?: Maybe<Scalars['String']>;
  /** Faceted filters applied to autocomplete results */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** The maximum number of autocomplete results */
  limit?: Maybe<Scalars['Int']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The raw query string */
  query: Scalars['String'];
  /**
   * Entity types to be autocompleted against
   * Optional, if none supplied, all searchable types will be autocompleted against
   */
  types?: Maybe<Array<EntityType>>;
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
  /** A list of entities to render in autocomplete */
  entities: Array<Entity>;
  /** The autocompletion results for specified entity type */
  suggestions: Array<Scalars['String']>;
  /** Entity type */
  type: EntityType;
};

/** The results returned on a single entity autocomplete query */
export type AutoCompleteResults = {
  __typename?: 'AutoCompleteResults';
  /** A list of entities to render in autocomplete */
  entities: Array<Entity>;
  /** The query string */
  query: Scalars['String'];
  /** The autocompletion results */
  suggestions: Array<Scalars['String']>;
};

/** AWS Bedrock provider-specific configuration */
export type AwsProviderConfig = {
  __typename?: 'AwsProviderConfig';
  /** AWS region where Bedrock is accessed (e.g., "us-west-2") */
  region: Scalars['String'];
};

/** For consumption by UI only */
export type BarChart = {
  __typename?: 'BarChart';
  bars: Array<NamedBar>;
  title: Scalars['String'];
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
  /** The ownership type to remove, optional. By default will remove regardless of ownership type. */
  ownershipTypeUrn?: Maybe<Scalars['String']>;
  /** The target assets to attach the owners to */
  resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when adding tags to a batch of assets */
export type BatchAddTagsInput = {
  /** The target assets to attach the tags to */
  resources: Array<ResourceRefInput>;
  /** The primary key of the Tags */
  tagUrns: Array<Scalars['String']>;
};

/** Input provided when adding glossary terms to a batch of assets */
export type BatchAddTermsInput = {
  /** The target assets to attach the glossary terms to */
  resources: Array<Maybe<ResourceRefInput>>;
  /** The primary key of the Glossary Terms */
  termUrns: Array<Scalars['String']>;
};

/** Input for batch assigning a form to different entities */
export type BatchAssignFormInput = {
  /** The entities that this form is being assigned to */
  entityUrns: Array<Scalars['String']>;
  /** The urn of the form being assigned to entities */
  formUrn: Scalars['String'];
};

/** Input provided when batch assigning a role to a list of users */
export type BatchAssignRoleInput = {
  /** The urns of the actors to assign the role to */
  actors: Array<Scalars['String']>;
  /** The urn of the role to assign to the actors. If undefined, will remove the role. */
  roleUrn?: Maybe<Scalars['String']>;
};

/** Arguments provided to batch update Dataset entities */
export type BatchDatasetUpdateInput = {
  /** Arguments provided to update the Dataset */
  update: DatasetUpdateInput;
  /** Primary key of the Dataset to which the update will be applied */
  urn: Scalars['String'];
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

/** Input for batch removing a form from different entities */
export type BatchRemoveFormInput = {
  /** The entities that this form is being removed from */
  entityUrns: Array<Scalars['String']>;
  /** The urn of the form being removed from entities */
  formUrn: Scalars['String'];
};

/** Input provided when removing owners from a batch of assets */
export type BatchRemoveOwnersInput = {
  /** The primary key of the owners */
  ownerUrns: Array<Scalars['String']>;
  /** The ownership type to remove, optional. By default will remove regardless of ownership type. */
  ownershipTypeUrn?: Maybe<Scalars['String']>;
  /** The target assets to remove the owners from */
  resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when removing tags from a batch of assets */
export type BatchRemoveTagsInput = {
  /** The target assets to remove the tags from */
  resources: Array<Maybe<ResourceRefInput>>;
  /** The primary key of the Tags */
  tagUrns: Array<Scalars['String']>;
};

/** Input provided when removing glossary terms from a batch of assets */
export type BatchRemoveTermsInput = {
  /** The target assets to remove the glossary terms from */
  resources: Array<Maybe<ResourceRefInput>>;
  /** The primary key of the Glossary Terms */
  termUrns: Array<Scalars['String']>;
};

/** Input properties required for batch setting a Application on other entities */
export type BatchSetApplicationInput = {
  /**
   * The urn of the application you are setting on a group of resources.
   * If this is null, the Application will be unset for the given resources.
   */
  applicationUrn?: Maybe<Scalars['String']>;
  /** The urns of the entities the given application should be set on */
  resourceUrns: Array<Scalars['String']>;
};

/** Input properties required for batch setting a DataProduct on other entities */
export type BatchSetDataProductInput = {
  /**
   * The urn of the data product you are setting on a group of resources.
   * If this is null, the Data Product will be unset for the given resources.
   */
  dataProductUrn?: Maybe<Scalars['String']>;
  /** The urns of the entities the given data product should be set on */
  resourceUrns: Array<Scalars['String']>;
};

/**
 * Input for batch adding/removing assets to/from multiple data products.
 * Available when multipleDataProductsPerAsset feature flag is enabled.
 */
export type BatchSetDataProductsInput = {
  /** The urns of the data products to add/remove assets to/from */
  dataProductUrns: Array<Scalars['String']>;
  /** The urns of the entities (assets) to add/remove */
  resourceUrns: Array<Scalars['String']>;
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
  /** Custom properties of the Batch */
  customProperties?: Maybe<Array<StringMapEntry>>;
  /** Any limit to the number of rows in the batch, if applied */
  limit?: Maybe<Scalars['Int']>;
  /** The native identifier as specified by the system operating on the batch. */
  nativeBatchId?: Maybe<Scalars['String']>;
  /** A query that identifies a batch of data */
  query?: Maybe<Scalars['String']>;
};

/** Input properties required for batch unsetting a specific Application from entities */
export type BatchUnsetApplicationInput = {
  /** The urn of the application to be removed from the resources */
  applicationUrn: Scalars['String'];
  /** The urns of the entities the given application should be removed from */
  resourceUrns: Array<Scalars['String']>;
};

/** Input provided when updating the deprecation status for a batch of assets. */
export type BatchUpdateDeprecationInput = {
  /** Optional - The time user plan to decommission this entity */
  decommissionTime?: Maybe<Scalars['Long']>;
  /** Whether the Entity is marked as deprecated. */
  deprecated: Scalars['Boolean'];
  /** Optional - Additional information about the entity deprecation plan */
  note?: Maybe<Scalars['String']>;
  /** Optional - URN to replace the entity with */
  replacement?: Maybe<Scalars['String']>;
  /** The target assets to attach the tags to */
  resources: Array<Maybe<ResourceRefInput>>;
};

/** Input provided when updating the soft-deleted status for a batch of assets */
export type BatchUpdateSoftDeletedInput = {
  /** Whether to mark the asset as soft-deleted (hidden) */
  deleted: Scalars['Boolean'];
  /** The urns of the assets to soft delete */
  urns: Array<Scalars['String']>;
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
  /** The number of elements included in the results */
  count?: Maybe<Scalars['Int']>;
  /**
   * Deprecated in favor of the more expressive orFilters field
   * Facet filters to apply to search results. These will be 'AND'-ed together.
   */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The browse path */
  path?: Maybe<Array<Scalars['String']>>;
  /** The starting point of paginated results */
  start?: Maybe<Scalars['Int']>;
  /** The browse entity type */
  type: EntityType;
};

/** A hierarchical entity path */
export type BrowsePath = {
  __typename?: 'BrowsePath';
  /** The components of the browse path */
  path: Array<Scalars['String']>;
};

export type BrowsePathEntry = {
  __typename?: 'BrowsePathEntry';
  /**
   * An optional entity associated with this browse entry. This will usually be a container entity.
   * If this entity is not populated, the name must be used.
   */
  entity?: Maybe<Entity>;
  /** The path name of a group of browse results */
  name: Scalars['String'];
};

/** A hierarchical entity path V2 */
export type BrowsePathV2 = {
  __typename?: 'BrowsePathV2';
  /** The components of the browse path */
  path: Array<BrowsePathEntry>;
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
  /** The number of entities within the group */
  count: Scalars['Long'];
  /** The path name of a group of browse results */
  name: Scalars['String'];
};

/** A group of Entities under a given browse path */
export type BrowseResultGroupV2 = {
  __typename?: 'BrowseResultGroupV2';
  /** The number of entities within the group */
  count: Scalars['Long'];
  /**
   * An optional entity associated with this browse group. This will usually be a container entity.
   * If this entity is not populated, the name must be used.
   */
  entity?: Maybe<Entity>;
  /** Whether or not there are any more groups underneath this group */
  hasSubGroups: Scalars['Boolean'];
  /** The path name of a group of browse results */
  name: Scalars['String'];
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
  /** The number of elements included in the results */
  count: Scalars['Int'];
  /** The browse results */
  entities: Array<Entity>;
  /** The groups present at the provided browse path */
  groups: Array<BrowseResultGroup>;
  /** Metadata containing resulting browse groups */
  metadata: BrowseResultMetadata;
  /** The starting point of paginated results */
  start: Scalars['Int'];
  /** The total number of browse results under the path with filters applied */
  total: Scalars['Int'];
};

/** The results of a browse path V2 traversal query */
export type BrowseResultsV2 = {
  __typename?: 'BrowseResultsV2';
  /** The number of groups included in the results */
  count: Scalars['Int'];
  /** The groups present at the provided browse path V2 */
  groups: Array<BrowseResultGroupV2>;
  /** Metadata containing resulting browse groups */
  metadata: BrowseResultMetadata;
  /** The starting point of paginated results */
  start: Scalars['Int'];
  /** The total number of browse groups under the path with filters applied */
  total: Scalars['Int'];
};

/** Input required for browse queries */
export type BrowseV2Input = {
  /** The number of elements included in the results */
  count?: Maybe<Scalars['Int']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The browse path V2 - a list with each entry being part of the browse path V2 */
  path?: Maybe<Array<Scalars['String']>>;
  /** The search query string */
  query?: Maybe<Scalars['String']>;
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** The starting point of paginated results */
  start?: Maybe<Scalars['Int']>;
  /** The browse entity type - deprecated use types instead */
  type?: Maybe<EntityType>;
  /** The browse entity type - deprecated use types instead. If not provided, all types will be used. */
  types?: Maybe<Array<EntityType>>;
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

/** Information where a file is stored */
export type BucketStorageLocation = {
  __typename?: 'BucketStorageLocation';
  /** The storage bucket this file is stored in */
  storageBucket: Scalars['String'];
  /** The key for where this file is stored inside of the given bucket */
  storageKey: Scalars['String'];
};

/** A Business Attribute, or a logical schema Field */
export type BusinessAttribute = Entity & {
  __typename?: 'BusinessAttribute';
  /** References to internal resources related to Business Attribute */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the Business Attribute */
  ownership?: Maybe<Ownership>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Properties about a Business Attribute */
  properties?: Maybe<BusinessAttributeInfo>;
  /** List of relationships between the source Entity and some destination entities with a given types */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status of the Dataset */
  status?: Maybe<Status>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Data Product */
  urn: Scalars['String'];
};


/** A Business Attribute, or a logical schema Field */
export type BusinessAttributeRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Input required to attach business attribute to an entity */
export type BusinessAttributeAssociation = {
  __typename?: 'BusinessAttributeAssociation';
  /** Reference back to the associated urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** Business Attribute itself */
  businessAttribute: BusinessAttribute;
};

/** Business Attribute type */
export type BusinessAttributeInfo = {
  __typename?: 'BusinessAttributeInfo';
  /** An AuditStamp corresponding to the creation of this chart */
  created: AuditStamp;
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** An optional AuditStamp corresponding to the deletion of this chart */
  deleted?: Maybe<AuditStamp>;
  /** description of business attribute */
  description?: Maybe<Scalars['String']>;
  /** Glossary terms associated with the business attribute */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** An AuditStamp corresponding to the modification of this chart */
  lastModified: AuditStamp;
  /** name of the business attribute */
  name: Scalars['String'];
  /** Tags associated with the business attribute */
  tags?: Maybe<GlobalTags>;
  /** Platform independent field type of the field */
  type?: Maybe<SchemaFieldDataType>;
};

export type BusinessAttributeInfoInput = {
  /** description of business attribute */
  description?: Maybe<Scalars['String']>;
  /** name of the business attribute */
  name: Scalars['String'];
  /** Platform independent field type of the field */
  type?: Maybe<SchemaFieldDataType>;
};

/** Business attributes attached to the metadata */
export type BusinessAttributes = {
  __typename?: 'BusinessAttributes';
  /** Business Attribute attached to the Metadata Entity */
  businessAttribute?: Maybe<BusinessAttributeAssociation>;
};

/** Input for cancelling an execution request input */
export type CancelIngestionExecutionRequestInput = {
  /** Urn of the specific execution request to cancel */
  executionRequestUrn: Scalars['String'];
  /** Urn of the ingestion source */
  ingestionSourceUrn: Scalars['String'];
};

export type CaveatDetails = {
  __typename?: 'CaveatDetails';
  /** Caveat Description */
  caveatDescription?: Maybe<Scalars['String']>;
  /** Relevant groups that were not represented in the evaluation dataset */
  groupsNotRepresented?: Maybe<Array<Scalars['String']>>;
  /** Did the results suggest any further testing */
  needsFurtherTesting?: Maybe<Scalars['Boolean']>;
};

export type CaveatsAndRecommendations = {
  __typename?: 'CaveatsAndRecommendations';
  /** Caveats on using this MLModel */
  caveats?: Maybe<CaveatDetails>;
  /** Ideal characteristics of an evaluation dataset for this MLModel */
  idealDatasetCharacteristics?: Maybe<Array<Scalars['String']>>;
  /** Recommendations on where this MLModel should be used */
  recommendations?: Maybe<Scalars['String']>;
};

/** For consumption by UI only */
export type Cell = {
  __typename?: 'Cell';
  entity?: Maybe<Entity>;
  linkParams?: Maybe<LinkParams>;
  value: Scalars['String'];
};

/** Captures information about who created/last modified/deleted the entity and when */
export type ChangeAuditStamps = {
  __typename?: 'ChangeAuditStamps';
  /** An AuditStamp corresponding to the creation */
  created: AuditStamp;
  /** An optional AuditStamp corresponding to the deletion */
  deleted?: Maybe<AuditStamp>;
  /** An AuditStamp corresponding to the modification */
  lastModified: AuditStamp;
};

/** Enum of CategoryTypes */
export enum ChangeCategoryType {
  /** When documentation has been edited */
  Documentation = 'DOCUMENTATION',
  /** When glossary terms have been added or removed */
  GlossaryTerm = 'GLOSSARY_TERM',
  /** When ownership has been modified */
  Ownership = 'OWNERSHIP',
  /** When parent relationship has been modified */
  Parent = 'PARENT',
  /** When related entities have been added or removed */
  RelatedEntities = 'RELATED_ENTITIES',
  /** When tags have been added or removed */
  Tag = 'TAG',
  /** When technical schemas have been added or removed */
  TechnicalSchema = 'TECHNICAL_SCHEMA'
}

/** An individual change in a transaction */
export type ChangeEvent = {
  __typename?: 'ChangeEvent';
  /** The audit stamp of the change */
  auditStamp?: Maybe<AuditStamp>;
  /** The category of the change */
  category?: Maybe<ChangeCategoryType>;
  /** description of the change */
  description?: Maybe<Scalars['String']>;
  /** The modifier of the change */
  modifier?: Maybe<Scalars['String']>;
  /** The operation of the change */
  operation?: Maybe<ChangeOperationType>;
  /** The parameters of the change */
  parameters?: Maybe<Array<TimelineParameterEntry>>;
  /** The urn of the entity that was changed */
  urn: Scalars['String'];
};

/** Enum of types of changes */
export enum ChangeOperationType {
  /** When an element is added */
  Add = 'ADD',
  /** When an element is modified */
  Modify = 'MODIFY',
  /** When an element is removed */
  Remove = 'REMOVE'
}

/** A change transaction is a set of changes that were committed together. */
export type ChangeTransaction = {
  __typename?: 'ChangeTransaction';
  /** The type of the change */
  changeType: ChangeOperationType;
  /** The list of changes in this transaction */
  changes?: Maybe<Array<ChangeEvent>>;
  /** The last semantic version that this schema was changed in */
  lastSemanticVersion: Scalars['String'];
  /** The time at which the transaction was committed */
  timestampMillis: Scalars['Long'];
  /** Version stamp of the change */
  versionStamp: Scalars['String'];
};

/** A Chart Metadata Entity */
export type Chart = BrowsableEntity & Entity & EntityWithRelationships & {
  __typename?: 'Chart';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the chart. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** An id unique within the charting tool */
  chartId: Scalars['String'];
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** The deprecation status of the chart */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the Chart */
  domain?: Maybe<DomainAssociation>;
  /**
   * Deprecated, use editableProperties field instead
   * Additional read write information about the Chart
   * @deprecated Field no longer supported
   */
  editableInfo?: Maybe<ChartEditableProperties>;
  /** Additional read write properties about the Chart */
  editableProperties?: Maybe<ChartEditableProperties>;
  /** Embed information about the Chart */
  embed?: Maybe<Embed>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use tags instead
   * The structured tags associated with the chart
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** The structured glossary terms associated with the dashboard */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Experimental! The resolved health statuses of the asset */
  health?: Maybe<Array<Health>>;
  /** Incidents associated with the Chart */
  incidents?: Maybe<EntityIncidentsResult>;
  /**
   * Deprecated, use properties field instead
   * Additional read only information about the chart
   * @deprecated Field no longer supported
   */
  info?: Maybe<ChartInfo>;
  /** Input fields to power the chart */
  inputFields?: Maybe<InputFields>;
  /** References to internal resources related to the dashboard */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Ownership metadata of the chart */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of containers for this entity */
  parentContainers?: Maybe<ParentContainersResult>;
  /** Standardized platform urn where the chart is defined */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties about the Chart */
  properties?: Maybe<ChartProperties>;
  /** Info about the query which is used to render the chart */
  query?: Maybe<ChartQuery>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /**
   * Not yet implemented.
   *
   * Experimental - Summary operational & usage statistics about a Chart
   */
  statsSummary?: Maybe<ChartStatsSummary>;
  /** Status metadata of the chart */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** The tags associated with the chart */
  tags?: Maybe<GlobalTags>;
  /**
   * The chart tool name
   * Note that this field will soon be deprecated in favor a unified notion of Data Platform
   */
  tool: Scalars['String'];
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Chart */
  urn: Scalars['String'];
};


/** A Chart Metadata Entity */
export type ChartAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A Chart Metadata Entity */
export type ChartIncidentsArgs = {
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  count?: Maybe<Scalars['Int']>;
  priority?: Maybe<IncidentPriority>;
  stage?: Maybe<IncidentStage>;
  start?: Maybe<Scalars['Int']>;
  state?: Maybe<IncidentState>;
};


/** A Chart Metadata Entity */
export type ChartLineageArgs = {
  input: LineageInput;
};


/** A Chart Metadata Entity */
export type ChartRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** A Chart Metadata Entity */
export type ChartRelationshipsArgs = {
  input: RelationshipsInput;
};

/** A Notebook cell which contains chart as content */
export type ChartCell = {
  __typename?: 'ChartCell';
  /** Unique id for the cell. */
  cellId: Scalars['String'];
  /** Title of the cell */
  cellTitle: Scalars['String'];
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
  /** Access level for the chart */
  access?: Maybe<AccessLevel>;
  /** An AuditStamp corresponding to the creation of this chart */
  created: AuditStamp;
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** An optional AuditStamp corresponding to the deletion of this chart */
  deleted?: Maybe<AuditStamp>;
  /** Description of the chart */
  description?: Maybe<Scalars['String']>;
  /** Native platform URL of the chart */
  externalUrl?: Maybe<Scalars['String']>;
  /**
   * Deprecated, use relationship Consumes instead
   * Data sources for the chart
   * @deprecated Field no longer supported
   */
  inputs?: Maybe<Array<Dataset>>;
  /** An AuditStamp corresponding to the modification of this chart */
  lastModified: AuditStamp;
  /** The time when this chart last refreshed */
  lastRefreshed?: Maybe<Scalars['Long']>;
  /** Display name of the chart */
  name: Scalars['String'];
  /** Access level for the chart */
  type?: Maybe<ChartType>;
};

/** Additional read only properties about the chart */
export type ChartProperties = {
  __typename?: 'ChartProperties';
  /** Access level for the chart */
  access?: Maybe<AccessLevel>;
  /** An AuditStamp corresponding to the creation of this chart */
  created: AuditStamp;
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** An optional AuditStamp corresponding to the deletion of this chart */
  deleted?: Maybe<AuditStamp>;
  /** Description of the chart */
  description?: Maybe<Scalars['String']>;
  /** Native platform URL of the chart */
  externalUrl?: Maybe<Scalars['String']>;
  /** An AuditStamp corresponding to the modification of this chart */
  lastModified: AuditStamp;
  /** The time when this chart last refreshed */
  lastRefreshed?: Maybe<Scalars['Long']>;
  /** Display name of the chart */
  name: Scalars['String'];
  /** Access level for the chart */
  type?: Maybe<ChartType>;
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
  /** LookML */
  Lookml = 'LOOKML',
  /** Standard ANSI SQL */
  Sql = 'SQL'
}

/** Experimental - subject to change. A summary of usage metrics about a Chart. */
export type ChartStatsSummary = {
  __typename?: 'ChartStatsSummary';
  /** The top users in the past 30 days */
  topUsersLast30Days?: Maybe<Array<CorpUser>>;
  /** The unique user count in the past 30 days */
  uniqueUserCountLast30Days?: Maybe<Scalars['Int']>;
  /** The total view count for the chart */
  viewCount?: Maybe<Scalars['Int']>;
  /** The view count in the last 30 days */
  viewCountLast30Days?: Maybe<Scalars['Int']>;
};

/** The type of a Chart Entity */
export enum ChartType {
  /** An area chart */
  Area = 'AREA',
  /** Bar graph */
  Bar = 'BAR',
  /** A box plot chart */
  BoxPlot = 'BOX_PLOT',
  /** A Cohort Analysis chart */
  Cohort = 'COHORT',
  /** A histogram chart */
  Histogram = 'HISTOGRAM',
  /** A line chart */
  Line = 'LINE',
  /** Pie chart */
  Pie = 'PIE',
  /** Scatter plot */
  Scatter = 'SCATTER',
  /** Table */
  Table = 'TABLE',
  /** Markdown formatted text */
  Text = 'TEXT',
  /** A word cloud chart */
  WordCloud = 'WORD_CLOUD'
}

/** Arguments provided to update a Chart Entity */
export type ChartUpdateInput = {
  /** Update to editable properties */
  editableProperties?: Maybe<ChartEditablePropertiesUpdate>;
  /**
   * Deprecated, use tags field instead
   * Update to global tags
   */
  globalTags?: Maybe<GlobalTagsUpdate>;
  /** Update to ownership */
  ownership?: Maybe<OwnershipUpdate>;
  /** Update to tags */
  tags?: Maybe<GlobalTagsUpdate>;
};

/** Configurations related to DataHub Chrome extension */
export type ChromeExtensionConfig = {
  __typename?: 'ChromeExtensionConfig';
  /** Whether the Chrome Extension is enabled */
  enabled: Scalars['Boolean'];
  /** Whether lineage is enabled */
  lineageEnabled: Scalars['Boolean'];
};

/** A container of other Metadata Entities */
export type Container = Entity & {
  __typename?: 'Container';
  /** The Roles and the properties to access the container */
  access?: Maybe<Access>;
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** Fetch an Entity Container by primary key (urn) */
  container?: Maybe<Container>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** The deprecation status of the container */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the Dataset */
  domain?: Maybe<DomainAssociation>;
  /** Read-write properties that originate in DataHub */
  editableProperties?: Maybe<ContainerEditableProperties>;
  /** Children entities inside of the Container */
  entities?: Maybe<SearchResults>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the dataset */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the dataset */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Ownership metadata of the dataset */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of containers for this entity */
  parentContainers?: Maybe<ParentContainersResult>;
  /** Standardized platform. */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Read-only properties that originate in the source data platform */
  properties?: Maybe<ContainerProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the container */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Sub types of the container, e.g. "Database" etc */
  subTypes?: Maybe<SubTypes>;
  /** Tags used for searching dataset */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the container */
  urn: Scalars['String'];
};


/** A container of other Metadata Entities */
export type ContainerAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A container of other Metadata Entities */
export type ContainerEntitiesArgs = {
  input?: Maybe<ContainerEntitiesInput>;
};


/** A container of other Metadata Entities */
export type ContainerRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
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
  /** The number of entities to include in result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Optional query filter for particular entities inside the container */
  query?: Maybe<Scalars['String']>;
  /** The offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Read-only properties that originate in the source data platform */
export type ContainerProperties = {
  __typename?: 'ContainerProperties';
  /** Custom properties of the Container */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** System description of the Container */
  description?: Maybe<Scalars['String']>;
  /** Native platform URL of the Container */
  externalUrl?: Maybe<Scalars['String']>;
  /** Display name of the Container */
  name: Scalars['String'];
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
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** Additional read write properties about the group */
  editableProperties?: Maybe<CorpGroupEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use properties field instead
   * Additional read only info about the group
   * @deprecated Field no longer supported
   */
  info?: Maybe<CorpGroupInfo>;
  /** Group name eg wherehows dev, ask_metadata */
  name: Scalars['String'];
  /** Origin info about this group. */
  origin?: Maybe<Origin>;
  /** Ownership metadata of the Corp Group */
  ownership?: Maybe<Ownership>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties about the group */
  properties?: Maybe<CorpGroupProperties>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the group */
  urn: Scalars['String'];
};


/** A DataHub Group entity, which represents a Person on the Metadata Entity Graph */
export type CorpGroupAspectsArgs = {
  input?: Maybe<AspectParams>;
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
  /** Email address for the group */
  email?: Maybe<Scalars['String']>;
  /** A URL which points to a picture which user wants to set as a profile photo */
  pictureLink?: Maybe<Scalars['String']>;
  /** Slack handle for the group */
  slack?: Maybe<Scalars['String']>;
};

/**
 * Deprecated, use CorpUserProperties instead
 * Additional read only info about a group
 */
export type CorpGroupInfo = {
  __typename?: 'CorpGroupInfo';
  /**
   * Deprecated, do not use
   * owners of this group
   * @deprecated Field no longer supported
   */
  admins?: Maybe<Array<CorpUser>>;
  /** The description provided for the group */
  description?: Maybe<Scalars['String']>;
  /** The name to display when rendering the group */
  displayName?: Maybe<Scalars['String']>;
  /** email of this group */
  email?: Maybe<Scalars['String']>;
  /**
   * Deprecated, do not use
   * List of groups urns in this group
   * @deprecated Field no longer supported
   */
  groups?: Maybe<Array<Scalars['String']>>;
  /**
   * Deprecated, use relationship IsMemberOfGroup instead
   * List of ldap urn in this group
   * @deprecated Field no longer supported
   */
  members?: Maybe<Array<CorpUser>>;
};

/** Additional read only properties about a group */
export type CorpGroupProperties = {
  __typename?: 'CorpGroupProperties';
  /** The description provided for the group */
  description?: Maybe<Scalars['String']>;
  /** display name of this group */
  displayName?: Maybe<Scalars['String']>;
  /** email of this group */
  email?: Maybe<Scalars['String']>;
  /** Slack handle for the group */
  slack?: Maybe<Scalars['String']>;
};

/** Arguments provided to update a CorpGroup Entity */
export type CorpGroupUpdateInput = {
  /** DataHub description of the group */
  description?: Maybe<Scalars['String']>;
  /** Email address for the group */
  email?: Maybe<Scalars['String']>;
  /** A URL which points to a picture which user wants to set as a profile photo */
  pictureLink?: Maybe<Scalars['String']>;
  /** Slack handle for the group */
  slack?: Maybe<Scalars['String']>;
};

/** A DataHub User entity, which represents a Person on the Metadata Entity Graph */
export type CorpUser = Entity & {
  __typename?: 'CorpUser';
  /**
   * Experimental API.
   * For fetching extra aspects that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /**
   * Deprecated, use editableProperties field instead
   * Read write info about the corp user
   * @deprecated Field no longer supported
   */
  editableInfo?: Maybe<CorpUserEditableInfo>;
  /** Read write properties about the corp user */
  editableProperties?: Maybe<CorpUserEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use the tags field instead
   * The structured tags associated with the user
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /**
   * Deprecated, use properties field instead
   * Additional read only info about the corp user
   * @deprecated Field no longer supported
   */
  info?: Maybe<CorpUserInfo>;
  /** Whether or not this user is a native DataHub user */
  isNativeUser?: Maybe<Scalars['Boolean']>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties about the corp user */
  properties?: Maybe<CorpUserProperties>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Settings that a user can customize through the datahub ui */
  settings?: Maybe<CorpUserSettings>;
  /** The status of the user */
  status?: Maybe<CorpUserStatus>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** The tags associated with the user */
  tags?: Maybe<GlobalTags>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the user */
  urn: Scalars['String'];
  /**
   * A username associated with the user
   * This uniquely identifies the user within DataHub
   */
  username: Scalars['String'];
};


/** A DataHub User entity, which represents a Person on the Metadata Entity Graph */
export type CorpUserAspectsArgs = {
  input?: Maybe<AspectParams>;
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
  /** Flag controlling whether the V2 UI for DataHub is shown. */
  showThemeV2?: Maybe<Scalars['Boolean']>;
};

/**
 * Deprecated, use CorpUserEditableProperties instead
 * Additional read write info about a user
 */
export type CorpUserEditableInfo = {
  __typename?: 'CorpUserEditableInfo';
  /** About me section of the user */
  aboutMe?: Maybe<Scalars['String']>;
  /** Display name to show on DataHub */
  displayName?: Maybe<Scalars['String']>;
  /** A URL which points to a picture which user wants to set as a profile photo */
  pictureLink?: Maybe<Scalars['String']>;
  /** Skills that the user possesses */
  skills?: Maybe<Array<Scalars['String']>>;
  /** Teams that the user belongs to */
  teams?: Maybe<Array<Scalars['String']>>;
  /** Title to show on DataHub */
  title?: Maybe<Scalars['String']>;
};

/** Additional read write properties about a user */
export type CorpUserEditableProperties = {
  __typename?: 'CorpUserEditableProperties';
  /** About me section of the user */
  aboutMe?: Maybe<Scalars['String']>;
  /** Display name to show on DataHub */
  displayName?: Maybe<Scalars['String']>;
  /** Email address for the user */
  email?: Maybe<Scalars['String']>;
  /** User persona, if present */
  persona?: Maybe<DataHubPersona>;
  /** Phone number for the user */
  phone?: Maybe<Scalars['String']>;
  /** A URL which points to a picture which user wants to set as a profile photo */
  pictureLink?: Maybe<Scalars['String']>;
  /** Platforms commonly used by the user, if present. */
  platforms?: Maybe<Array<DataPlatform>>;
  /** Skills that the user possesses */
  skills?: Maybe<Array<Scalars['String']>>;
  /** The slack handle of the user */
  slack?: Maybe<Scalars['String']>;
  /** Teams that the user belongs to */
  teams?: Maybe<Array<Scalars['String']>>;
  /** Title to show on DataHub */
  title?: Maybe<Scalars['String']>;
};

/** Settings related to the home page for a user */
export type CorpUserHomePageSettings = {
  __typename?: 'CorpUserHomePageSettings';
  /** List of urns of the announcements dismissed by the User. */
  dismissedAnnouncementUrns?: Maybe<Array<Maybe<Scalars['String']>>>;
  /** The default page template for the User. */
  pageTemplate?: Maybe<DataHubPageTemplate>;
};

/**
 * Deprecated, use CorpUserProperties instead
 * Additional read only info about a user
 */
export type CorpUserInfo = {
  __typename?: 'CorpUserInfo';
  /** Whether the user is active */
  active: Scalars['Boolean'];
  /** two uppercase letters country code */
  countryCode?: Maybe<Scalars['String']>;
  /** Custom properties of the ldap */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** department id the user belong to */
  departmentId?: Maybe<Scalars['Long']>;
  /** department name this user belong to */
  departmentName?: Maybe<Scalars['String']>;
  /** Display name of the user */
  displayName?: Maybe<Scalars['String']>;
  /** Email address of the user */
  email?: Maybe<Scalars['String']>;
  /** first name of the user */
  firstName?: Maybe<Scalars['String']>;
  /** Common name of this user, format is firstName plus lastName */
  fullName?: Maybe<Scalars['String']>;
  /** last name of the user */
  lastName?: Maybe<Scalars['String']>;
  /** Direct manager of the user */
  manager?: Maybe<CorpUser>;
  /** Title of the user */
  title?: Maybe<Scalars['String']>;
};

/** Additional read only properties about a user */
export type CorpUserProperties = {
  __typename?: 'CorpUserProperties';
  /** Whether the user is active */
  active: Scalars['Boolean'];
  /** two uppercase letters country code */
  countryCode?: Maybe<Scalars['String']>;
  /** Custom properties of the ldap */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** department id the user belong to */
  departmentId?: Maybe<Scalars['Long']>;
  /** department name this user belong to */
  departmentName?: Maybe<Scalars['String']>;
  /** Display name of the user */
  displayName?: Maybe<Scalars['String']>;
  /** Email address of the user */
  email?: Maybe<Scalars['String']>;
  /** first name of the user */
  firstName?: Maybe<Scalars['String']>;
  /** Common name of this user, format is firstName plus lastName */
  fullName?: Maybe<Scalars['String']>;
  /** last name of the user */
  lastName?: Maybe<Scalars['String']>;
  /** Direct manager of the user */
  manager?: Maybe<CorpUser>;
  /** Title of the user */
  title?: Maybe<Scalars['String']>;
};

/** Settings that a user can customize through the datahub ui */
export type CorpUserSettings = {
  __typename?: 'CorpUserSettings';
  /** Settings that control look and feel of the DataHub UI for the user */
  appearance?: Maybe<CorpUserAppearanceSettings>;
  /** Settings related to the home page for a user */
  homePage?: Maybe<CorpUserHomePageSettings>;
  /** Settings related to the DataHub Views feature */
  views?: Maybe<CorpUserViewsSettings>;
};

/** The state of a CorpUser */
export enum CorpUserStatus {
  /** A User that has been provisioned and logged in */
  Active = 'ACTIVE',
  /** A user that has been suspended */
  Suspended = 'SUSPENDED'
}

/** Arguments provided to update a CorpUser Entity */
export type CorpUserUpdateInput = {
  /** About me section of the user */
  aboutMe?: Maybe<Scalars['String']>;
  /** Display name to show on DataHub */
  displayName?: Maybe<Scalars['String']>;
  /** Email address for the user */
  email?: Maybe<Scalars['String']>;
  /** The user's persona urn" */
  personaUrn?: Maybe<Scalars['String']>;
  /** Phone number for the user */
  phone?: Maybe<Scalars['String']>;
  /** A URL which points to a picture which user wants to set as a profile photo */
  pictureLink?: Maybe<Scalars['String']>;
  /** The platforms that the user frequently works with */
  platformUrns?: Maybe<Array<Scalars['String']>>;
  /** Skills that the user possesses */
  skills?: Maybe<Array<Scalars['String']>>;
  /** The slack handle of the user */
  slack?: Maybe<Scalars['String']>;
  /** Teams that the user belongs to */
  teams?: Maybe<Array<Scalars['String']>>;
  /** Title to show on DataHub */
  title?: Maybe<Scalars['String']>;
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
  OrgCostType = 'ORG_COST_TYPE'
}

export type CostValue = {
  __typename?: 'CostValue';
  /** Organizational Cost Code */
  costCode?: Maybe<Scalars['String']>;
  /** Organizational Cost ID */
  costId?: Maybe<Scalars['Float']>;
};

export type CreateAccessTokenInput = {
  /** The actor associated with the Access Token. */
  actorUrn: Scalars['String'];
  /** Description of the token if defined. */
  description?: Maybe<Scalars['String']>;
  /** The duration for which the Access Token is valid. */
  duration: AccessTokenDuration;
  /** The name of the token to be generated. */
  name: Scalars['String'];
  /** The type of the Access Token. */
  type: AccessTokenType;
};

/** Input required for creating a Application. */
export type CreateApplicationInput = {
  /** An Optional Domain */
  domainUrn?: Maybe<Scalars['String']>;
  /** An optional id for the new application */
  id?: Maybe<Scalars['String']>;
  /** Properties about the Application */
  properties: CreateApplicationPropertiesInput;
};

/** Input properties required for creating a Application */
export type CreateApplicationPropertiesInput = {
  /** An optional description for the Application */
  description?: Maybe<Scalars['String']>;
  /** A display name for the Application */
  name: Scalars['String'];
};

/** Input required for creating a BusinessAttribute. */
export type CreateBusinessAttributeInput = {
  /** description of business attribute */
  description?: Maybe<Scalars['String']>;
  /** Optional! A custom id to use as the primary key identifier. If not provided, a random UUID will be generated as the id. */
  id?: Maybe<Scalars['String']>;
  /** name of the business attribute */
  name: Scalars['String'];
  /** Platform independent field type of the field */
  type?: Maybe<SchemaFieldDataType>;
};

/** Input for creating a DataHub file */
export type CreateDataHubFileInput = {
  /** SHA-256 hash of file contents */
  contentHash?: Maybe<Scalars['String']>;
  /** Unique id for the file (will be used to create the URN) */
  id: Scalars['String'];
  /** MIME type of the file (e.g., image/png, application/pdf) */
  mimeType: Scalars['String'];
  /** The original filename as uploaded by the user */
  originalFileName: Scalars['String'];
  /** Optional URN of the entity this file is associated with */
  referencedByAsset?: Maybe<Scalars['String']>;
  /** The scenario/context in which this file was uploaded */
  scenario: UploadDownloadScenario;
  /** The dataset schema field urn this file is referenced by */
  schemaField?: Maybe<Scalars['String']>;
  /** Size of the file in bytes */
  sizeInBytes: Scalars['Long'];
  /** The key for where this file is stored inside of the given bucket */
  storageKey: Scalars['String'];
};

/** Response from creating a DataHub file */
export type CreateDataHubFileResponse = {
  __typename?: 'CreateDataHubFileResponse';
  /** The created DataHub file entity */
  file: DataHubFile;
};

/** Input required for creating a DataProduct. */
export type CreateDataProductInput = {
  /** The primary key of the Domain */
  domainUrn: Scalars['String'];
  /** An optional id for the new data product */
  id?: Maybe<Scalars['String']>;
  /** Properties about the Query */
  properties: CreateDataProductPropertiesInput;
};

/** Input properties required for creating a DataProduct */
export type CreateDataProductPropertiesInput = {
  /** An optional description for the DataProduct */
  description?: Maybe<Scalars['String']>;
  /** A display name for the DataProduct */
  name: Scalars['String'];
};

/** Input required to create a new Document */
export type CreateDocumentInput = {
  /** Content of the Document */
  contents: DocumentContentInput;
  /**
   * Optional! A custom id to use as the primary key identifier for the document.
   * If not provided, a random UUID will be generated as the id.
   */
  id?: Maybe<Scalars['String']>;
  /** Optional owners for the document. If not provided, the creator is automatically added as an owner. */
  owners?: Maybe<Array<OwnerInput>>;
  /** Optional URN of the parent document */
  parentDocument?: Maybe<Scalars['String']>;
  /** Optional URNs of related assets */
  relatedAssets?: Maybe<Array<Scalars['String']>>;
  /** Optional URNs of related documents */
  relatedDocuments?: Maybe<Array<Scalars['String']>>;
  /**
   * Optional settings for the document. If not provided, defaults will be used.
   * Documents created from the UI sidebar will always have showInGlobalContext=true.
   * API/programmatic clients can set showInGlobalContext=false to create private context documents.
   */
  settings?: Maybe<DocumentSettingsInput>;
  /** Optional initial state of the document. Defaults to UNPUBLISHED if not provided. */
  state?: Maybe<DocumentState>;
  /**
   * Optional sub-type of the Document (e.g., "FAQ", "Tutorial", "Reference").
   * If not provided, the document will have no type set.
   */
  subType?: Maybe<Scalars['String']>;
  /** Optional title for the document */
  title?: Maybe<Scalars['String']>;
};

/** Input required to create a new Domain. */
export type CreateDomainInput = {
  /** Optional description for the Domain */
  description?: Maybe<Scalars['String']>;
  /** Optional! A custom id to use as the primary key identifier for the domain. If not provided, a random UUID will be generated as the id. */
  id?: Maybe<Scalars['String']>;
  /** Display name for the Domain */
  name: Scalars['String'];
  /**
   * Optional - Add owners to the domain.
   * If not provided, we'll automatically assign the current actor as the owner.
   */
  owners?: Maybe<Array<OwnerInput>>;
  /** Optional parent domain urn for the domain */
  parentDomain?: Maybe<Scalars['String']>;
};

/** Input for batch assigning a form to different entities */
export type CreateDynamicFormAssignmentInput = {
  /** The urn of the form being assigned to entities that match some criteria */
  formUrn: Scalars['String'];
  /**
   * A list of disjunctive criterion for the filter. (or operation to combine filters).
   * Entities that match this filter will have this form applied to them.
   * Currently, we only support a set of fields to filter on and they are:
   * (1) platform (2) subType (3) container (4) _entityType (5) domain
   */
  orFilters: Array<AndFilterInput>;
};

/** Input for batch removing a form from different entities */
export type CreateFormInput = {
  /** Information on how this form should be assigned to users/groups */
  actors?: Maybe<FormActorAssignmentInput>;
  /** The optional description of the form being created */
  description?: Maybe<Scalars['String']>;
  /** Advanced: Optionally provide an ID to create a form urn from */
  id?: Maybe<Scalars['String']>;
  /** The name of the form being created */
  name: Scalars['String'];
  /** The type of this form, whether it's verification or completion. Default is completion. */
  prompts?: Maybe<Array<CreatePromptInput>>;
  /** The type of this form, whether it's verification or completion. Default is completion. */
  type?: Maybe<FormType>;
};

/** Input required to create a new Glossary Entity - a Node or a Term. */
export type CreateGlossaryEntityInput = {
  /** Description for the Node or Term */
  description?: Maybe<Scalars['String']>;
  /** Optional! A custom id to use as the primary key identifier for the domain. If not provided, a random UUID will be generated as the id. */
  id?: Maybe<Scalars['String']>;
  /** Display name for the Node or Term */
  name: Scalars['String'];
  /** Optional parent node urn for the Glossary Node or Term */
  parentNode?: Maybe<Scalars['String']>;
};

/** Input for creating a new group */
export type CreateGroupInput = {
  /** The description of the group */
  description?: Maybe<Scalars['String']>;
  /** Optional! A custom id to use as the primary key identifier for the group. If not provided, a random UUID will be generated as the id. */
  id?: Maybe<Scalars['String']>;
  /** The display name of the group */
  name: Scalars['String'];
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

export type CreateOwnershipTypeInput = {
  /** The description of the Custom Ownership Type */
  description: Scalars['String'];
  /** The name of the Custom Ownership Type */
  name: Scalars['String'];
};

/** Input provided when creating a Post */
export type CreatePostInput = {
  /** The content of the post */
  content: UpdatePostContentInput;
  /** The type of post */
  postType: PostType;
  /** Optional target URN for the post */
  resourceUrn?: Maybe<Scalars['String']>;
  /** Optional target subresource for the post */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to attach the Tag to */
  subResourceType?: Maybe<SubResourceType>;
};

/** Input for creating form prompts */
export type CreatePromptInput = {
  /** The optional description of the prompt */
  description?: Maybe<Scalars['String']>;
  /** Advanced: Optionally provide an ID to this prompt. All prompt IDs must be globally unique. */
  id?: Maybe<Scalars['String']>;
  /** Whether this prompt will be required or not. Default is false. */
  required?: Maybe<Scalars['Boolean']>;
  /** The params required if this prompt type is STRUCTURED_PROPERTY or FIELDS_STRUCTURED_PROPERTY */
  structuredPropertyParams?: Maybe<StructuredPropertyParamsInput>;
  /** The title of the prompt */
  title: Scalars['String'];
  /** The type of the prompt. */
  type: FormPromptType;
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
  /** An optional description for the Query */
  description?: Maybe<Scalars['String']>;
  /** An optional display name for the Query */
  name?: Maybe<Scalars['String']>;
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
  /** An optional description for the secret */
  description?: Maybe<Scalars['String']>;
  /** The name of the secret for reference in ingestion recipes */
  name: Scalars['String'];
  /** The value of the secret, to be encrypted and stored */
  value: Scalars['String'];
};

/** Input arguments for creating a service account */
export type CreateServiceAccountInput = {
  /** An optional description for the service account. */
  description?: Maybe<Scalars['String']>;
  /** An optional display name for the service account. */
  displayName?: Maybe<Scalars['String']>;
};

/** Input for creating a new structured property entity */
export type CreateStructuredPropertyInput = {
  /** The optional input for specifying a list of allowed values */
  allowedValues?: Maybe<Array<AllowedValueInput>>;
  /**
   * The optional input for specifying if one or multiple values can be applied.
   * Default is one value (single cardinality)
   */
  cardinality?: Maybe<PropertyCardinality>;
  /** The optional description for this property */
  description?: Maybe<Scalars['String']>;
  /** The optional display name for this property */
  displayName?: Maybe<Scalars['String']>;
  /**
   * The list of entity types that this property can be applied to.
   * For example: ["urn:li:entityType:datahub.dataset"]
   */
  entityTypes: Array<Scalars['String']>;
  /** (Advanced) An optional unique ID to use when creating the urn of this entity */
  id?: Maybe<Scalars['String']>;
  /** Whether the property will be mutable once it is applied or not. Default is false. */
  immutable?: Maybe<Scalars['Boolean']>;
  /**
   * The unique fully qualified name of this structured property, dot delimited.
   * This will be required to match the ID of this structured property.
   */
  qualifiedName?: Maybe<Scalars['String']>;
  /** Settings for this structured property */
  settings?: Maybe<StructuredPropertySettingsInput>;
  /** The optional input for specifying specific entity types as values */
  typeQualifier?: Maybe<TypeQualifierInput>;
  /**
   * The urn of the value type that this structured property accepts.
   * For example: urn:li:dataType:datahub.string or urn:li:dataType:datahub.date
   */
  valueType: Scalars['String'];
};

/** Input required to create a new Tag */
export type CreateTagInput = {
  /** Optional description for the Tag */
  description?: Maybe<Scalars['String']>;
  /** Optional! A custom id to use as the primary key identifier for the Tag. If not provided, a random UUID will be generated as the id. */
  id?: Maybe<Scalars['String']>;
  /** Display name for the Tag */
  name: Scalars['String'];
};

/** Input for creating a test connection request */
export type CreateTestConnectionRequestInput = {
  /** A JSON-encoded recipe */
  recipe: Scalars['String'];
  /** Advanced: The version of the ingestion framework to use */
  version?: Maybe<Scalars['String']>;
};

export type CreateTestInput = {
  /** The category of the Test (user defined) */
  category: Scalars['String'];
  /** The test definition */
  definition: TestDefinitionInput;
  /** Description of the test */
  description?: Maybe<Scalars['String']>;
  /** Advanced: a custom id for the test. */
  id?: Maybe<Scalars['String']>;
  /** The name of the Test */
  name: Scalars['String'];
};

/** Input provided when creating a DataHub View */
export type CreateViewInput = {
  /** The view definition itself */
  definition: DataHubViewDefinitionInput;
  /** An optional description of the View */
  description?: Maybe<Scalars['String']>;
  /** The name of the View */
  name: Scalars['String'];
  /** The type of View */
  viewType: DataHubViewType;
};

/** A cron schedule */
export type CronSchedule = {
  __typename?: 'CronSchedule';
  /** A cron-formatted execution interval, as a cron string, e.g. 1 * * * * */
  cron: Scalars['String'];
  /** Timezone in which the cron interval applies, e.g. America/Los_Angeles */
  timezone: Scalars['String'];
};

/** Information about a custom assertion */
export type CustomAssertionInfo = {
  __typename?: 'CustomAssertionInfo';
  /** The entity targeted by this custom assertion. */
  entityUrn: Scalars['String'];
  /** The field serving as input to the assertion, if any. */
  field?: Maybe<SchemaFieldRef>;
  /** Logic comprising a raw, unstructured assertion. */
  logic?: Maybe<Scalars['String']>;
  /** The type of custom assertion. */
  type: Scalars['String'];
};

/** An entry in a custom properties map represented as a tuple */
export type CustomPropertiesEntry = {
  __typename?: 'CustomPropertiesEntry';
  /** The urn of the entity this property came from for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** The key of the map entry */
  key: Scalars['String'];
  /** The value fo the map entry */
  value?: Maybe<Scalars['String']>;
};

/** A Dashboard Metadata Entity */
export type Dashboard = BrowsableEntity & Entity & EntityWithRelationships & {
  __typename?: 'Dashboard';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the dashboard. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /** An id unique within the dashboard tool */
  dashboardId: Scalars['String'];
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** The deprecation status of the dashboard */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the Dashboard */
  domain?: Maybe<DomainAssociation>;
  /**
   * Deprecated, use editableProperties instead
   * Additional read write properties about the Dashboard
   * @deprecated Field no longer supported
   */
  editableInfo?: Maybe<DashboardEditableProperties>;
  /** Additional read write properties about the dashboard */
  editableProperties?: Maybe<DashboardEditableProperties>;
  /** Embed information about the Dashboard */
  embed?: Maybe<Embed>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use tags field instead
   * The structured tags associated with the dashboard
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** The structured glossary terms associated with the dashboard */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Experimental! The resolved health statuses of the asset */
  health?: Maybe<Array<Health>>;
  /** Incidents associated with the Dashboard */
  incidents?: Maybe<EntityIncidentsResult>;
  /**
   * Deprecated, use properties field instead
   * Additional read only information about the dashboard
   * @deprecated Field no longer supported
   */
  info?: Maybe<DashboardInfo>;
  /** Input fields that power all the charts in the dashboard */
  inputFields?: Maybe<InputFields>;
  /** References to internal resources related to the dashboard */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Ownership metadata of the dashboard */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of containers for this entity */
  parentContainers?: Maybe<ParentContainersResult>;
  /** Standardized platform urn where the dashboard is defined */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties about the dashboard */
  properties?: Maybe<DashboardProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Experimental - Summary operational & usage statistics about a Dashboard */
  statsSummary?: Maybe<DashboardStatsSummary>;
  /** Status metadata of the dashboard */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Sub Types of the dashboard */
  subTypes?: Maybe<SubTypes>;
  /** The tags associated with the dashboard */
  tags?: Maybe<GlobalTags>;
  /**
   * The dashboard tool name
   * Note that this will soon be deprecated in favor of a standardized notion of Data Platform
   */
  tool: Scalars['String'];
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Dashboard */
  urn: Scalars['String'];
  /** Experimental (Subject to breaking change) -- Statistics about how this Dashboard is used */
  usageStats?: Maybe<DashboardUsageQueryResult>;
};


/** A Dashboard Metadata Entity */
export type DashboardAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A Dashboard Metadata Entity */
export type DashboardIncidentsArgs = {
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  count?: Maybe<Scalars['Int']>;
  priority?: Maybe<IncidentPriority>;
  stage?: Maybe<IncidentStage>;
  start?: Maybe<Scalars['Int']>;
  state?: Maybe<IncidentState>;
};


/** A Dashboard Metadata Entity */
export type DashboardLineageArgs = {
  input: LineageInput;
};


/** A Dashboard Metadata Entity */
export type DashboardRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** A Dashboard Metadata Entity */
export type DashboardRelationshipsArgs = {
  input: RelationshipsInput;
};


/** A Dashboard Metadata Entity */
export type DashboardUsageStatsArgs = {
  endTimeMillis?: Maybe<Scalars['Long']>;
  limit?: Maybe<Scalars['Int']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
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
  /**
   * Access level for the dashboard
   * Note that this will soon be deprecated for low usage
   */
  access?: Maybe<AccessLevel>;
  /**
   * Deprecated, use relationship Contains instead
   * Charts that comprise the dashboard
   * @deprecated Field no longer supported
   */
  charts: Array<Chart>;
  /** An AuditStamp corresponding to the creation of this dashboard */
  created: AuditStamp;
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** An optional AuditStamp corresponding to the deletion of this dashboard */
  deleted?: Maybe<AuditStamp>;
  /** Description of the dashboard */
  description?: Maybe<Scalars['String']>;
  /** Native platform URL of the dashboard */
  externalUrl?: Maybe<Scalars['String']>;
  /** An AuditStamp corresponding to the modification of this dashboard */
  lastModified: AuditStamp;
  /** The time when this dashboard last refreshed */
  lastRefreshed?: Maybe<Scalars['Long']>;
  /** Display of the dashboard */
  name: Scalars['String'];
};

/** Additional read only properties about a Dashboard */
export type DashboardProperties = {
  __typename?: 'DashboardProperties';
  /**
   * Access level for the dashboard
   * Note that this will soon be deprecated for low usage
   */
  access?: Maybe<AccessLevel>;
  /** An AuditStamp corresponding to the creation of this dashboard */
  created: AuditStamp;
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** An optional AuditStamp corresponding to the deletion of this dashboard */
  deleted?: Maybe<AuditStamp>;
  /** Description of the dashboard */
  description?: Maybe<Scalars['String']>;
  /** Native platform URL of the dashboard */
  externalUrl?: Maybe<Scalars['String']>;
  /** An AuditStamp corresponding to the modification of this dashboard */
  lastModified: AuditStamp;
  /** The time when this dashboard last refreshed */
  lastRefreshed?: Maybe<Scalars['Long']>;
  /** Display of the dashboard */
  name: Scalars['String'];
};

/** Experimental - subject to change. A summary of usage metrics about a Dashboard. */
export type DashboardStatsSummary = {
  __typename?: 'DashboardStatsSummary';
  /** The top users in the past 30 days */
  topUsersLast30Days?: Maybe<Array<CorpUser>>;
  /** The unique user count in the past 30 days */
  uniqueUserCountLast30Days?: Maybe<Scalars['Int']>;
  /** The total view count for the dashboard */
  viewCount?: Maybe<Scalars['Int']>;
  /** The view count in the last 30 days */
  viewCountLast30Days?: Maybe<Scalars['Int']>;
};

/** Arguments provided to update a Dashboard Entity */
export type DashboardUpdateInput = {
  /** Update to editable properties */
  editableProperties?: Maybe<DashboardEditablePropertiesUpdate>;
  /**
   * Deprecated, use tags field instead
   * Update to global tags
   */
  globalTags?: Maybe<GlobalTagsUpdate>;
  /** Update to ownership */
  ownership?: Maybe<OwnershipUpdate>;
  /** Update to tags */
  tags?: Maybe<GlobalTagsUpdate>;
};

/** An aggregation of Dashboard usage statistics */
export type DashboardUsageAggregation = {
  __typename?: 'DashboardUsageAggregation';
  /** The time window start time */
  bucket?: Maybe<Scalars['Long']>;
  /** The time window span */
  duration?: Maybe<WindowDuration>;
  /** The rolled up usage metrics */
  metrics?: Maybe<DashboardUsageAggregationMetrics>;
  /** The resource urn associated with the usage information, eg a Dashboard urn */
  resource?: Maybe<Scalars['String']>;
};

/** Rolled up metrics about Dashboard usage over time */
export type DashboardUsageAggregationMetrics = {
  __typename?: 'DashboardUsageAggregationMetrics';
  /** The total number of dashboard executions within the time range */
  executionsCount?: Maybe<Scalars['Int']>;
  /** The unique number of dashboard users within the time range */
  uniqueUserCount?: Maybe<Scalars['Int']>;
  /** The total number of dashboard views within the time range */
  viewsCount?: Maybe<Scalars['Int']>;
};

/** A set of absolute dashboard usage metrics */
export type DashboardUsageMetrics = TimeSeriesAspect & {
  __typename?: 'DashboardUsageMetrics';
  /** The total number of dashboard execution */
  executionsCount?: Maybe<Scalars['Int']>;
  /**
   * The total number of times dashboard has been favorited
   * FIXME: Qualifies as Popularity Metric rather than Usage Metric?
   */
  favoritesCount?: Maybe<Scalars['Int']>;
  /** The time when this dashboard was last viewed */
  lastViewed?: Maybe<Scalars['Long']>;
  /** The time at which the metrics were reported */
  timestampMillis: Scalars['Long'];
  /** The total number of dashboard views */
  viewsCount?: Maybe<Scalars['Int']>;
};

/** The result of a dashboard usage query */
export type DashboardUsageQueryResult = {
  __typename?: 'DashboardUsageQueryResult';
  /** A set of rolled up aggregations about the dashboard usage */
  aggregations?: Maybe<DashboardUsageQueryResultAggregations>;
  /** A set of relevant time windows for use in displaying usage statistics */
  buckets?: Maybe<Array<Maybe<DashboardUsageAggregation>>>;
  /** A set of absolute dashboard usage metrics */
  metrics?: Maybe<Array<DashboardUsageMetrics>>;
};

/** A set of rolled up aggregations about the Dashboard usage */
export type DashboardUsageQueryResultAggregations = {
  __typename?: 'DashboardUsageQueryResultAggregations';
  /** The total number of dashboard executions within the queried time range */
  executionsCount?: Maybe<Scalars['Int']>;
  /** The count of unique Dashboard users within the queried time range */
  uniqueUserCount?: Maybe<Scalars['Int']>;
  /** The specific per user usage counts within the queried time range */
  users?: Maybe<Array<Maybe<DashboardUserUsageCounts>>>;
  /** The total number of dashboard views within the queried time range */
  viewsCount?: Maybe<Scalars['Int']>;
};

/** Information about individual user usage of a Dashboard */
export type DashboardUserUsageCounts = {
  __typename?: 'DashboardUserUsageCounts';
  /** number of dashboard executions by the user */
  executionsCount?: Maybe<Scalars['Int']>;
  /**
   * Normalized numeric metric representing user's dashboard usage
   * Higher value represents more usage
   */
  usageCount?: Maybe<Scalars['Int']>;
  /** The user of the Dashboard */
  user?: Maybe<CorpUser>;
  /** number of times dashboard has been viewed by the user */
  viewsCount?: Maybe<Scalars['Int']>;
};

/**
 * A Data Contract Entity. A Data Contract is a verifiable group of assertions regarding various aspects of the data: its freshness (sla),
 * schema, and data quality or validity. This group of assertions represents a data owner's commitment to producing data that confirms to the agreed
 * upon contract. Each dataset can have a single contract. The contract can be in a "passing" or "violating" state, depending
 * on whether the assertions that compose the contract are passing or failing.
 * Note that the data contract entity is currently in early preview (beta).
 */
export type DataContract = Entity & {
  __typename?: 'DataContract';
  /** Properties describing the data contract */
  properties?: Maybe<DataContractProperties>;
  /** List of relationships between the source Entity and some destination entities with a given types */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The status of the data contract */
  status?: Maybe<DataContractStatus>;
  /** Structured properties about this Data Contract */
  structuredProperties?: Maybe<StructuredProperties>;
  /** The standard entity type */
  type: EntityType;
  /** A primary key of the data contract */
  urn: Scalars['String'];
};


/**
 * A Data Contract Entity. A Data Contract is a verifiable group of assertions regarding various aspects of the data: its freshness (sla),
 * schema, and data quality or validity. This group of assertions represents a data owner's commitment to producing data that confirms to the agreed
 * upon contract. Each dataset can have a single contract. The contract can be in a "passing" or "violating" state, depending
 * on whether the assertions that compose the contract are passing or failing.
 * Note that the data contract entity is currently in early preview (beta).
 */
export type DataContractRelationshipsArgs = {
  input: RelationshipsInput;
};

export type DataContractProperties = {
  __typename?: 'DataContractProperties';
  /** A set of data quality related contracts, e.g. table and column-level contract constraints. */
  dataQuality?: Maybe<Array<DataQualityContract>>;
  /** The urn of the related entity, e.g. the Dataset today. In the future, we may support additional contract entities. */
  entityUrn: Scalars['String'];
  /**
   * The Freshness (SLA) portion of the contract.
   * As of today, it is expected that there will not be more than 1 Freshness contract. If there are, only the first will be displayed.
   */
  freshness?: Maybe<Array<FreshnessContract>>;
  /**
   * The schema / structural portion of the contract.
   * As of today, it is expected that there will not be more than 1 Schema contract. If there are, only the first will be displayed.
   */
  schema?: Maybe<Array<SchemaContract>>;
};

/** The state of the data contract */
export enum DataContractState {
  /** The data contract is active. */
  Active = 'ACTIVE',
  /** The data contract is pending. Note that this symbol is currently experimental. */
  Pending = 'PENDING'
}

export type DataContractStatus = {
  __typename?: 'DataContractStatus';
  /** The state of the data contract */
  state: DataContractState;
};

/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlow = BrowsableEntity & Entity & EntityWithRelationships & HasExecutionRuns & {
  __typename?: 'DataFlow';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the data flow. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** Cluster of the flow */
  cluster: Scalars['String'];
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /**
   * Deprecated, use relationship IsPartOf instead
   * Data Jobs
   * @deprecated Field no longer supported
   */
  dataJobs?: Maybe<DataFlowDataJobsRelationships>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** The deprecation status of the Data Flow */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the DataFlow */
  domain?: Maybe<DomainAssociation>;
  /** Additional read write properties about a Data Flow */
  editableProperties?: Maybe<DataFlowEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** Id of the flow */
  flowId: Scalars['String'];
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use tags field instead
   * The structured tags associated with the dataflow
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** The structured glossary terms associated with the dashboard */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Experimental! The resolved health statuses of the asset */
  health?: Maybe<Array<Health>>;
  /** Incidents associated with the DataFlow */
  incidents?: Maybe<EntityIncidentsResult>;
  /**
   * Deprecated, use properties field instead
   * Additional read only information about a Data flow
   * @deprecated Field no longer supported
   */
  info?: Maybe<DataFlowInfo>;
  /** References to internal resources related to the dashboard */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Workflow orchestrator ei Azkaban, Airflow */
  orchestrator: Scalars['String'];
  /** Ownership metadata of the flow */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of containers for this entity */
  parentContainers?: Maybe<ParentContainersResult>;
  /** Standardized platform urn where the datflow is defined */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties about a Data flow */
  properties?: Maybe<DataFlowProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  runs?: Maybe<DataProcessInstanceResult>;
  /** Status metadata of the dataflow */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** The tags associated with the dataflow */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of a Data Flow */
  urn: Scalars['String'];
};


/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlowAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlowIncidentsArgs = {
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  count?: Maybe<Scalars['Int']>;
  priority?: Maybe<IncidentPriority>;
  stage?: Maybe<IncidentStage>;
  start?: Maybe<Scalars['Int']>;
  state?: Maybe<IncidentState>;
};


/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlowLineageArgs = {
  input: LineageInput;
};


/**
 * A Data Flow Metadata Entity, representing an set of pipelined Data Job or Tasks required
 * to produce an output Dataset Also known as a Data Pipeline
 */
export type DataFlowRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
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
export type DataFlowRunsArgs = {
  count?: Maybe<Scalars['Int']>;
  start?: Maybe<Scalars['Int']>;
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
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Description of the flow */
  description?: Maybe<Scalars['String']>;
  /** External URL associated with the DataFlow */
  externalUrl?: Maybe<Scalars['String']>;
  /** Display name of the flow */
  name: Scalars['String'];
  /** Optional project or namespace associated with the flow */
  project?: Maybe<Scalars['String']>;
};

/** Additional read only properties about a Data Flow aka Pipeline */
export type DataFlowProperties = {
  __typename?: 'DataFlowProperties';
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Description of the flow */
  description?: Maybe<Scalars['String']>;
  /** External URL associated with the DataFlow */
  externalUrl?: Maybe<Scalars['String']>;
  /** Display name of the flow */
  name: Scalars['String'];
  /** Optional project or namespace associated with the flow */
  project?: Maybe<Scalars['String']>;
};

/** Arguments provided to update a Data Flow aka Pipeline Entity */
export type DataFlowUpdateInput = {
  /** Update to editable properties */
  editableProperties?: Maybe<DataFlowEditablePropertiesUpdate>;
  /**
   * Deprecated, use tags field instead
   * Update to global tags
   */
  globalTags?: Maybe<GlobalTagsUpdate>;
  /** Update to ownership */
  ownership?: Maybe<OwnershipUpdate>;
  /** Update to tags */
  tags?: Maybe<GlobalTagsUpdate>;
};

/** A connection between DataHub and an external Platform. */
export type DataHubConnection = Entity & {
  __typename?: 'DataHubConnection';
  /** The connection details */
  details: DataHubConnectionDetails;
  /** The external Data Platform associated with the connection */
  platform: DataPlatform;
  /** Not implemented! */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type field */
  type: EntityType;
  /** The urn of the connection */
  urn: Scalars['String'];
};


/** A connection between DataHub and an external Platform. */
export type DataHubConnectionRelationshipsArgs = {
  input: RelationshipsInput;
};

/** The details of the Connection */
export type DataHubConnectionDetails = {
  __typename?: 'DataHubConnectionDetails';
  /** A JSON-encoded connection. Present when type is JSON. */
  json?: Maybe<DataHubJsonConnection>;
  /** The name for this DataHub connection */
  name?: Maybe<Scalars['String']>;
  /** The type or format of connection */
  type: DataHubConnectionDetailsType;
};

/** The type of a DataHub connection */
export enum DataHubConnectionDetailsType {
  /** A json-encoded set of connection details. */
  Json = 'JSON'
}

/** A DataHub file entity representing a file stored in S3 */
export type DataHubFile = Entity & {
  __typename?: 'DataHubFile';
  /** The main information about a DataHub file */
  info: DataHubFileInfo;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the DataHub File */
  urn: Scalars['String'];
};


/** A DataHub file entity representing a file stored in S3 */
export type DataHubFileRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Information about a DataHub file */
export type DataHubFileInfo = {
  __typename?: 'DataHubFileInfo';
  /** Info about where a file is stored */
  bucketStorageLocation: BucketStorageLocation;
  /** Audit stamp for when and by whom this file was created */
  created: ResolvedAuditStamp;
  /** MIME type of the file (e.g., image/png, application/pdf) */
  mimeType: Scalars['String'];
  /** The original filename as uploaded by the user */
  originalFileName: Scalars['String'];
  /** Optional entity this file is associated with */
  referencedByAsset?: Maybe<Entity>;
  /** The scenario/context in which this file was uploaded */
  scenario: UploadDownloadScenario;
  /** The dataset schema field this file is referenced by */
  schemaField?: Maybe<SchemaFieldEntity>;
  /** Size of the file in bytes */
  sizeInBytes: Scalars['Long'];
};

/** The details of a JSON Connection */
export type DataHubJsonConnection = {
  __typename?: 'DataHubJsonConnection';
  /** The JSON blob containing the specific connection details. */
  blob: Scalars['String'];
};

/** The details of a JSON Connection */
export type DataHubJsonConnectionInput = {
  /** The JSON blob containing the specific connection details. */
  blob: Scalars['String'];
};

/** A Page Module used for rendering custom or default layouts in the UI */
export type DataHubPageModule = Entity & {
  __typename?: 'DataHubPageModule';
  /** Whether or not this module exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The main properties of a DataHub page module */
  properties: DataHubPageModuleProperties;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Page Module */
  urn: Scalars['String'];
};


/** A Page Module used for rendering custom or default layouts in the UI */
export type DataHubPageModuleRelationshipsArgs = {
  input: RelationshipsInput;
};

/** The specific parameters stored for a module */
export type DataHubPageModuleParams = {
  __typename?: 'DataHubPageModuleParams';
  /** The params required if the module is type ASSET_COLLECTION */
  assetCollectionParams?: Maybe<AssetCollectionModuleParams>;
  /** The params required if the module is type HIERARCHY_VIEW */
  hierarchyViewParams?: Maybe<HierarchyViewModuleParams>;
  /** The params required if the module is type LINK */
  linkParams?: Maybe<LinkModuleParams>;
  /** The params required if the module is type RICH_TEXT */
  richTextParams?: Maybe<RichTextModuleParams>;
};

/** The main properties of a DataHub page module */
export type DataHubPageModuleProperties = {
  __typename?: 'DataHubPageModuleProperties';
  /** Audit stamp for when and by whom this module was created */
  created: ResolvedAuditStamp;
  /** Audit stamp for when and by whom this module was last updated */
  lastModified: ResolvedAuditStamp;
  /** The display name of this module */
  name: Scalars['String'];
  /** The specific parameters stored for this module */
  params: DataHubPageModuleParams;
  /** Info about the surface area of the product that this module is deployed in */
  type: DataHubPageModuleType;
  /** Info about the visibility of this module */
  visibility: DataHubPageModuleVisibility;
};

/** Enum containing the types of page modules that there are */
export enum DataHubPageModuleType {
  /** Module displaying the assets of parent entities */
  Assets = 'ASSETS',
  /** A module with a collection of assets */
  AssetCollection = 'ASSET_COLLECTION',
  /** Module displaying the hierarchy of the children of a given entity. Glossary or Domains. */
  ChildHierarchy = 'CHILD_HIERARCHY',
  /** Module displaying the columns of a dataset */
  Columns = 'COLUMNS',
  /** Module displaying child data products of a given domain */
  DataProducts = 'DATA_PRODUCTS',
  /** Module displaying the top domains */
  Domains = 'DOMAINS',
  /** A module displaying a hierarchy to navigate */
  Hierarchy = 'HIERARCHY',
  /** Module displaying the lineage of an asset */
  Lineage = 'LINEAGE',
  /** Link type module */
  Link = 'LINK',
  /** Module displaying assets owned by a user */
  OwnedAssets = 'OWNED_ASSETS',
  /** Module displaying the platforms in the instance */
  Platforms = 'PLATFORMS',
  /** Module displaying the related terms of a given glossary term */
  RelatedTerms = 'RELATED_TERMS',
  /** Module containing rich text to be rendered */
  RichText = 'RICH_TEXT',
  /** Unknown module type - this can occur with corrupted data or rolling back to versions without new modules */
  Unknown = 'UNKNOWN'
}

/** Info about the visibility of this module */
export type DataHubPageModuleVisibility = {
  __typename?: 'DataHubPageModuleVisibility';
  /** The scope of this module and who can use/see it */
  scope?: Maybe<PageModuleScope>;
};

/** A Page Template used for rendering custom or default layouts in the UI */
export type DataHubPageTemplate = Entity & {
  __typename?: 'DataHubPageTemplate';
  /** The main properties of a DataHub page template */
  properties: DataHubPageTemplateProperties;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Page Template */
  urn: Scalars['String'];
};


/** A Page Template used for rendering custom or default layouts in the UI */
export type DataHubPageTemplateRelationshipsArgs = {
  input: RelationshipsInput;
};

/** The page template info for asset summaries */
export type DataHubPageTemplateAssetSummary = {
  __typename?: 'DataHubPageTemplateAssetSummary';
  /** The list of properties shown on an asset summary page header. */
  summaryElements?: Maybe<Array<SummaryElement>>;
};

/** The main properties of a DataHub page template */
export type DataHubPageTemplateProperties = {
  __typename?: 'DataHubPageTemplateProperties';
  /** The optional info for asset summaries. Should be populated if surfaceType is ASSET_SUMMARY */
  assetSummary?: Maybe<DataHubPageTemplateAssetSummary>;
  /** Audit stamp for when and by whom this template was created */
  created: ResolvedAuditStamp;
  /** Audit stamp for when and by whom this template was last updated */
  lastModified: ResolvedAuditStamp;
  /** The rows of modules contained in this template */
  rows: Array<DataHubPageTemplateRow>;
  /** Info about the surface area of the product that this template is deployed in */
  surface: DataHubPageTemplateSurface;
  /** Info about the visibility of this template */
  visibility: DataHubPageTemplateVisibility;
};

/** A row of modules contained in a template */
export type DataHubPageTemplateRow = {
  __typename?: 'DataHubPageTemplateRow';
  /** The modules that exist in this template row */
  modules: Array<DataHubPageModule>;
};

/** Info about the surface area of the product that this template is deployed in */
export type DataHubPageTemplateSurface = {
  __typename?: 'DataHubPageTemplateSurface';
  /** Where exactly is this template bing used */
  surfaceType?: Maybe<PageTemplateSurfaceType>;
};

/** Info about the visibility of this template */
export type DataHubPageTemplateVisibility = {
  __typename?: 'DataHubPageTemplateVisibility';
  /** The scope of this template and who can use/see it */
  scope?: Maybe<PageTemplateScope>;
};

/** A standardized type of a user */
export type DataHubPersona = {
  __typename?: 'DataHubPersona';
  /** The urn of the persona type */
  urn: Scalars['String'];
};

/** An DataHub Platform Access Policy -  Policies determine who can perform what actions against which resources on the platform */
export type DataHubPolicy = Entity & {
  __typename?: 'DataHubPolicy';
  /** The actors that the Policy grants privileges to */
  actors: ActorFilter;
  /** The description of the Policy */
  description?: Maybe<Scalars['String']>;
  /** Whether the Policy is editable, ie system policies, or not */
  editable: Scalars['Boolean'];
  /** The name of the Policy */
  name: Scalars['String'];
  /** The type of the Policy */
  policyType: PolicyType;
  /** The privileges that the Policy grants */
  privileges: Array<Scalars['String']>;
  /** Granular API for querying edges extending from the Role */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The resources that the Policy privileges apply to */
  resources?: Maybe<ResourceFilter>;
  /** The present state of the Policy */
  state: PolicyState;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the Policy */
  urn: Scalars['String'];
};


/** An DataHub Platform Access Policy -  Policies determine who can perform what actions against which resources on the platform */
export type DataHubPolicyRelationshipsArgs = {
  input: RelationshipsInput;
};

/** A DataHub Role is a high-level abstraction on top of Policies that dictates what actions users can take. */
export type DataHubRole = Entity & {
  __typename?: 'DataHubRole';
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The description of the Role */
  description: Scalars['String'];
  /** The name of the Role. */
  name: Scalars['String'];
  /** Granular API for querying edges extending from the Role */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the role */
  urn: Scalars['String'];
};


/** A DataHub Role is a high-level abstraction on top of Policies that dictates what actions users can take. */
export type DataHubRoleAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A DataHub Role is a high-level abstraction on top of Policies that dictates what actions users can take. */
export type DataHubRoleRelationshipsArgs = {
  input: RelationshipsInput;
};

/** An DataHub View - Filters that are applied across the application automatically. */
export type DataHubView = Entity & {
  __typename?: 'DataHubView';
  /** The definition of the View */
  definition: DataHubViewDefinition;
  /** The description of the View */
  description?: Maybe<Scalars['String']>;
  /** The name of the View */
  name: Scalars['String'];
  /** Granular API for querying edges extending from the View */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the View */
  urn: Scalars['String'];
  /** The type of the View */
  viewType: DataHubViewType;
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
  /** A set of filters combined using the operator. If left empty, then no filters will be applied. */
  filters: Array<FacetFilter>;
  /** The operator used to combine the filters. */
  operator: LogicalOperator;
};

/** Input required for creating a DataHub View Definition */
export type DataHubViewFilterInput = {
  /** A set of filters combined via an operator. If left empty, then no filters will be applied. */
  filters: Array<FacetFilterInput>;
  /** The operator used to combine the filters. */
  operator: LogicalOperator;
};

/** The type of a DataHub View */
export enum DataHubViewType {
  /** A global view, e.g. role view */
  Global = 'GLOBAL',
  /** A personal view - e.g. saved filters */
  Personal = 'PERSONAL'
}

/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJob = BrowsableEntity & Entity & EntityWithRelationships & HasExecutionRuns & {
  __typename?: 'DataJob';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the data job. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /**
   * Deprecated, use relationship IsPartOf instead
   * The associated data flow
   */
  dataFlow?: Maybe<DataFlow>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Data Transform Logic associated with the Data Job */
  dataTransformLogic?: Maybe<DataTransformLogic>;
  /** The deprecation status of the Data Flow */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the Data Job */
  domain?: Maybe<DomainAssociation>;
  /** Additional read write properties associated with the Data Job */
  editableProperties?: Maybe<DataJobEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use the tags field instead
   * The structured tags associated with the DataJob
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** The structured glossary terms associated with the dashboard */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Experimental! The resolved health statuses of the asset */
  health?: Maybe<Array<Health>>;
  /** Incidents associated with the DataJob */
  incidents?: Maybe<EntityIncidentsResult>;
  /**
   * Deprecated, use properties field instead
   * Additional read only information about a Data processing job
   * @deprecated Field no longer supported
   */
  info?: Maybe<DataJobInfo>;
  /** Information about the inputs and outputs of a Data processing job including column-level lineage. */
  inputOutput?: Maybe<DataJobInputOutput>;
  /** References to internal resources related to the dashboard */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Id of the job */
  jobId: Scalars['String'];
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Ownership metadata of the job */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of containers for this entity */
  parentContainers?: Maybe<ParentContainersResult>;
  /** Standardized platform urn where the data job is defined */
  platform?: Maybe<DataPlatform>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties associated with the Data Job */
  properties?: Maybe<DataJobProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  runs?: Maybe<DataProcessInstanceResult>;
  /** Status metadata of the DataJob */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** The tags associated with the DataJob */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Data Job */
  urn: Scalars['String'];
};


/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJobAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/**
 * A Data Job Metadata Entity, representing an individual unit of computation or Task
 * to produce an output Dataset Always part of a parent Data Flow aka Pipeline
 */
export type DataJobIncidentsArgs = {
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  count?: Maybe<Scalars['Int']>;
  priority?: Maybe<IncidentPriority>;
  stage?: Maybe<IncidentStage>;
  start?: Maybe<Scalars['Int']>;
  state?: Maybe<IncidentState>;
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
export type DataJobRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
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
export type DataJobRunsArgs = {
  count?: Maybe<Scalars['Int']>;
  start?: Maybe<Scalars['Int']>;
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
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Job description */
  description?: Maybe<Scalars['String']>;
  /** External URL associated with the DataJob */
  externalUrl?: Maybe<Scalars['String']>;
  /** Job display name */
  name: Scalars['String'];
};

/**
 * The lineage information for a DataJob
 * TODO Rename this to align with other Lineage models
 */
export type DataJobInputOutput = {
  __typename?: 'DataJobInputOutput';
  /**
   * Lineage information for the column-level. Includes a list of objects
   * detailing which columns are upstream and which are downstream of each other.
   * The upstream and downstream columns are from datasets.
   */
  fineGrainedLineages?: Maybe<Array<FineGrainedLineage>>;
  /**
   * Deprecated, use relationship DownstreamOf instead
   * Input datajobs that this data job depends on
   * @deprecated Field no longer supported
   */
  inputDatajobs?: Maybe<Array<DataJob>>;
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
};

/** Additional read only properties about a Data Job aka Task */
export type DataJobProperties = {
  __typename?: 'DataJobProperties';
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Job description */
  description?: Maybe<Scalars['String']>;
  /** External URL associated with the DataJob */
  externalUrl?: Maybe<Scalars['String']>;
  /** Job display name */
  name: Scalars['String'];
};

/** Arguments provided to update a Data Job aka Task Entity */
export type DataJobUpdateInput = {
  /** Update to editable properties */
  editableProperties?: Maybe<DataJobEditablePropertiesUpdate>;
  /**
   * Deprecated, use tags field instead
   * Update to global tags
   */
  globalTags?: Maybe<GlobalTagsUpdate>;
  /** Update to ownership */
  ownership?: Maybe<OwnershipUpdate>;
  /** Update to tags */
  tags?: Maybe<GlobalTagsUpdate>;
};

/**
 * A Data Platform represents a specific third party Data System or Tool Examples include
 * warehouses like Snowflake, orchestrators like Airflow, and dashboarding tools like Looker
 */
export type DataPlatform = Entity & {
  __typename?: 'DataPlatform';
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
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Name of the data platform */
  name: Scalars['String'];
  /** Additional read only properties associated with a data platform */
  properties?: Maybe<DataPlatformProperties>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** Urn of the data platform */
  urn: Scalars['String'];
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
  /** The delimiter in the dataset names on the data platform */
  datasetNameDelimiter: Scalars['String'];
  /** Display name associated with the platform */
  displayName?: Maybe<Scalars['String']>;
  /** A logo URL associated with the platform */
  logoUrl?: Maybe<Scalars['String']>;
  /** The platform category */
  type: PlatformType;
};

/** A Data Platform instance represents an instance of a 3rd party platform like Looker, Snowflake, etc. */
export type DataPlatformInstance = Entity & {
  __typename?: 'DataPlatformInstance';
  /** The deprecation status of the data platform instance */
  deprecation?: Maybe<Deprecation>;
  /** The platform instance id */
  instanceId: Scalars['String'];
  /** References to internal resources related to the data platform instance */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the data platform instance */
  ownership?: Maybe<Ownership>;
  /** Name of the data platform */
  platform: DataPlatform;
  /** Additional read only properties associated with a data platform instance */
  properties?: Maybe<DataPlatformInstanceProperties>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the container */
  status?: Maybe<Status>;
  /** Tags used for searching the data platform instance */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** Urn of the data platform */
  urn: Scalars['String'];
};


/** A Data Platform instance represents an instance of a 3rd party platform like Looker, Snowflake, etc. */
export type DataPlatformInstanceRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Additional read only properties about a DataPlatformInstance */
export type DataPlatformInstanceProperties = {
  __typename?: 'DataPlatformInstanceProperties';
  /** Custom properties of the data platform instance */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Read only technical description for the data platform instance */
  description?: Maybe<Scalars['String']>;
  /** External URL associated with the data platform instance */
  externalUrl?: Maybe<Scalars['String']>;
  /** The name of the data platform instance used in display */
  name?: Maybe<Scalars['String']>;
};

/** Additional read only properties about a Data Platform */
export type DataPlatformProperties = {
  __typename?: 'DataPlatformProperties';
  /** The delimiter in the dataset names on the data platform */
  datasetNameDelimiter: Scalars['String'];
  /** Display name associated with the platform */
  displayName?: Maybe<Scalars['String']>;
  /** A logo URL associated with the platform */
  logoUrl?: Maybe<Scalars['String']>;
  /** The platform category */
  type: PlatformType;
};

/**
 * A DataProcessInstance Metadata Entity, representing an individual run of
 * a task or datajob.
 */
export type DataProcessInstance = Entity & EntityWithRelationships & {
  __typename?: 'DataProcessInstance';
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /**
   * When the run was kicked off
   * @deprecated Use `properties.created`
   */
  created?: Maybe<AuditStamp>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The link to view the task run in the source system */
  externalUrl?: Maybe<Scalars['String']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Additional properties when subtype is Training Run */
  mlTrainingRunProperties?: Maybe<MlTrainingRunProperties>;
  /**
   * The name of the data process
   * @deprecated Use `properties.name`
   */
  name?: Maybe<Scalars['String']>;
  /** Recursively get the lineage of containers for this entity */
  parentContainers?: Maybe<ParentContainersResult>;
  /** The parent entity whose run instance it is */
  parentTemplate?: Maybe<Entity>;
  /** Standardized platform urn where the data process instance is defined */
  platform?: Maybe<DataPlatform>;
  /** Additional read only properties associated with the Data Process Instance */
  properties?: Maybe<DataProcessInstanceProperties>;
  /**
   * Edges extending from this entity.
   * In the UI, used for inputs, outputs and parentTemplate
   */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The history of state changes for the run */
  state?: Maybe<Array<Maybe<DataProcessRunEvent>>>;
  /** Status metadata of the data process instance */
  status?: Maybe<Status>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the DataProcessInstance */
  urn: Scalars['String'];
};


/**
 * A DataProcessInstance Metadata Entity, representing an individual run of
 * a task or datajob.
 */
export type DataProcessInstanceLineageArgs = {
  input: LineageInput;
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
export type DataProcessInstanceStateArgs = {
  endTimeMillis?: Maybe<Scalars['Long']>;
  limit?: Maybe<Scalars['Int']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
};

/** Properties describing a data process instance's execution metadata */
export type DataProcessInstanceProperties = {
  __typename?: 'DataProcessInstanceProperties';
  /** When this process instance was created */
  created: AuditStamp;
  /** Additional custom properties specific to this process instance */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** URL to view this process instance in the external system */
  externalUrl?: Maybe<Scalars['String']>;
  /** The display name of this process instance */
  name: Scalars['String'];
};

/** Data Process instances that match the provided query */
export type DataProcessInstanceResult = {
  __typename?: 'DataProcessInstanceResult';
  /** The number of entities to include in result set */
  count?: Maybe<Scalars['Int']>;
  /** The data process instances that produced or consumed the entity */
  runs?: Maybe<Array<Maybe<DataProcessInstance>>>;
  /** The offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** The total number of run events returned */
  total?: Maybe<Scalars['Int']>;
};

/** the result of a run, part of the run state */
export type DataProcessInstanceRunResult = {
  __typename?: 'DataProcessInstanceRunResult';
  /** The outcome of the run in the data platforms native language */
  nativeResultType?: Maybe<Scalars['String']>;
  /** The outcome of the run */
  resultType?: Maybe<DataProcessInstanceRunResultType>;
};

/** The result of the data process run */
export enum DataProcessInstanceRunResultType {
  /** The run finished in failure */
  Failure = 'FAILURE',
  /** The run was skipped */
  Skipped = 'SKIPPED',
  /** The run finished successfully */
  Success = 'SUCCESS',
  /** The run failed and is up for retry */
  UpForRetry = 'UP_FOR_RETRY'
}

/** A state change event in the data process instance lifecycle */
export type DataProcessRunEvent = TimeSeriesAspect & {
  __typename?: 'DataProcessRunEvent';
  /** The try number that this instance run is in */
  attempt?: Maybe<Scalars['Int']>;
  /** The duration of the run in milliseconds */
  durationMillis?: Maybe<Scalars['Long']>;
  /** The result of a run */
  result?: Maybe<DataProcessInstanceRunResult>;
  /** The status of the data process instance */
  status?: Maybe<DataProcessRunStatus>;
  /** The timestamp associated with the run event in milliseconds */
  timestampMillis: Scalars['Long'];
};

/** The status of the data process instance */
export enum DataProcessRunStatus {
  /** The data process instance has completed */
  Complete = 'COMPLETE',
  /** The data process instance has started but not completed */
  Started = 'STARTED'
}

/** A Data Product, or a logical grouping of Metadata Entities */
export type DataProduct = Entity & {
  __typename?: 'DataProduct';
  /**
   * Deprecated, use applications instead
   * The application associated with the data product
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the data product */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The Domain associated with the Data Product */
  domain?: Maybe<DomainAssociation>;
  /** Children entities inside of the DataProduct */
  entities?: Maybe<SearchResults>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the Data Product */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the Data Product */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the Data Product */
  ownership?: Maybe<Ownership>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Properties about a Data Product */
  properties?: Maybe<DataProductProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Settings associated with this asset */
  settings?: Maybe<AssetSettings>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags used for searching Data Product */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Data Product */
  urn: Scalars['String'];
};


/** A Data Product, or a logical grouping of Metadata Entities */
export type DataProductAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A Data Product, or a logical grouping of Metadata Entities */
export type DataProductEntitiesArgs = {
  input?: Maybe<SearchAcrossEntitiesInput>;
};


/** A Data Product, or a logical grouping of Metadata Entities */
export type DataProductRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** A Data Product, or a logical grouping of Metadata Entities */
export type DataProductRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Input required to fetch the entities inside of a Data Product. */
export type DataProductEntitiesInput = {
  /** The number of entities to include in result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Optional query filter for particular entities inside the Data Product */
  query?: Maybe<Scalars['String']>;
  /** The offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Properties about a domain */
export type DataProductProperties = {
  __typename?: 'DataProductProperties';
  /** A Resolved Audit Stamp corresponding to the creation of this resource */
  createdOn?: Maybe<ResolvedAuditStamp>;
  /** Custom properties of the Data Product */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Description of the Data Product */
  description?: Maybe<Scalars['String']>;
  /** External URL for the DataProduct (most likely GitHub repo where Data Products are managed as code) */
  externalUrl?: Maybe<Scalars['String']>;
  /** Display name of the Data Product */
  name: Scalars['String'];
  /** Number of children entities inside of the Data Product. This number includes soft deleted entities. */
  numAssets?: Maybe<Scalars['Int']>;
};

export type DataQualityContract = {
  __typename?: 'DataQualityContract';
  /** The assertion representing the schema contract. */
  assertion: Assertion;
};

/** Input required to create a data quality contract */
export type DataQualityContractInput = {
  /** The assertion monitoring this part of the data contract. Assertion must be of type Dataset, Volume, Field / Column, or Custom SQL. */
  assertionUrn: Scalars['String'];
};

/** Information about a transformation applied to data assets */
export type DataTransform = {
  __typename?: 'DataTransform';
  /** The transformation may be defined by a query statement */
  queryStatement?: Maybe<QueryStatement>;
};

/** Information about transformations applied to data assets */
export type DataTransformLogic = {
  __typename?: 'DataTransformLogic';
  /** List of transformations applied */
  transforms: Array<DataTransform>;
};

/** A data type registered in DataHub */
export type DataTypeEntity = Entity & {
  __typename?: 'DataTypeEntity';
  /** Info about this type including its name */
  info: DataTypeInfo;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Query */
  urn: Scalars['String'];
};


/** A data type registered in DataHub */
export type DataTypeEntityRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Properties about an individual data type */
export type DataTypeInfo = {
  __typename?: 'DataTypeInfo';
  /** The description of this type */
  description?: Maybe<Scalars['String']>;
  /** The display name of this type */
  displayName?: Maybe<Scalars['String']>;
  /** The fully qualified name of the type. This includes its namespace */
  qualifiedName: Scalars['String'];
  /** The standard data type */
  type: StdDataType;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type Dataset = BrowsableEntity & Entity & EntityWithRelationships & HasLogicalParent & SupportsVersions & {
  __typename?: 'Dataset';
  /** The Roles and the properties to access the dataset */
  access?: Maybe<Access>;
  /**
   * Deprecated, use applications instead
   * The application associated with the dataset
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the dataset */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** Assertions associated with the Dataset */
  assertions?: Maybe<EntityAssertionsResult>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the dataset. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /** An optional Data Contract defined for the Dataset. */
  contract?: Maybe<DataContract>;
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
  /**
   * Lineage information for the column-level. Includes a list of objects
   * detailing which columns are upstream and which are downstream of each other.
   * The upstream and downstream columns are from datasets.
   */
  fineGrainedLineages?: Maybe<Array<FineGrainedLineage>>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
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
  /** Incidents associated with the Dataset */
  incidents?: Maybe<EntityIncidentsResult>;
  /** References to internal resources related to the dataset */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** If this entity represents a physical asset, this is its logical parent, from which metadata can propagate. */
  logicalParent?: Maybe<Entity>;
  /**
   * Unique guid for dataset
   * No longer to be used as the Dataset display name. Use properties.name instead
   */
  name: Scalars['String'];
  /** Operational events for an entity. */
  operations?: Maybe<Array<Operation>>;
  /** Statistics about how this Dataset has been operated on */
  operationsStats?: Maybe<OperationsQueryResult>;
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
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
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
  /** Settings associated with this asset */
  settings?: Maybe<AssetSettings>;
  /** Metadata about the datasets siblings */
  siblings?: Maybe<SiblingProperties>;
  /** Executes a search on only the siblings of an entity */
  siblingsSearch?: Maybe<ScrollResults>;
  /** Experimental - Summary operational & usage statistics about a Dataset */
  statsSummary?: Maybe<DatasetStatsSummary>;
  /** Status of the Dataset */
  status?: Maybe<Status>;
  /** Structured properties about this Dataset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** Tags used for searching dataset */
  tags?: Maybe<GlobalTags>;
  /** The results of evaluating tests */
  testResults?: Maybe<TestResults>;
  /** Returns a set of capabilities regarding our timerseries indices */
  timeseriesCapabilities?: Maybe<TimeseriesCapabilitiesResult>;
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
   * timeZone accepts standard IANA time zone identifier ie. America/New_York
   */
  usageStats?: Maybe<UsageQueryResult>;
  versionProperties?: Maybe<VersionProperties>;
  /** View related properties. Only relevant if subtypes field contains view. */
  viewProperties?: Maybe<ViewProperties>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetAssertionsArgs = {
  count?: Maybe<Scalars['Int']>;
  includeSoftDeleted?: Maybe<Scalars['Boolean']>;
  start?: Maybe<Scalars['Int']>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetDatasetProfilesArgs = {
  endTimeMillis?: Maybe<Scalars['Long']>;
  filter?: Maybe<FilterInput>;
  limit?: Maybe<Scalars['Int']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetIncidentsArgs = {
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  count?: Maybe<Scalars['Int']>;
  priority?: Maybe<IncidentPriority>;
  stage?: Maybe<IncidentStage>;
  start?: Maybe<Scalars['Int']>;
  state?: Maybe<IncidentState>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetLineageArgs = {
  input: LineageInput;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetOperationsArgs = {
  endTimeMillis?: Maybe<Scalars['Long']>;
  filter?: Maybe<FilterInput>;
  limit?: Maybe<Scalars['Int']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetOperationsStatsArgs = {
  input?: Maybe<OperationsStatsInput>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetRelationshipsArgs = {
  input: RelationshipsInput;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetRunsArgs = {
  count?: Maybe<Scalars['Int']>;
  direction: RelationshipDirection;
  start?: Maybe<Scalars['Int']>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetSchemaMetadataArgs = {
  version?: Maybe<Scalars['Long']>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetSiblingsSearchArgs = {
  input: ScrollAcrossEntitiesInput;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type DatasetUsageStatsArgs = {
  range?: Maybe<TimeRange>;
  resource?: Maybe<Scalars['String']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
  timeZone?: Maybe<Scalars['String']>;
};

/** Detailed information about a Dataset Assertion */
export type DatasetAssertionInfo = {
  __typename?: 'DatasetAssertionInfo';
  /** Standardized assertion operator */
  aggregation?: Maybe<AssertionStdAggregation>;
  /** The urn of the dataset that the assertion is related to */
  datasetUrn: Scalars['String'];
  /** The fields serving as input to the assertion. Empty if there are none. */
  fields?: Maybe<Array<SchemaFieldRef>>;
  /** Logic comprising a raw, unstructured assertion. */
  logic?: Maybe<Scalars['String']>;
  /** Native parameters required for the assertion. */
  nativeParameters?: Maybe<Array<StringMapEntry>>;
  /** The native operator for the assertion. For Great Expectations, this will contain the original expectation name. */
  nativeType?: Maybe<Scalars['String']>;
  /** Standardized assertion operator */
  operator: AssertionStdOperator;
  /** Standard parameters required for the assertion. e.g. min_value, max_value, value, columns */
  parameters?: Maybe<AssertionStdParameters>;
  /** The scope of the Dataset assertion. */
  scope: DatasetAssertionScope;
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
  Unknown = 'UNKNOWN'
}

/**
 * Deprecated, use Deprecation instead
 * Information about Dataset deprecation status
 * Note that this model will soon be migrated to a more general purpose Entity status
 */
export type DatasetDeprecation = {
  __typename?: 'DatasetDeprecation';
  /** The user who will be credited for modifying this deprecation content */
  actor?: Maybe<Scalars['String']>;
  /** The time user plan to decommission this dataset */
  decommissionTime?: Maybe<Scalars['Long']>;
  /** Whether the dataset has been deprecated by owner */
  deprecated: Scalars['Boolean'];
  /** Additional information about the dataset deprecation plan */
  note: Scalars['String'];
};

/** An update for the deprecation information for a Metadata Entity */
export type DatasetDeprecationUpdate = {
  /** The time user plan to decommission this dataset */
  decommissionTime?: Maybe<Scalars['Long']>;
  /** Whether the dataset is deprecated */
  deprecated: Scalars['Boolean'];
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
  /** Editable name of the Dataset */
  name?: Maybe<Scalars['String']>;
};

/** Update to writable Dataset fields */
export type DatasetEditablePropertiesUpdate = {
  /** Writable description aka documentation for a Dataset */
  description: Scalars['String'];
  /** Editable name of the Dataset */
  name?: Maybe<Scalars['String']>;
};

/** An individual Dataset Field Profile */
export type DatasetFieldProfile = {
  __typename?: 'DatasetFieldProfile';
  /** Volume of each column value for a low-cardinality / categorical field */
  distinctValueFrequencies?: Maybe<Array<ValueFrequency>>;
  /** The standardized path of the field */
  fieldPath: Scalars['String'];
  /** The max value for the field */
  max?: Maybe<Scalars['String']>;
  /** The mean value for the field */
  mean?: Maybe<Scalars['String']>;
  /** The median value for the field */
  median?: Maybe<Scalars['String']>;
  /** The min value for the field */
  min?: Maybe<Scalars['String']>;
  /** The number of NULL row values across the Dataset */
  nullCount?: Maybe<Scalars['Long']>;
  /** The proportion of rows with NULL values across the Dataset */
  nullProportion?: Maybe<Scalars['Float']>;
  /**
   * Sorted list of quantile cutoffs for the field, in ascending order
   * Only for numerical columns
   */
  quantiles?: Maybe<Array<Quantile>>;
  /** A set of sample values for the field */
  sampleValues?: Maybe<Array<Scalars['String']>>;
  /** The standard deviation for the field */
  stdev?: Maybe<Scalars['String']>;
  /** The unique value count for the field across the Dataset */
  uniqueCount?: Maybe<Scalars['Long']>;
  /** The proportion of rows with unique values across the Dataset */
  uniqueProportion?: Maybe<Scalars['Float']>;
};

/** Describes a generic filter on a dataset */
export type DatasetFilter = {
  __typename?: 'DatasetFilter';
  /** The raw query if using a SQL FilterType */
  sql?: Maybe<Scalars['String']>;
  /** Type of partition */
  type: DatasetFilterType;
};

/** Input required to create or update a DatasetFilter */
export type DatasetFilterInput = {
  /** The raw query if using a SQL FilterType */
  sql?: Maybe<Scalars['String']>;
  /** Type of partition */
  type: DatasetFilterType;
};

/** Type of partition */
export enum DatasetFilterType {
  /** Use a SQL string to apply the filter */
  Sql = 'SQL'
}

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
  View = 'VIEW'
}

/** A Dataset Profile associated with a Dataset, containing profiling statistics about the Dataset */
export type DatasetProfile = TimeSeriesAspect & {
  __typename?: 'DatasetProfile';
  /** An optional column count of the Dataset */
  columnCount?: Maybe<Scalars['Long']>;
  /** An optional set of per field statistics obtained in the profile */
  fieldProfiles?: Maybe<Array<DatasetFieldProfile>>;
  /** Information about the partition that was profiled */
  partitionSpec?: Maybe<PartitionSpec>;
  /** An optional row count of the Dataset */
  rowCount?: Maybe<Scalars['Long']>;
  /** The storage size in bytes */
  sizeInBytes?: Maybe<Scalars['Long']>;
  /** The time at which the profile was reported */
  timestampMillis: Scalars['Long'];
};

/** Additional read only properties about a Dataset */
export type DatasetProperties = {
  __typename?: 'DatasetProperties';
  /** Created timestamp millis associated with the Dataset */
  created?: Maybe<Scalars['Long']>;
  /** Actor associated with the Dataset's created timestamp */
  createdActor?: Maybe<Scalars['String']>;
  /** Custom properties of the Dataset */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Read only technical description for dataset */
  description?: Maybe<Scalars['String']>;
  /** External URL associated with the Dataset */
  externalUrl?: Maybe<Scalars['String']>;
  /** Last Modified timestamp millis associated with the Dataset */
  lastModified: AuditStamp;
  /**
   * Actor associated with the Dataset's lastModified timestamp.
   * Deprecated - Use lastModified.actor instead.
   * @deprecated Field no longer supported
   */
  lastModifiedActor?: Maybe<Scalars['String']>;
  /** The name of the dataset used in display */
  name: Scalars['String'];
  /**
   * Environment in which the dataset belongs to or where it was generated
   * Note that this field will soon be deprecated in favor of a more standardized concept of Environment
   */
  origin: FabricType;
  /** Fully-qualified name of the Dataset */
  qualifiedName?: Maybe<Scalars['String']>;
};

/** Experimental - subject to change. A summary of usage metrics about a Dataset. */
export type DatasetStatsSummary = {
  __typename?: 'DatasetStatsSummary';
  /** The query count in the past 30 days */
  queryCountLast30Days?: Maybe<Scalars['Int']>;
  /** The top users in the past 30 days */
  topUsersLast30Days?: Maybe<Array<CorpUser>>;
  /** The unique user count in the past 30 days */
  uniqueUserCountLast30Days?: Maybe<Scalars['Int']>;
};

/** Arguments provided to update a Dataset Entity */
export type DatasetUpdateInput = {
  /** Update to deprecation status */
  deprecation?: Maybe<DatasetDeprecationUpdate>;
  /** Update to editable properties */
  editableProperties?: Maybe<DatasetEditablePropertiesUpdate>;
  /** Update to editable schema metadata of the dataset */
  editableSchemaMetadata?: Maybe<EditableSchemaMetadataUpdate>;
  /**
   * Deprecated, use tags field instead
   * Update to global tags
   */
  globalTags?: Maybe<GlobalTagsUpdate>;
  /** Update to institutional memory, ie documentation */
  institutionalMemory?: Maybe<InstitutionalMemoryUpdate>;
  /** Update to ownership */
  ownership?: Maybe<OwnershipUpdate>;
  /** Update to tags */
  tags?: Maybe<GlobalTagsUpdate>;
};

/** For consumption by UI only */
export enum DateInterval {
  Day = 'DAY',
  Hour = 'HOUR',
  Minute = 'MINUTE',
  Month = 'MONTH',
  Second = 'SECOND',
  Week = 'WEEK',
  Year = 'YEAR'
}

/** For consumption by UI only */
export type DateRange = {
  __typename?: 'DateRange';
  end: Scalars['String'];
  start: Scalars['String'];
};

/**
 * Experimental API result to debug Access for users.
 * Backward incompatible changes will be made without notice in the future.
 */
export type DebugAccessResult = {
  __typename?: 'DebugAccessResult';
  /** Union of `roles` + `rolesViaGroups` that the user has. */
  allRoles: Array<Scalars['String']>;
  /** Groups that the user belongs to. */
  groups: Array<Scalars['String']>;
  /**
   * List of groups that the user is assigned to AND where the group has a role.
   * This is a subset of the groups property.
   */
  groupsWithRoles: Array<Scalars['String']>;
  /** List of Policy that apply to this user directly or indirectly. */
  policies: Array<Scalars['String']>;
  /** List of privileges that this user has directly or indirectly. */
  privileges: Array<Scalars['String']>;
  /** Roles that the user has. */
  roles: Array<Scalars['String']>;
  /**
   * Final set of roles that are coming through groups.
   * If not role assigned to groups, then this would be empty.
   */
  rolesViaGroups: Array<Scalars['String']>;
};

/** Input for deleting a form */
export type DeleteFormInput = {
  /** The urn of the form that is being deleted */
  urn: Scalars['String'];
};

/** Input for deleting a DataHub page module */
export type DeletePageModuleInput = {
  /** The URN of the page module to delete */
  urn: Scalars['String'];
};

/** Input for deleting a DataHub page template */
export type DeletePageTemplateInput = {
  /** The URN of the page template to delete */
  urn: Scalars['String'];
};

/** Input for deleting a form */
export type DeleteStructuredPropertyInput = {
  /** The urn of the structured properties that is being deleted */
  urn: Scalars['String'];
};

/** Information about Metadata Entity deprecation status */
export type Deprecation = {
  __typename?: 'Deprecation';
  /** The user who will be credited for modifying this deprecation content */
  actor?: Maybe<Scalars['String']>;
  /** The hydrated user who will be credited for modifying this deprecation content */
  actorEntity?: Maybe<Entity>;
  /** The time user plan to decommission this entity */
  decommissionTime?: Maybe<Scalars['Long']>;
  /** Whether the entity has been deprecated by owner */
  deprecated: Scalars['Boolean'];
  /** Additional information about the entity deprecation plan */
  note?: Maybe<Scalars['String']>;
  /** The optional replacement entity */
  replacement?: Maybe<Entity>;
};

/** Incubating. Updates the description of a resource. Currently supports DatasetField descriptions only */
export type DescriptionUpdateInput = {
  /** The new description */
  description: Scalars['String'];
  /** The primary key of the resource to attach the description to, eg dataset urn */
  resourceUrn: Scalars['String'];
  /** A sub resource identifier, eg dataset field path */
  subResource?: Maybe<Scalars['String']>;
  /** An optional sub resource type */
  subResourceType?: Maybe<SubResourceType>;
};

/** Properties related to how the entity is displayed in the Datahub UI */
export type DisplayProperties = {
  __typename?: 'DisplayProperties';
  /** Color associated with the entity in Hex. For example #FFFFFF */
  colorHex?: Maybe<Scalars['String']>;
  /** The icon associated with the entity */
  icon?: Maybe<IconProperties>;
};

/** Update a particular entity's display properties */
export type DisplayPropertiesUpdateInput = {
  /** Color associated with the entity in Hex. For example #FFFFFF */
  colorHex?: Maybe<Scalars['String']>;
  /** The icon associated with the entity */
  icon?: Maybe<IconPropertiesInput>;
};

/** Global (platform-level) settings related to the doc propagation feature */
export type DocPropagationSettings = {
  __typename?: 'DocPropagationSettings';
  /** The default doc propagation setting for the platform. */
  docColumnPropagation?: Maybe<Scalars['Boolean']>;
};

/** A Document entity in DataHub */
export type Document = Entity & {
  __typename?: 'Document';
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /**
   * Change history for this document.
   * Returns a chronological list of changes made to the document.
   */
  changeHistory: Array<DocumentChange>;
  /** Data Platform Instance associated with the Document */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Documentation aspect containing editable description for this Document */
  documentation?: Maybe<Documentation>;
  /** The Domain associated with the Document */
  domain?: Maybe<DomainAssociation>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** Glossary terms associated with the Document */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Information about the Document */
  info?: Maybe<DocumentInfo>;
  /** References to internal resources related to the Document (links) */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the Document */
  ownership?: Maybe<Ownership>;
  /**
   * Recursively get the lineage of parent documents for this document.
   * Returns parents with direct parent first followed by the parent's parent, etc.
   */
  parentDocuments?: Maybe<ParentDocumentsResult>;
  /**
   * The platform that this Document originates in. For example, "Notion" for representing docs ingested from Notion.
   * For documents natively authored on DataHub, the platform will be "DataHub".
   */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Settings specific to the Document */
  settings?: Maybe<DocumentSettings>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** The sub-type of the Document (e.g., "FAQ", "Tutorial", "Reference", etc.) */
  subType?: Maybe<Scalars['String']>;
  /** Tags applied to the Document */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Document */
  urn: Scalars['String'];
};


/** A Document entity in DataHub */
export type DocumentAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A Document entity in DataHub */
export type DocumentChangeHistoryArgs = {
  endTimeMillis?: Maybe<Scalars['Long']>;
  limit?: Maybe<Scalars['Int']>;
  startTimeMillis?: Maybe<Scalars['Long']>;
};


/** A Document entity in DataHub */
export type DocumentRelationshipsArgs = {
  input: RelationshipsInput;
};

/**
 * A change made to a document.
 * Represents a single modification with timestamp, actor, and description.
 */
export type DocumentChange = {
  __typename?: 'DocumentChange';
  /** User who made the change (optional, may not be available for all changes) */
  actor?: Maybe<CorpUser>;
  /** Type of change that occurred */
  changeType: DocumentChangeType;
  /** Human-readable description of what changed */
  description: Scalars['String'];
  /**
   * Additional context about the change (optional).
   * For example, if a document was moved, this might contain the old and new parent URNs.
   */
  details?: Maybe<Array<StringMapEntry>>;
  /** When the change occurred (milliseconds since epoch) */
  timestamp: Scalars['Long'];
};

/** Types of changes that can occur to a document */
export enum DocumentChangeType {
  /** Document was created */
  Created = 'CREATED',
  /** Document was deleted */
  Deleted = 'DELETED',
  /** Document was moved to a different parent */
  ParentChanged = 'PARENT_CHANGED',
  /** Relationships to assets (datasets, dashboards, etc.) were added or removed */
  RelatedAssetsChanged = 'RELATED_ASSETS_CHANGED',
  /** Relationships to other documents were added or removed */
  RelatedDocumentsChanged = 'RELATED_DOCUMENTS_CHANGED',
  /** Document state changed (e.g., published <-> unpublished) */
  StateChanged = 'STATE_CHANGED',
  /** Document text content was modified */
  TextChanged = 'TEXT_CHANGED',
  /** Document title was modified */
  TitleChanged = 'TITLE_CHANGED'
}

/** The contents of a Document */
export type DocumentContent = {
  __typename?: 'DocumentContent';
  /** The text contents of the Document */
  text: Scalars['String'];
};

/** Input for Document content */
export type DocumentContentInput = {
  /** The text contents of the Document */
  text: Scalars['String'];
};

/** Information about a Document */
export type DocumentInfo = {
  __typename?: 'DocumentInfo';
  /** Content of the Document */
  contents: DocumentContent;
  /** The audit stamp for when the document was created */
  created: ResolvedAuditStamp;
  /** Custom properties of the Document */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** The audit stamp for when the document was last modified (any field) */
  lastModified: ResolvedAuditStamp;
  /** The parent document of this Document */
  parentDocument?: Maybe<DocumentParentDocument>;
  /** Assets referenced by or related to this Document */
  relatedAssets?: Maybe<Array<DocumentRelatedAsset>>;
  /** Documents referenced by or related to this Document */
  relatedDocuments?: Maybe<Array<DocumentRelatedDocument>>;
  /**
   * Information about the external source of this document.
   * Only populated for third-party documents ingested from external systems.
   * If null, the document is first-party (created directly in DataHub).
   */
  source?: Maybe<DocumentSource>;
  /** Status of the Document (published, unpublished, etc.) */
  status?: Maybe<DocumentStatus>;
  /** Optional title for the document */
  title?: Maybe<Scalars['String']>;
};

/** The parent document of the document */
export type DocumentParentDocument = {
  __typename?: 'DocumentParentDocument';
  /** The hierarchical parent document for this document */
  document: Document;
};

/** A data asset referenced by a Document */
export type DocumentRelatedAsset = {
  __typename?: 'DocumentRelatedAsset';
  /** The asset referenced by or related to the document */
  asset: Entity;
};

/** A document referenced by or related to another Document */
export type DocumentRelatedDocument = {
  __typename?: 'DocumentRelatedDocument';
  /** The document referenced by or related to the document */
  document: Document;
};

/** Settings specific to a Document */
export type DocumentSettings = {
  __typename?: 'DocumentSettings';
  /**
   * Whether or not this document should be visible in the global context (e.g., global navigation, knowledge base search).
   * When false, the document is accessible primarily through the assets it is related to.
   * When true, the document appears in the global documents space accessible to all users.
   */
  showInGlobalContext: Scalars['Boolean'];
};

/** Input for Document settings */
export type DocumentSettingsInput = {
  /**
   * Whether or not this document should be visible in the global context (e.g., global navigation, knowledge base search).
   * Defaults to true if not specified.
   */
  showInGlobalContext?: Maybe<Scalars['Boolean']>;
};

/** Information about the external source of a document */
export type DocumentSource = {
  __typename?: 'DocumentSource';
  /** Unique identifier in the external system */
  externalId?: Maybe<Scalars['String']>;
  /** URL to the external source where this document originated */
  externalUrl?: Maybe<Scalars['String']>;
  /** The type of the source */
  sourceType: DocumentSourceType;
};

/** The type of source for a document */
export enum DocumentSourceType {
  /** The document was ingested from an external source */
  External = 'EXTERNAL',
  /** Created via the DataHub UI or API */
  Native = 'NATIVE'
}

/** The state of a Document */
export enum DocumentState {
  /** Document is published and visible to users */
  Published = 'PUBLISHED',
  /** Document is not published publically */
  Unpublished = 'UNPUBLISHED'
}

/** Status information for a Document */
export type DocumentStatus = {
  __typename?: 'DocumentStatus';
  /** The current state of the document */
  state: DocumentState;
};

/** Object containing the documentation aspect for an entity */
export type Documentation = {
  __typename?: 'Documentation';
  /** Structured properties on this entity */
  documentations: Array<DocumentationAssociation>;
};

/** Object containing the documentation aspect for an entity */
export type DocumentationAssociation = {
  __typename?: 'DocumentationAssociation';
  /** Information about who, why, and how this metadata was applied */
  attribution?: Maybe<MetadataAttribution>;
  /** Structured properties on this entity */
  documentation: Scalars['String'];
};

/** A domain, or a logical grouping of Metadata Entities */
export type Domain = Entity & {
  __typename?: 'Domain';
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** Display properties for the domain */
  displayProperties?: Maybe<DisplayProperties>;
  /** Children entities inside of the Domain */
  entities?: Maybe<SearchResults>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** Id of the domain */
  id: Scalars['String'];
  /** References to internal resources related to the dataset */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the dataset */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of parent domains for this entity */
  parentDomains?: Maybe<ParentDomainsResult>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Properties about a domain */
  properties?: Maybe<DomainProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Settings associated with this asset */
  settings?: Maybe<AssetSettings>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the domain */
  urn: Scalars['String'];
};


/** A domain, or a logical grouping of Metadata Entities */
export type DomainAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** A domain, or a logical grouping of Metadata Entities */
export type DomainEntitiesArgs = {
  input?: Maybe<DomainEntitiesInput>;
};


/** A domain, or a logical grouping of Metadata Entities */
export type DomainRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** A domain, or a logical grouping of Metadata Entities */
export type DomainRelationshipsArgs = {
  input: RelationshipsInput;
};

export type DomainAssociation = {
  __typename?: 'DomainAssociation';
  /** Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** The domain related to the assocaited urn */
  domain: Domain;
};

/** Input required to fetch the entities inside of a Domain. */
export type DomainEntitiesInput = {
  /** The number of entities to include in result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Optional query filter for particular entities inside the domain */
  query?: Maybe<Scalars['String']>;
  /** The offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Properties about a domain */
export type DomainProperties = {
  __typename?: 'DomainProperties';
  /** A Resolved Audit Stamp corresponding to the creation of this resource */
  createdOn?: Maybe<ResolvedAuditStamp>;
  /** Description of the Domain */
  description?: Maybe<Scalars['String']>;
  /** Display name of the domain */
  name: Scalars['String'];
};

/** Deprecated, use relationships query instead */
export type DownstreamEntityRelationships = {
  __typename?: 'DownstreamEntityRelationships';
  entities?: Maybe<Array<Maybe<EntityRelationshipLegacy>>>;
};

/** An ERModelRelationship is a high-level abstraction that dictates what datasets fields are erModelRelationshiped. */
export type ErModelRelationship = Entity & EntityWithRelationships & {
  __typename?: 'ERModelRelationship';
  /** An additional set of of read write properties */
  editableProperties?: Maybe<ErModelRelationshipEditableProperties>;
  /** The structured glossary terms associated with the dataset */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Unique id for the erModelRelationship */
  id: Scalars['String'];
  /** References to internal resources related to the dataset */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** No-op required for the model */
  lineage?: Maybe<EntityLineageResult>;
  /** Ownership metadata of the dataset */
  ownership?: Maybe<Ownership>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** An additional set of read only properties */
  properties?: Maybe<ErModelRelationshipProperties>;
  /** List of relationships between the source Entity and some destination entities with a given types */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status of the Dataset */
  status?: Maybe<Status>;
  /** Tags used for searching dataset */
  tags?: Maybe<GlobalTags>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the role */
  urn: Scalars['String'];
};


/** An ERModelRelationship is a high-level abstraction that dictates what datasets fields are erModelRelationshiped. */
export type ErModelRelationshipLineageArgs = {
  input: LineageInput;
};


/** An ERModelRelationship is a high-level abstraction that dictates what datasets fields are erModelRelationshiped. */
export type ErModelRelationshipRelationshipsArgs = {
  input: RelationshipsInput;
};

/** The Cardinality of the ERModelRelationship */
export enum ErModelRelationshipCardinality {
  /** Many to Many */
  NN = 'N_N',
  /** Many to One */
  NOne = 'N_ONE',
  /** One to Many */
  OneN = 'ONE_N',
  /** One to One */
  OneOne = 'ONE_ONE'
}

/** Additional properties about a ERModelRelationship */
export type ErModelRelationshipEditableProperties = {
  __typename?: 'ERModelRelationshipEditableProperties';
  /** Documentation of the ERModelRelationship */
  description?: Maybe<Scalars['String']>;
  /** Display name of the ERModelRelationship */
  name?: Maybe<Scalars['String']>;
};

/** Update to writable Dataset fields */
export type ErModelRelationshipEditablePropertiesUpdate = {
  /** Writable description for ERModelRelationship */
  description: Scalars['String'];
  /** Display name of the ERModelRelationship */
  name?: Maybe<Scalars['String']>;
};

/** Additional properties about a ERModelRelationship */
export type ErModelRelationshipProperties = {
  __typename?: 'ERModelRelationshipProperties';
  /** Cardinality of the ERModelRelationship */
  cardinality: ErModelRelationshipCardinality;
  /** Created actor urn associated with the ERModelRelationship */
  createdActor?: Maybe<Entity>;
  /** Created timestamp millis associated with the ERModelRelationship */
  createdTime?: Maybe<Scalars['Long']>;
  /** The urn of destination */
  destination: Dataset;
  /** The name of the ERModelRelationship used in display */
  name: Scalars['String'];
  /** The relationFieldMappings */
  relationshipFieldMappings?: Maybe<Array<RelationshipFieldMapping>>;
  /** The urn of source */
  source: Dataset;
};

/** Details about the ERModelRelationship */
export type ErModelRelationshipPropertiesInput = {
  /** optional flag about the ERModelRelationship is getting create */
  created?: Maybe<Scalars['Boolean']>;
  /** optional field to prevent created time while the ERModelRelationship is getting update */
  createdAt?: Maybe<Scalars['Long']>;
  /** optional field to prevent create actor while the ERModelRelationship is getting update */
  createdBy?: Maybe<Scalars['String']>;
  /** Details about the ERModelRelationship */
  destination: Scalars['String'];
  /** optional field to store cardinality of the ERModelRelationship */
  erModelRelationshipCardinality?: Maybe<ErModelRelationshipCardinality>;
  /** Details about the ERModelRelationship */
  name: Scalars['String'];
  /** Details about the ERModelRelationship */
  relationshipFieldmappings?: Maybe<Array<RelationshipFieldMappingInput>>;
  /** Details about the ERModelRelationship */
  source: Scalars['String'];
};

/** Input required to create/update a new ERModelRelationship */
export type ErModelRelationshipUpdateInput = {
  /** Update to editable properties */
  editableProperties?: Maybe<ErModelRelationshipEditablePropertiesUpdate>;
  /** Details about the ERModelRelationship */
  properties?: Maybe<ErModelRelationshipPropertiesInput>;
};

/** Editable schema field metadata ie descriptions, tags, etc */
export type EditableSchemaFieldInfo = {
  __typename?: 'EditableSchemaFieldInfo';
  /** Edited description of the field */
  description?: Maybe<Scalars['String']>;
  /** Flattened name of a field identifying the field the editable info is applied to */
  fieldPath: Scalars['String'];
  /**
   * Deprecated, use tags field instead
   * Tags associated with the field
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** Glossary terms associated with the field */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Tags associated with the field */
  tags?: Maybe<GlobalTags>;
};

/** Update to writable schema field metadata */
export type EditableSchemaFieldInfoUpdate = {
  /** Edited description of the field */
  description?: Maybe<Scalars['String']>;
  /** Flattened name of a field identifying the field the editable info is applied to */
  fieldPath: Scalars['String'];
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
  /** A description of the Tag */
  description?: Maybe<Scalars['String']>;
  /** A display name for the Tag */
  name?: Maybe<Scalars['String']>;
};

/** Information required to render an embedded version of an asset */
export type Embed = {
  __typename?: 'Embed';
  /** A URL which can be rendered inside of an iframe. */
  renderUrl?: Maybe<Scalars['String']>;
};

/**
 * Embedding model configuration for generating semantic search vectors.
 * Only includes minimal settings required for clients to match server embedding behavior.
 */
export type EmbeddingConfig = {
  __typename?: 'EmbeddingConfig';
  /** AWS Bedrock-specific configuration (only populated when provider is "aws-bedrock") */
  awsProviderConfig?: Maybe<AwsProviderConfig>;
  /**
   * Embedding storage key for SemanticContent aspects (e.g., "cohere_embed_v3").
   * This is the canonical key used in the embeddings map when storing SemanticContent aspects.
   * Server-provided to ensure exact match between client (writing) and server (querying).
   */
  modelEmbeddingKey: Scalars['String'];
  /** Model identifier (e.g., "cohere.embed-english-v3") */
  modelId: Scalars['String'];
  /** Embedding provider type (e.g., "aws-bedrock") */
  provider: Scalars['String'];
};

/** A top level Metadata Entity */
export type Entity = {
  /** List of relationships between the source Entity and some destination entities with a given types */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key of the Metadata Entity */
  urn: Scalars['String'];
};


/** A top level Metadata Entity */
export type EntityRelationshipsArgs = {
  input: RelationshipsInput;
};

/** A list of Assertions Associated with an Entity */
export type EntityAssertionsResult = {
  __typename?: 'EntityAssertionsResult';
  /** The assertions themselves */
  assertions: Array<Assertion>;
  /** The number of assertions in the returned result set */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of assertions in the result set */
  total: Scalars['Int'];
};

/** Input for the get entity counts endpoint */
export type EntityCountInput = {
  types?: Maybe<Array<EntityType>>;
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

export type EntityCountResult = {
  __typename?: 'EntityCountResult';
  count: Scalars['Int'];
  entityType: EntityType;
};

export type EntityCountResults = {
  __typename?: 'EntityCountResults';
  counts?: Maybe<Array<EntityCountResult>>;
};

/** A list of Incidents Associated with an Entity */
export type EntityIncidentsResult = {
  __typename?: 'EntityIncidentsResult';
  /** The number of assertions in the returned result set */
  count: Scalars['Int'];
  /** The incidents themselves */
  incidents: Array<Incident>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of assertions in the result set */
  total: Scalars['Int'];
};

/** A list of lineage information associated with a source Entity */
export type EntityLineageResult = {
  __typename?: 'EntityLineageResult';
  /** Number of results in the returned result set */
  count?: Maybe<Scalars['Int']>;
  /** The number of results that were filtered out of the page (soft-deleted or non-existent) */
  filtered?: Maybe<Scalars['Int']>;
  /** Relationships in the result set */
  relationships: Array<LineageRelationship>;
  /** Start offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** Total number of results in the result set */
  total?: Maybe<Scalars['Int']>;
};

/** An overview of the field that was matched in the entity search document */
export type EntityPath = {
  __typename?: 'EntityPath';
  /** Path of entities between source and destination nodes */
  path: Array<Maybe<Entity>>;
};

/** Shared privileges object across entities. Not all privileges apply to every entity. */
export type EntityPrivileges = {
  __typename?: 'EntityPrivileges';
  /** Whether or not a user can update assertions for an asset */
  canEditAssertions?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the data product(s) that the entity belongs to */
  canEditDataProducts?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the deprecation status for an entity */
  canEditDeprecation?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the description for the entity */
  canEditDescription?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the domain(s) for the entity */
  canEditDomains?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user update the embed information */
  canEditEmbed?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update glossary terms for the entity */
  canEditGlossaryTerms?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the incidents for an asset */
  canEditIncidents?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can create or delete lineage edges for an entity. */
  canEditLineage?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the links for the entity */
  canEditLinks?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the owners for the entity */
  canEditOwners?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the properties for the entity (e.g. dataset) */
  canEditProperties?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the Queries for the entity (e.g. dataset) */
  canEditQueries?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the schema field tags for a dataset */
  canEditSchemaFieldDescription?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the schema field tags for a dataset */
  canEditSchemaFieldGlossaryTerms?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update the schema field tags for a dataset */
  canEditSchemaFieldTags?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can update tags for the entity */
  canEditTags?: Maybe<Scalars['Boolean']>;
  /** Whether the user can manage asset summary */
  canManageAssetSummary?: Maybe<Scalars['Boolean']>;
  /**
   * Whether or not a user can create child entities under a parent entity.
   * For example, can one create Terms/Node sunder a Glossary Node.
   */
  canManageChildren?: Maybe<Scalars['Boolean']>;
  /** Whether or not a user can delete or move this entity. */
  canManageEntity?: Maybe<Scalars['Boolean']>;
  /** Whether the user can view dataset operations */
  canViewDatasetOperations?: Maybe<Scalars['Boolean']>;
  /** Whether the user can view dataset profiling stats */
  canViewDatasetProfile?: Maybe<Scalars['Boolean']>;
  /** Whether the user can view dataset usage stats */
  canViewDatasetUsage?: Maybe<Scalars['Boolean']>;
};

/** Configuration for an entity profile */
export type EntityProfileConfig = {
  __typename?: 'EntityProfileConfig';
  /**
   * The enum value from EntityProfileTab for which tab should be showed by default on
   * entity profile pages. If null, rely on default sorting from React code.
   */
  defaultTab?: Maybe<Scalars['String']>;
};

/** Context to define the entity profile page */
export type EntityProfileParams = {
  __typename?: 'EntityProfileParams';
  /** Type of the enity being displayed */
  type: EntityType;
  /** Urn of the entity being shown */
  urn: Scalars['String'];
};

/** Configuration for different entity profiles */
export type EntityProfilesConfig = {
  __typename?: 'EntityProfilesConfig';
  /** The configurations for a Domain entity profile */
  domain?: Maybe<EntityProfileConfig>;
};

/** A relationship between two entities TODO Migrate all entity relationships to this more generic model */
export type EntityRelationship = {
  __typename?: 'EntityRelationship';
  /** An AuditStamp corresponding to the last modification of this relationship */
  created?: Maybe<AuditStamp>;
  /** The direction of the relationship relative to the source entity */
  direction: RelationshipDirection;
  /** Entity that is related via lineage */
  entity?: Maybe<Entity>;
  /** The type of the relationship */
  type: Scalars['String'];
};

/** Deprecated, use relationships query instead */
export type EntityRelationshipLegacy = {
  __typename?: 'EntityRelationshipLegacy';
  /** An AuditStamp corresponding to the last modification of this relationship */
  created?: Maybe<AuditStamp>;
  /** Entity that is related via lineage */
  entity?: Maybe<EntityWithRelationships>;
};

/** A list of relationship information associated with a source Entity */
export type EntityRelationshipsResult = {
  __typename?: 'EntityRelationshipsResult';
  /** Number of results in the returned result set */
  count?: Maybe<Scalars['Int']>;
  /** Relationships in the result set */
  relationships: Array<EntityRelationship>;
  /** Start offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** Total number of results in the result set */
  total?: Maybe<Scalars['Int']>;
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
  /** A DataHub Access Token */
  AccessToken = 'ACCESS_TOKEN',
  /** An application */
  Application = 'APPLICATION',
  /** A DataHub Assertion */
  Assertion = 'ASSERTION',
  /** A Business Attribute */
  BusinessAttribute = 'BUSINESS_ATTRIBUTE',
  /** The Chart Entity */
  Chart = 'CHART',
  /** A container of Metadata Entities */
  Container = 'CONTAINER',
  /** The CorpGroup Entity */
  CorpGroup = 'CORP_GROUP',
  /** The CorpUser Entity */
  CorpUser = 'CORP_USER',
  /** A Custom Ownership Type */
  CustomOwnershipType = 'CUSTOM_OWNERSHIP_TYPE',
  /** The Dashboard Entity */
  Dashboard = 'DASHBOARD',
  /** A connection to an external source. */
  DatahubConnection = 'DATAHUB_CONNECTION',
  /** An DataHub File */
  DatahubFile = 'DATAHUB_FILE',
  /** An DataHub Page Module */
  DatahubPageModule = 'DATAHUB_PAGE_MODULE',
  /** An DataHub Page Template */
  DatahubPageTemplate = 'DATAHUB_PAGE_TEMPLATE',
  /** A DataHub Policy */
  DatahubPolicy = 'DATAHUB_POLICY',
  /** A DataHub Role */
  DatahubRole = 'DATAHUB_ROLE',
  /** A DataHub View */
  DatahubView = 'DATAHUB_VIEW',
  /** The Dataset Entity */
  Dataset = 'DATASET',
  /** A data contract */
  DataContract = 'DATA_CONTRACT',
  /** The Data Flow (or Data Pipeline) Entity, */
  DataFlow = 'DATA_FLOW',
  /** The Data Job (or Data Task) Entity */
  DataJob = 'DATA_JOB',
  /** The DataPlatform Entity */
  DataPlatform = 'DATA_PLATFORM',
  /** Data Platform Instance Entity */
  DataPlatformInstance = 'DATA_PLATFORM_INSTANCE',
  /** An instance of an individual run of a data job or data flow */
  DataProcessInstance = 'DATA_PROCESS_INSTANCE',
  /** A Data Product */
  DataProduct = 'DATA_PRODUCT',
  /**
   * "
   * A data type registered to DataHub
   */
  DataType = 'DATA_TYPE',
  /** A Knowledge Article */
  Document = 'DOCUMENT',
  /** A Domain containing Metadata Entities */
  Domain = 'DOMAIN',
  /**
   * "
   * A type of entity registered to DataHub
   */
  EntityType = 'ENTITY_TYPE',
  /** The ERModelRelationship Entity */
  ErModelRelationship = 'ER_MODEL_RELATIONSHIP',
  /** A DataHub ExecutionRequest */
  ExecutionRequest = 'EXECUTION_REQUEST',
  /**
   * "
   * A form entity on entities
   */
  Form = 'FORM',
  /** The Glossary Node Entity */
  GlossaryNode = 'GLOSSARY_NODE',
  /** The Glossary Term Entity */
  GlossaryTerm = 'GLOSSARY_TERM',
  /** A DataHub incident - SaaS only */
  Incident = 'INCIDENT',
  /** A DataHub Managed Ingestion Source */
  IngestionSource = 'INGESTION_SOURCE',
  /** The ML Feature Entity */
  Mlfeature = 'MLFEATURE',
  /** ML Feature Table Entity */
  MlfeatureTable = 'MLFEATURE_TABLE',
  /** The ML Model Entity */
  Mlmodel = 'MLMODEL',
  /** The MLModelGroup Entity */
  MlmodelGroup = 'MLMODEL_GROUP',
  /** The ML Primary Key Entity */
  MlprimaryKey = 'MLPRIMARY_KEY',
  /** The Notebook Entity */
  Notebook = 'NOTEBOOK',
  /** Another entity type - refer to a provided entity type urn. */
  Other = 'OTHER',
  /** A DataHub Post */
  Post = 'POST',
  /** A dataset query */
  Query = 'QUERY',
  /**
   * "
   * A type of entity that is restricted to the user
   */
  Restricted = 'RESTRICTED',
  /**
   * "
   * A Role from an organisation
   */
  Role = 'ROLE',
  /** A Schema Field */
  SchemaField = 'SCHEMA_FIELD',
  /**
   * "
   * An structured property on entities
   */
  StructuredProperty = 'STRUCTURED_PROPERTY',
  /** The Tag Entity */
  Tag = 'TAG',
  /** A DataHub Test */
  Test = 'TEST',
  /** A set of versioned entities, representing a single source / logical entity over time */
  VersionSet = 'VERSION_SET'
}

/** An entity type registered in DataHub */
export type EntityTypeEntity = Entity & {
  __typename?: 'EntityTypeEntity';
  /** Info about this type including its name */
  info: EntityTypeInfo;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Query */
  urn: Scalars['String'];
};


/** An entity type registered in DataHub */
export type EntityTypeEntityRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Properties about an individual entity type */
export type EntityTypeInfo = {
  __typename?: 'EntityTypeInfo';
  /** The description of this type */
  description?: Maybe<Scalars['String']>;
  /** The display name of this type */
  displayName?: Maybe<Scalars['String']>;
  /** The fully qualified name of the entity type. This includes its namespace */
  qualifiedName: Scalars['String'];
  /** The standard entity type */
  type: EntityType;
};

export type EntityTypeToPlatforms = {
  /** Entity type to ignore as hops, if no platform is applied applies to all entities of this type. */
  entityType: EntityType;
  /** List of platforms to ignore as hops, empty implies all. Must be a valid platform urn */
  platforms?: Maybe<Array<Scalars['String']>>;
};

/** Deprecated, use relationships field instead */
export type EntityWithRelationships = {
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Metadata Entity */
  urn: Scalars['String'];
};


/** Deprecated, use relationships field instead */
export type EntityWithRelationshipsLineageArgs = {
  input: LineageInput;
};


/** Deprecated, use relationships field instead */
export type EntityWithRelationshipsRelationshipsArgs = {
  input: RelationshipsInput;
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
export type ExecutionRequest = Entity & {
  __typename?: 'ExecutionRequest';
  /** Unique id for the execution request */
  id: Scalars['String'];
  /** Input provided when creating the Execution Request */
  input: ExecutionRequestInput;
  /** Unused for execution requests */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Result of the execution request */
  result?: Maybe<ExecutionRequestResult>;
  /** The ingestion source of this execution request */
  source?: Maybe<IngestionSource>;
  /** The standard Entity Type */
  type: EntityType;
  /** Urn of the execution request */
  urn: Scalars['String'];
};


/** Retrieve an ingestion execution request */
export type ExecutionRequestRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Input provided when creating an Execution Request */
export type ExecutionRequestInput = {
  __typename?: 'ExecutionRequestInput';
  /** The actor who created this execution request */
  actor?: Maybe<CorpUser>;
  /**
   * Urn of the actor who created this execution request
   * @deprecated Use actor instead
   */
  actorUrn?: Maybe<Scalars['String']>;
  /** Arguments provided when creating the execution request */
  arguments?: Maybe<Array<StringMapEntry>>;
  /** The specific executor to route the request to. If none is provided, a "default" executor is used. */
  executorId?: Maybe<Scalars['String']>;
  /** The time at which the request was created */
  requestedAt: Scalars['Long'];
  /** The source of the execution request */
  source: ExecutionRequestSource;
  /** The type of the task to executed */
  task: Scalars['String'];
};

/** The result of an ExecutionRequest */
export type ExecutionRequestResult = {
  __typename?: 'ExecutionRequestResult';
  /** Duration of the task */
  durationMs?: Maybe<Scalars['Long']>;
  /** A report about the ingestion run */
  report?: Maybe<Scalars['String']>;
  /** Time at which the task began */
  startTimeMs?: Maybe<Scalars['Long']>;
  /** The result of the request, e.g. either SUCCEEDED or FAILED */
  status: Scalars['String'];
  /** A structured report for this Execution Request */
  structuredReport?: Maybe<StructuredReport>;
};

/** Information about the source of an execution request */
export type ExecutionRequestSource = {
  __typename?: 'ExecutionRequestSource';
  /** The urn of the ingestion source, if applicable */
  ingestionSource?: Maybe<Scalars['String']>;
  /** The type of the source, e.g. SCHEDULED_INGESTION_SOURCE */
  type?: Maybe<Scalars['String']>;
};

export type ExtraProperty = {
  __typename?: 'ExtraProperty';
  /** Name of the extra property */
  name: Scalars['String'];
  /** Value of the extra property */
  value: Scalars['String'];
};

/**
 * An environment identifier for a particular Entity, ie staging or production
 * Note that this model will soon be deprecated in favor of a more general purpose of notion
 * of data environment
 */
export enum FabricType {
  /** Designates corporation fabrics */
  Corp = 'CORP',
  /** Designates development fabrics */
  Dev = 'DEV',
  /** Designates early integration fabrics */
  Ei = 'EI',
  /** Designates non-production fabrics */
  NonProd = 'NON_PROD',
  /** Designates alternative spelling PROD */
  Prd = 'PRD',
  /** Designates pre-production fabrics */
  Pre = 'PRE',
  /** Designates production fabrics */
  Prod = 'PROD',
  /** Designates quality assurance fabrics */
  Qa = 'QA',
  /** Designates review fabrics */
  Rvw = 'RVW',
  /** Designates sandbox fabrics */
  Sandbox = 'SANDBOX',
  /** Designates staging fabrics */
  Stg = 'STG',
  /** Designates testing fabrics */
  Test = 'TEST',
  /** Designates alternative spelling TEST */
  Tst = 'TST',
  /** Designates user acceptance testing fabrics */
  Uat = 'UAT'
}

/** A single filter value */
export type FacetFilter = {
  __typename?: 'FacetFilter';
  /** Condition for the values. */
  condition?: Maybe<FilterOperator>;
  /** Name of field to filter by */
  field: Scalars['String'];
  /** If the filter should or should not be matched */
  negated?: Maybe<Scalars['Boolean']>;
  /** Values, one of which the intended field should match. */
  values: Array<Scalars['String']>;
};

/** Facet filters to apply to search results */
export type FacetFilterInput = {
  /** Condition for the values. How to If unset, assumed to be equality */
  condition?: Maybe<FilterOperator>;
  /** Name of field to filter by */
  field: Scalars['String'];
  /** If the filter should or should not be matched */
  negated?: Maybe<Scalars['Boolean']>;
  /**
   * Value of the field to filter by. Deprecated in favor of `values`, which should accept a single element array for a
   * value
   */
  value?: Maybe<Scalars['String']>;
  /** Values, one of which the intended field should match. */
  values?: Maybe<Array<Scalars['String']>>;
};

/** Contains valid fields to filter search results further on */
export type FacetMetadata = {
  __typename?: 'FacetMetadata';
  /** Aggregated search result counts by value of the field */
  aggregations: Array<AggregationMetadata>;
  /** Display name of the field */
  displayName?: Maybe<Scalars['String']>;
  /** Entity corresponding to the facet */
  entity?: Maybe<Entity>;
  /** Name of a field present in the search entity */
  field: Scalars['String'];
};

/** Configurations related to DataHub Views feature */
export type FeatureFlagsConfig = {
  __typename?: 'FeatureFlagsConfig';
  /** Enables displaying the asset summary page */
  assetSummaryPageV1: Scalars['Boolean'];
  /** Whether business attribute entity should be shown */
  businessAttributeEntityEnabled: Scalars['Boolean'];
  /** If enabled, shows the context documents feature in the sidebar. */
  contextDocumentsEnabled: Scalars['Boolean'];
  /** Whether data contracts should be enabled */
  dataContractsEnabled: Scalars['Boolean'];
  /** Enables displaying the dataset summary page */
  datasetSummaryPageV1: Scalars['Boolean'];
  /** If enabled, allows uploading of files for documentation. */
  documentationFileUploadV1: Scalars['Boolean'];
  /** Whether dataset names are editable */
  editableDatasetNameEnabled: Scalars['Boolean'];
  /** If turned on, exposes the versioning feature by allowing users to link entities in the UI. */
  entityVersioningEnabled: Scalars['Boolean'];
  /** Whether ERModelRelationship Tables Feature should be shown. */
  erModelRelationshipFeatureEnabled: Scalars['Boolean'];
  /** If enabled, allows glossary-based policies to be created. */
  glossaryBasedPoliciesEnabled: Scalars['Boolean'];
  /**
   * If turned on, hides DBT Sources from lineage by:
   * i) Hiding the source in the lineage graph, if it has no downstreams
   * ii) Swapping to the source's sibling urn on V2 lineage graph
   * iii) Representing source sibling as a merged node, with both icons on graph and combined version in sidebar
   */
  hideDbtSourceInLineage: Scalars['Boolean'];
  /** If enabled, hides lineage information in search result cards */
  hideLineageInSearchCards: Scalars['Boolean'];
  /** Enables displaying the ingestion onboarding redesign */
  ingestionOnboardingRedesignV1: Scalars['Boolean'];
  /** Whether to show the new lineage visualization. */
  lineageGraphV2: Scalars['Boolean'];
  /** Enables the redesign of the lineage v2 graph */
  lineageGraphV3: Scalars['Boolean'];
  /** Enables logical models feature */
  logicalModelsEnabled: Scalars['Boolean'];
  /** If enabled, allows assets to belong to multiple data products simultaneously. */
  multipleDataProductsPerAsset: Scalars['Boolean'];
  /**
   * Enables the nested Domains feature that allows users to have sub-Domains.
   * If this is off, Domains appear "flat" again.
   */
  nestedDomainsEnabled: Scalars['Boolean'];
  /** Whether browse v2 is platform mode, which means that platforms are displayed instead of entity types at the root. */
  platformBrowseV2: Scalars['Boolean'];
  /**
   * Whether read only mode is enabled on an instance.
   * Right now this only affects ability to edit user profile image URL but can be extended.
   */
  readOnlyModeEnabled: Scalars['Boolean'];
  /** Enables links to schema field-level lineage on lineage explorer. */
  schemaFieldCLLEnabled: Scalars['Boolean'];
  /** If turned on, schema field lineage will always fetch ghost entities and present them as real */
  schemaFieldLineageIgnoreStatus: Scalars['Boolean'];
  /** Whether we should show AccessManagement tab in the datahub UI. */
  showAccessManagement: Scalars['Boolean'];
  /** Whether we should show CTAs in the UI related to moving to DataHub Cloud by DataHub. */
  showAcrylInfo: Scalars['Boolean'];
  /** If turned on, we display auto complete results. Otherwise, do not. */
  showAutoCompleteResults: Scalars['Boolean'];
  /** Whether browse V2 sidebar should be shown */
  showBrowseV2: Scalars['Boolean'];
  /** If enabled, show the default external links on the entity page */
  showDefaultExternalLinks: Scalars['Boolean'];
  /** If turned on, show the "has siblings" filter in search */
  showHasSiblingsFilter: Scalars['Boolean'];
  /** If turned on, show the re-designed home page */
  showHomePageRedesign: Scalars['Boolean'];
  /** Enables displaying the homepage user role underneath the name. Only available for custom home page. */
  showHomepageUserRole: Scalars['Boolean'];
  /** If turned on, show the re-designed Ingestions page */
  showIngestionPageRedesign: Scalars['Boolean'];
  /** If enabled, we will show the introduce page in the V2 UI experience to add a title and select platforms */
  showIntroducePage: Scalars['Boolean'];
  /** If enabled, show the expand more button (>>) in the lineage graph */
  showLineageExpandMore: Scalars['Boolean'];
  /** If turned on, show the manage structured properties tab in the govern dropdown */
  showManageStructuredProperties: Scalars['Boolean'];
  /** If enabled, users will be able to view the tags management experience */
  showManageTags: Scalars['Boolean'];
  /** If turned on, show the newly designed nav bar in the V2 experience */
  showNavBarRedesign: Scalars['Boolean'];
  /** Whether product updates on the sidebar is enabled. Will go to oss. */
  showProductUpdates: Scalars['Boolean'];
  /** If turned on, show the redesigned search bar's autocomplete */
  showSearchBarAutocompleteRedesign: Scalars['Boolean'];
  /** Whether search filters V2 should be shown or the default filter side-panel */
  showSearchFiltersV2: Scalars['Boolean'];
  /** If turned on, all siblings will be separated with no way to get to a "combined" sibling view */
  showSeparateSiblings: Scalars['Boolean'];
  /** If turned on, show the re-designed Stats tab on the entity page */
  showStatsTabRedesign: Scalars['Boolean'];
  /**
   * Sets the default theme to V2.
   * If `themeV2Toggleable` is set, then users can toggle between V1 and V2.
   * If not, then the default is the only option.
   */
  themeV2Default: Scalars['Boolean'];
  /**
   * Allows the V2 theme to be turned on.
   * Includes new UX for home page, search, entity profiles, and lineage.
   * If false, then the V2 experience will be unavailable even if themeV2Default or themeV2Toggleable are true.
   */
  themeV2Enabled: Scalars['Boolean'];
  /** Allows the V2 theme to be toggled by users. */
  themeV2Toggleable: Scalars['Boolean'];
};

/** A definition of a Field (Column) assertion. */
export type FieldAssertionInfo = {
  __typename?: 'FieldAssertionInfo';
  /** The entity targeted by this Field check. */
  entityUrn: Scalars['String'];
  /** The definition of an assertion that validates a common metric obtained about a field / column for a set of rows. */
  fieldMetricAssertion?: Maybe<FieldMetricAssertion>;
  /** The definition of an assertion that validates individual values of a field / column for a set of rows. */
  fieldValuesAssertion?: Maybe<FieldValuesAssertion>;
  /**
   * A definition of the specific filters that should be applied, when performing monitoring.
   * If not provided, there is no filter, and the full table is under consideration.
   */
  filter?: Maybe<DatasetFilter>;
  /** The type of the field assertion being monitored. */
  type: FieldAssertionType;
};

/** The type of a Field assertion */
export enum FieldAssertionType {
  /**
   * An assertion used to validate the value of a common field / column metric (e.g. aggregation)
   * such as null count + percentage, min, max, median, and more.
   */
  FieldMetric = 'FIELD_METRIC',
  /** An assertion used to validate the values contained with a field / column given a set of rows. */
  FieldValues = 'FIELD_VALUES'
}

/** An association for field-level form prompts */
export type FieldFormPromptAssociation = {
  __typename?: 'FieldFormPromptAssociation';
  /** The schema field path */
  fieldPath: Scalars['String'];
  /** When and by whom this form field-level prompt has last been modified */
  lastModified: ResolvedAuditStamp;
};

/** A definition of a Field Metric assertion. */
export type FieldMetricAssertion = {
  __typename?: 'FieldMetricAssertion';
  /** The field under evaluation */
  field: SchemaFieldSpec;
  /** The specific metric to assert against. */
  metric: FieldMetricType;
  /** The predicate to evaluate against the metric for the field / column. */
  operator: AssertionStdOperator;
  /** Standard parameters required for the assertion. */
  parameters?: Maybe<AssertionStdParameters>;
};

/**
 * A standard metric that can be derived from the set of values
 * for a specific field / column of a dataset / table.
 */
export enum FieldMetricType {
  /**
   * The number of empty string values found in the value set (applies to string columns).
   * Note: This is a completely different metric different from NULL_COUNT!
   */
  EmptyCount = 'EMPTY_COUNT',
  /**
   * The percentage of empty string values to total rows for the dataset (applies to string columns).
   * Note: This is a completely different metric different from NULL_PERCENTAGE!
   */
  EmptyPercentage = 'EMPTY_PERCENTAGE',
  /** The maximum value in the column set (applies to numeric columns) */
  Max = 'MAX',
  /** The maximum length found in the column set (applies to string columns) */
  MaxLength = 'MAX_LENGTH',
  /** The mean length found in the column set (applies to numeric columns) */
  Mean = 'MEAN',
  /** The median length found in the column set (applies to numeric columns) */
  Median = 'MEDIAN',
  /** The minimum value in the column set (applies to numeric columns) */
  Min = 'MIN',
  /** The minimum length found in the column set (applies to string columns) */
  MinLength = 'MIN_LENGTH',
  /** The number of negative values found in the value set (applies to numeric columns) */
  NegativeCount = 'NEGATIVE_COUNT',
  /** The percentage of negative values to total rows for the dataset (applies to numeric columns) */
  NegativePercentage = 'NEGATIVE_PERCENTAGE',
  /** The number of null values found in the column value set */
  NullCount = 'NULL_COUNT',
  /** The percentage of null values to total rows for the dataset */
  NullPercentage = 'NULL_PERCENTAGE',
  /** The stddev length found in the column set (applies to numeric columns) */
  Stddev = 'STDDEV',
  /** The number of unique values found in the column value set */
  UniqueCount = 'UNIQUE_COUNT',
  /** The percentage of unique values to total rows for the dataset */
  UniquePercentage = 'UNIQUE_PERCENTAGE',
  /** The number of zero values found in the value set (applies to numeric columns) */
  ZeroCount = 'ZERO_COUNT',
  /** The percentage of zero values to total rows for the dataset (applies to numeric columns) */
  ZeroPercentage = 'ZERO_PERCENTAGE'
}

/** Definition of a transform applied to the values of a column / field. */
export type FieldTransform = {
  __typename?: 'FieldTransform';
  /** The type of the field transform. */
  type: FieldTransformType;
};

/** The type of the Field Transform */
export enum FieldTransformType {
  /** Obtain the length of a string field / column (applicable to string types) */
  Length = 'LENGTH'
}

/** The usage for a particular Dataset field */
export type FieldUsageCounts = {
  __typename?: 'FieldUsageCounts';
  /** The count of usages */
  count?: Maybe<Scalars['Int']>;
  /** The path of the field */
  fieldName?: Maybe<Scalars['String']>;
};

/** A definition of a Field Values assertion. */
export type FieldValuesAssertion = {
  __typename?: 'FieldValuesAssertion';
  /** Whether to ignore or allow nulls when running the values assertion. */
  excludeNulls: Scalars['Boolean'];
  /** Additional customization about when the assertion should be officially considered failing. */
  failThreshold: FieldValuesFailThreshold;
  /** The field under evaluation. */
  field: SchemaFieldSpec;
  /**
   * The predicate to evaluate against a single value of the field.
   * Depending on the operator, parameters may be required
   */
  operator: AssertionStdOperator;
  /** Standard parameters required for the assertion. */
  parameters?: Maybe<AssertionStdParameters>;
  /** An optional transform to apply to field values before evaluating the operator. */
  transform?: Maybe<FieldTransform>;
};

export type FieldValuesFailThreshold = {
  __typename?: 'FieldValuesFailThreshold';
  /** The type of failure threshold. */
  type: FieldValuesFailThresholdType;
  /** The value of the threshold, either representing a count or percentage. */
  value: Scalars['Long'];
};

/** The type of failure threshold. */
export enum FieldValuesFailThresholdType {
  /**
   * The maximum number of column values (i.e. rows) that are allowed
   * to fail the defined expectations before the assertion officially fails.
   */
  Count = 'COUNT',
  /**
   * The maximum percentage of rows that are allowed
   * to fail the defined column expectations before the assertion officially fails.
   */
  Percentage = 'PERCENTAGE'
}

/** A set of filter criteria */
export type FilterInput = {
  /** A list of conjunctive filters */
  and: Array<FacetFilterInput>;
};

export enum FilterOperator {
  /** Represent the relation: URN field matches any nested parent in addition to the given URN */
  AncestorsIncl = 'ANCESTORS_INCL',
  /** Represent the relation: String field contains value, e.g. name contains Profile */
  Contain = 'CONTAIN',
  /** Represent the relation: URN field any nested children in addition to the given URN */
  DescendantsIncl = 'DESCENDANTS_INCL',
  /** Represent the relation: String field ends with value, e.g. name ends with Event */
  EndWith = 'END_WITH',
  /** Represent the relation: field = value, e.g. platform = hdfs */
  Equal = 'EQUAL',
  /** Represents the relation: The field exists. If the field is an array, the field is either not present or empty. */
  Exists = 'EXISTS',
  /** Represent the relation greater than, e.g. ownerCount > 5 */
  GreaterThan = 'GREATER_THAN',
  /** Represent the relation greater than or equal to, e.g. ownerCount >= 5 */
  GreaterThanOrEqualTo = 'GREATER_THAN_OR_EQUAL_TO',
  /** Represent the relation: field = value (case-insensitive), e.g. platform = HDFS */
  Iequal = 'IEQUAL',
  /** * Represent the relation: String field is one of the array values to, e.g. name in ["Profile", "Event"] */
  In = 'IN',
  /** Represent the relation less than, e.g. ownerCount < 3 */
  LessThan = 'LESS_THAN',
  /** Represent the relation less than or equal to, e.g. ownerCount <= 3 */
  LessThanOrEqualTo = 'LESS_THAN_OR_EQUAL_TO',
  /** Represent the relation: URN field matches any nested child or parent in addition to the given URN */
  RelatedIncl = 'RELATED_INCL',
  /** Represent the relation: String field starts with value, e.g. name starts with PageView */
  StartWith = 'START_WITH'
}

export type FineGrainedLineage = {
  __typename?: 'FineGrainedLineage';
  downstreams?: Maybe<Array<SchemaFieldRef>>;
  query?: Maybe<Scalars['String']>;
  transformOperation?: Maybe<Scalars['String']>;
  upstreams?: Maybe<Array<SchemaFieldRef>>;
};

/** A fixed interval schedule. */
export type FixedIntervalSchedule = {
  __typename?: 'FixedIntervalSchedule';
  /** How many units. Defaults to 1. */
  multiple: Scalars['Int'];
  /** Interval unit such as minute/hour/day etc. */
  unit: DateInterval;
};

export type FloatBox = {
  __typename?: 'FloatBox';
  floatValue: Scalars['Float'];
};

/** Metadata around a foreign key constraint between two datasets */
export type ForeignKeyConstraint = {
  __typename?: 'ForeignKeyConstraint';
  /** The foreign dataset for easy reference */
  foreignDataset?: Maybe<Dataset>;
  /** List of fields in the foreign dataset */
  foreignFields?: Maybe<Array<Maybe<SchemaFieldEntity>>>;
  /** The human-readable name of the constraint */
  name?: Maybe<Scalars['String']>;
  /** List of fields in this dataset */
  sourceFields?: Maybe<Array<Maybe<SchemaFieldEntity>>>;
};

/** A form that helps with filling out metadata on an entity */
export type Form = Entity & {
  __typename?: 'Form';
  /** Information about this form */
  info: FormInfo;
  /** Ownership metadata of the form */
  ownership?: Maybe<Ownership>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Form */
  urn: Scalars['String'];
};


/** A form that helps with filling out metadata on an entity */
export type FormRelationshipsArgs = {
  input: RelationshipsInput;
};

export type FormActorAssignment = {
  __typename?: 'FormActorAssignment';
  /** Groups that the form is assigned to. If null, then no groups are specifically targeted. */
  groups?: Maybe<Array<CorpGroup>>;
  /**
   * Whether or not the current actor is universally assigned to this form, either by user or by group.
   * Note that this does not take into account entity ownership based assignment.
   */
  isAssignedToMe: Scalars['Boolean'];
  /** Whether the form should be completed by owners of the assets which the form is applied to. */
  owners: Scalars['Boolean'];
  /** Urns of the users that the form is assigned to. If null, then no users are specifically targeted. */
  users?: Maybe<Array<CorpUser>>;
};

/** Input for assigning a form to actors */
export type FormActorAssignmentInput = {
  /** The optional list of group urns to assign this form to */
  groups?: Maybe<Array<Scalars['String']>>;
  /** Whether this form will be applied to owners of associated entities or not. Default is true. */
  owners?: Maybe<Scalars['Boolean']>;
  /** The optional list of user urns to assign this form to */
  users?: Maybe<Array<Scalars['String']>>;
};

/** Update input for assigning a form to actors */
export type FormActorAssignmentUpdateInput = {
  /** The optional list of group urns to assign this form to */
  groupsToAdd?: Maybe<Array<Scalars['String']>>;
  /** The groups being removed from being assigned to this form */
  groupsToRemove?: Maybe<Array<Scalars['String']>>;
  /** Whether this form will be applied to owners of associated entities or not. Default is true. */
  owners?: Maybe<Scalars['Boolean']>;
  /** The optional list of user urns to assign this form to */
  usersToAdd?: Maybe<Array<Scalars['String']>>;
  /** The users being removed from being assigned to this form */
  usersToRemove?: Maybe<Array<Scalars['String']>>;
};

export type FormAssociation = {
  __typename?: 'FormAssociation';
  /** Reference back to the urn with the form on it for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** The prompt that are already completed for this form */
  completedPrompts?: Maybe<Array<FormPromptAssociation>>;
  /** The form related to the associated urn */
  form: Form;
  /** The prompt that still need to be completed for this form */
  incompletePrompts?: Maybe<Array<FormPromptAssociation>>;
};

/** Properties about an individual Form */
export type FormInfo = {
  __typename?: 'FormInfo';
  /** The actors that are assigned to complete the forms for the associated entities. */
  actors: FormActorAssignment;
  /** The description of this form */
  description?: Maybe<Scalars['String']>;
  /** The name of this form */
  name: Scalars['String'];
  /** The prompt for this form */
  prompts: Array<FormPrompt>;
  /** The type of this form */
  type: FormType;
};

/** A prompt shown to the user to collect metadata about an entity */
export type FormPrompt = {
  __typename?: 'FormPrompt';
  /** The description of this prompt */
  description?: Maybe<Scalars['String']>;
  /** The urn of the parent form that this prompt is part of */
  formUrn: Scalars['String'];
  /** The ID of this prompt. This will be globally unique. */
  id: Scalars['String'];
  /** Whether the prompt is required for the form to be considered completed. */
  required: Scalars['Boolean'];
  /** The params for this prompt if type is STRUCTURED_PROPERTY */
  structuredPropertyParams?: Maybe<StructuredPropertyParams>;
  /** The title of this prompt */
  title: Scalars['String'];
  /** The description of this prompt */
  type: FormPromptType;
};

/** A form that helps with filling out metadata on an entity */
export type FormPromptAssociation = {
  __typename?: 'FormPromptAssociation';
  /** Optional information about the field-level prompt associations. */
  fieldAssociations?: Maybe<FormPromptFieldAssociations>;
  /** The unique id of the form prompt */
  id: Scalars['String'];
  /** When and by whom this form prompt has last been modified */
  lastModified: ResolvedAuditStamp;
};

/** Information about the field-level prompt associations. */
export type FormPromptFieldAssociations = {
  __typename?: 'FormPromptFieldAssociations';
  /** If this form prompt is for fields, this will contain a list of completed associations per field */
  completedFieldPrompts?: Maybe<Array<FieldFormPromptAssociation>>;
  /** If this form prompt is for fields, this will contain a list of incomlete associations per field */
  incompleteFieldPrompts?: Maybe<Array<FieldFormPromptAssociation>>;
};

/** Enum of all form prompt types */
export enum FormPromptType {
  /** A schema field-level structured property form prompt type. */
  FieldsStructuredProperty = 'FIELDS_STRUCTURED_PROPERTY',
  /** A structured property form prompt type. */
  StructuredProperty = 'STRUCTURED_PROPERTY'
}

/** The type of a form. This is optional on a form entity */
export enum FormType {
  /** This form is used to help with filling out metadata on entities */
  Completion = 'COMPLETION',
  /** This form is used for "verifying" entities as a state for governance and compliance */
  Verification = 'VERIFICATION'
}

/** Verification object that has been applied to the entity via a completed form. */
export type FormVerificationAssociation = {
  __typename?: 'FormVerificationAssociation';
  /** The form related to the associated urn */
  form: Form;
  /** When this verification was applied to this entity */
  lastModified?: Maybe<ResolvedAuditStamp>;
};

/** Requirements forms that are assigned to an entity. */
export type Forms = {
  __typename?: 'Forms';
  /** Forms that have been completed. */
  completedForms: Array<FormAssociation>;
  /** Forms that are still incomplete. */
  incompleteForms: Array<FormAssociation>;
  /** Verifications that have been applied to the entity via completed forms. */
  verifications: Array<FormVerificationAssociation>;
};

/** Information about an Freshness assertion. */
export type FreshnessAssertionInfo = {
  __typename?: 'FreshnessAssertionInfo';
  /** The urn of the entity that the Freshness assertion is related to */
  entityUrn: Scalars['String'];
  /** A filter applied when querying an external Dataset or Table */
  filter?: Maybe<DatasetFilter>;
  /** Produce FAIL Assertion Result if the asset is not updated on the cadence and within the time range described by the schedule. */
  schedule: FreshnessAssertionSchedule;
  /** The type of the Freshness Assertion */
  type: FreshnessAssertionType;
};

/** Attributes defining a single Freshness schedule. */
export type FreshnessAssertionSchedule = {
  __typename?: 'FreshnessAssertionSchedule';
  /** A cron schedule. This is populated if the type is CRON. */
  cron?: Maybe<FreshnessCronSchedule>;
  /** A fixed interval schedule. This is populated if the type is FIXED_INTERVAL. */
  fixedInterval?: Maybe<FixedIntervalSchedule>;
  /** The type of schedule */
  type: FreshnessAssertionScheduleType;
};

/** The type of an Freshness assertion */
export enum FreshnessAssertionScheduleType {
  /** An schedule based on a CRON schedule representing the expected event times. */
  Cron = 'CRON',
  /** A scheduled based on a recurring fixed schedule which is used to compute the expected operation window. E.g. "every 24 hours". */
  FixedInterval = 'FIXED_INTERVAL',
  /** A schedule computed based on when the assertion was last evaluated, to the current moment in time. */
  SinceTheLastCheck = 'SINCE_THE_LAST_CHECK'
}

/** The type of an Freshness assertion */
export enum FreshnessAssertionType {
  /** An assertion defined against a Dataset Change Operation - insert, update, delete, etc */
  DatasetChange = 'DATASET_CHANGE',
  /** An assertion defined against a Data Job run */
  DataJobRun = 'DATA_JOB_RUN'
}

export type FreshnessContract = {
  __typename?: 'FreshnessContract';
  /** The assertion representing the Freshness contract. */
  assertion: Assertion;
};

/** Input required to create an Freshness contract */
export type FreshnessContractInput = {
  /** The assertion monitoring this part of the data contract. Assertion must be of type Freshness. */
  assertionUrn: Scalars['String'];
};

/** A cron-formatted schedule */
export type FreshnessCronSchedule = {
  __typename?: 'FreshnessCronSchedule';
  /** A cron-formatted execution interval, as a cron string, e.g. 1 * * * * */
  cron: Scalars['String'];
  /** Timezone in which the cron interval applies, e.g. America/Los Angeles */
  timezone: Scalars['String'];
  /**
   * An optional offset in milliseconds to SUBTRACT from the timestamp generated by the cron schedule
   * to generate the lower bounds of the "Freshness window", or the window of time in which an event must have occurred in order for the Freshness
   * to be considering passing.
   * If left empty, the start of the Freshness window will be the _end_ of the previously evaluated Freshness window.
   */
  windowStartOffsetMs?: Maybe<Scalars['Long']>;
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
  /** The actor associated with the Access Token. */
  actorUrn: Scalars['String'];
  /** The duration for which the Access Token is valid. */
  duration: AccessTokenDuration;
  /** The type of the Access Token. */
  type: AccessTokenType;
};

/** Input for getting granted privileges */
export type GetGrantedPrivilegesInput = {
  /** Urn of the actor */
  actorUrn: Scalars['String'];
  /**
   * Whether to include policy evaluation details.
   * This will only return result if user has Manage Policies Privilege.
   */
  includeEvaluationDetails?: Maybe<Scalars['Boolean']>;
  /** Spec to identify resource. If empty, gets privileges granted to the actor */
  resourceSpec?: Maybe<ResourceSpec>;
};

/** Input provided when getting an invite token */
export type GetInviteTokenInput = {
  /** The urn of the role to get the invite token for */
  roleUrn?: Maybe<Scalars['String']>;
};

/** Input for the getUploadPresignedUrl query. */
export type GetPresignedUploadUrlInput = {
  /** The URN of the asset associated with the upload (required when scenario is ASSET_DOCUMENTATION). */
  assetUrn?: Maybe<Scalars['String']>;
  /** Optional content type of file e.g. "application/pdf" */
  contentType?: Maybe<Scalars['String']>;
  /** Original name of the file to upload */
  fileName: Scalars['String'];
  /** The scenario for the upload (e.g., ASSET_DOCUMENTATION, PROFILE_IMAGE). */
  scenario: UploadDownloadScenario;
  /** The dataset schema field this file is referenced by */
  schemaFieldUrn?: Maybe<Scalars['String']>;
};

export type GetPresignedUploadUrlResponse = {
  __typename?: 'GetPresignedUploadUrlResponse';
  fileId: Scalars['String'];
  url: Scalars['String'];
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
  /** The number of Glossary Entities in the returned result set */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
};

/** The result when getting Glossary entities */
export type GetRootGlossaryNodesResult = {
  __typename?: 'GetRootGlossaryNodesResult';
  /** The number of nodes in the returned result */
  count: Scalars['Int'];
  /** A list of Glossary Nodes without a parent node */
  nodes: Array<GlossaryNode>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of nodes in the result set */
  total: Scalars['Int'];
};

/** The result when getting root GlossaryTerms */
export type GetRootGlossaryTermsResult = {
  __typename?: 'GetRootGlossaryTermsResult';
  /** The number of terms in the returned result */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** A list of Glossary Terms without a parent node */
  terms: Array<GlossaryTerm>;
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
  /** List of schema blame. Absent when there are no fields to return history for. */
  schemaFieldBlameList?: Maybe<Array<SchemaFieldBlame>>;
  /** Selected semantic version */
  version?: Maybe<SemanticVersionStruct>;
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
  /** All semantic versions. Absent when there are no versions. */
  semanticVersionList?: Maybe<Array<SemanticVersionStruct>>;
  /** Selected semantic version */
  version?: Maybe<SemanticVersionStruct>;
};

/** Input arguments for retrieving the plaintext values of a set of secrets */
export type GetSecretValuesInput = {
  /** A list of secret names */
  secrets: Array<Scalars['String']>;
};

/**
 * Input for getting timeline from a specific version.
 * Todo: this is where additional filtering would go such as start & end times/versions, change types, etc
 */
export type GetTimelineInput = {
  /** The change category types to filter by. If left empty, will fetch all. */
  changeCategories?: Maybe<Array<ChangeCategoryType>>;
  /** The urn to fetch timeline for */
  urn: Scalars['String'];
};

/** Result of getting timeline from a specific version. */
export type GetTimelineResult = {
  __typename?: 'GetTimelineResult';
  changeTransactions: Array<ChangeTransaction>;
};

/** Global settings related to the home page for an instance */
export type GlobalHomePageSettings = {
  __typename?: 'GlobalHomePageSettings';
  /** The default page template for the home page for this instance */
  defaultTemplate?: Maybe<DataHubPageTemplate>;
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
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** Carries information about where an entity originated from. */
  childrenCount?: Maybe<GlossaryNodeChildrenCount>;
  /** Display properties for the glossary node */
  displayProperties?: Maybe<DisplayProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** Executes a search on the children of this glossary node */
  glossaryChildrenSearch?: Maybe<ScrollResults>;
  /** References to internal resources related to the Glossary Node */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Ownership metadata of the glossary term */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of glossary nodes for this entity */
  parentNodes?: Maybe<ParentNodesResult>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional properties associated with the Glossary Term */
  properties?: Maybe<GlossaryNodeProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Settings associated with this asset */
  settings?: Maybe<AssetSettings>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the glossary term */
  urn: Scalars['String'];
};


/**
 * A Glossary Node, or a directory in a Business Glossary represents a container of
 * Glossary Terms or other Glossary Nodes
 */
export type GlossaryNodeAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/**
 * A Glossary Node, or a directory in a Business Glossary represents a container of
 * Glossary Terms or other Glossary Nodes
 */
export type GlossaryNodeGlossaryChildrenSearchArgs = {
  input: ScrollAcrossEntitiesInput;
};


/**
 * A Glossary Node, or a directory in a Business Glossary represents a container of
 * Glossary Terms or other Glossary Nodes
 */
export type GlossaryNodeRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/**
 * A Glossary Node, or a directory in a Business Glossary represents a container of
 * Glossary Terms or other Glossary Nodes
 */
export type GlossaryNodeRelationshipsArgs = {
  input: RelationshipsInput;
};

/** All of the parent nodes for GlossaryTerms and GlossaryNodes */
export type GlossaryNodeChildrenCount = {
  __typename?: 'GlossaryNodeChildrenCount';
  /** The number of child glossary nodes */
  nodesCount: Scalars['Int'];
  /** The number of child glossary terms */
  termsCount: Scalars['Int'];
};

/** Additional read only properties about a Glossary Node */
export type GlossaryNodeProperties = {
  __typename?: 'GlossaryNodeProperties';
  /** A Resolved Audit Stamp corresponding to the creation of this resource */
  createdOn?: Maybe<ResolvedAuditStamp>;
  /** Custom properties of the Glossary Node */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Description of the glossary term */
  description?: Maybe<Scalars['String']>;
  /** The name of the Glossary Term */
  name: Scalars['String'];
};

/**
 * A Glossary Term, or a node in a Business Glossary representing a standardized domain
 * data type
 */
export type GlossaryTerm = Entity & {
  __typename?: 'GlossaryTerm';
  /**
   * Deprecated, use applications instead
   * The application associated with the glossary term
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the glossary term */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The deprecation status of the Glossary Term */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the glossary term */
  domain?: Maybe<DomainAssociation>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use properties field instead
   * Details of the Glossary Term
   */
  glossaryTermInfo?: Maybe<GlossaryTermInfo>;
  /** hierarchicalName of glossary term */
  hierarchicalName: Scalars['String'];
  /** References to internal resources related to the Glossary Term */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /**
   * A unique identifier for the Glossary Term. Deprecated - Use properties.name field instead.
   * @deprecated Field no longer supported
   */
  name: Scalars['String'];
  /** Ownership metadata of the glossary term */
  ownership?: Maybe<Ownership>;
  /** Recursively get the lineage of glossary nodes for this entity */
  parentNodes?: Maybe<ParentNodesResult>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional properties associated with the Glossary Term */
  properties?: Maybe<GlossaryTermProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Schema metadata of the dataset */
  schemaMetadata?: Maybe<SchemaMetadata>;
  /** Settings associated with this asset */
  settings?: Maybe<AssetSettings>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the glossary term */
  urn: Scalars['String'];
};


/**
 * A Glossary Term, or a node in a Business Glossary representing a standardized domain
 * data type
 */
export type GlossaryTermAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/**
 * A Glossary Term, or a node in a Business Glossary representing a standardized domain
 * data type
 */
export type GlossaryTermRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
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
  /** The actor who is responsible for the term being added" */
  actor?: Maybe<CorpUser>;
  /** Reference back to the associated urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** Information about who, why, and how this metadata was applied */
  attribution?: Maybe<MetadataAttribution>;
  /** The context of how/why this term is associated */
  context?: Maybe<Scalars['String']>;
  /** The glossary term itself */
  term: GlossaryTerm;
};

/**
 * Deprecated, use GlossaryTermProperties instead
 * Information about a glossary term
 */
export type GlossaryTermInfo = {
  __typename?: 'GlossaryTermInfo';
  /** Properties of the glossary term */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /**
   * Definition of the glossary term. Deprecated - Use 'description' instead.
   * @deprecated Field no longer supported
   */
  definition: Scalars['String'];
  /** Description of the glossary term */
  description?: Maybe<Scalars['String']>;
  /** The name of the Glossary Term */
  name?: Maybe<Scalars['String']>;
  /** Schema definition of glossary term */
  rawSchema?: Maybe<Scalars['String']>;
  /** Source Ref of the glossary term */
  sourceRef?: Maybe<Scalars['String']>;
  /** Source Url of the glossary term */
  sourceUrl?: Maybe<Scalars['String']>;
  /** Term Source of the glossary term */
  termSource: Scalars['String'];
};

/** Additional read only properties about a Glossary Term */
export type GlossaryTermProperties = {
  __typename?: 'GlossaryTermProperties';
  /** A Resolved Audit Stamp corresponding to the creation of this resource */
  createdOn?: Maybe<ResolvedAuditStamp>;
  /** Properties of the glossary term */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /**
   * Definition of the glossary term. Deprecated - Use 'description' instead.
   * @deprecated Field no longer supported
   */
  definition: Scalars['String'];
  /** Description of the glossary term */
  description?: Maybe<Scalars['String']>;
  /** The name of the Glossary Term */
  name: Scalars['String'];
  /** Schema definition of glossary term */
  rawSchema?: Maybe<Scalars['String']>;
  /** Source Ref of the glossary term */
  sourceRef?: Maybe<Scalars['String']>;
  /** Source Url of the glossary term */
  sourceUrl?: Maybe<Scalars['String']>;
  /** Term Source of the glossary term */
  termSource: Scalars['String'];
};

/** Glossary Terms attached to a particular Metadata Entity */
export type GlossaryTerms = {
  __typename?: 'GlossaryTerms';
  /** The set of glossary terms attached to the Metadata Entity */
  terms?: Maybe<Array<GlossaryTermAssociation>>;
};

/** A single grouping criterion for grouping search results */
export type GroupingCriterion = {
  /**
   * The base entity type that needs to be grouped
   * e.g. schemaField
   * Omitting this field will result in all base entities being grouped into the groupingEntityType.
   */
  baseEntityType?: Maybe<EntityType>;
  /**
   * The type of entity being grouped into
   * e.g. dataset, domain, etc.
   */
  groupingEntityType: EntityType;
};

/** A grouping specification for search results. */
export type GroupingSpec = {
  /**
   * A list of grouping criteria for grouping search results.
   * There is no implied order in the grouping criteria.
   */
  groupingCriteria?: Maybe<Array<GroupingCriterion>>;
};

export type HasExecutionRuns = {
  /** History of runs of this task */
  runs?: Maybe<DataProcessInstanceResult>;
};


export type HasExecutionRunsRunsArgs = {
  count?: Maybe<Scalars['Int']>;
  start?: Maybe<Scalars['Int']>;
};

export type HasLogicalParent = {
  /** If this entity represents a physical asset, this is its logical parent, from which metadata can propagate. */
  logicalParent?: Maybe<Entity>;
};

/** The resolved Health of an Asset */
export type Health = {
  __typename?: 'Health';
  /** If type=INCIDENTS and status=FAIL, populate the details of the latest incident. */
  activeIncidentHealthDetails?: Maybe<ActiveIncidentHealthDetails>;
  /**
   * NOTE: @deprecated
   * The causes responsible for the health status
   * I.e., the assertion urns that are failing
   */
  causes?: Maybe<Array<Scalars['String']>>;
  /** If type=ASSERTIONS, populate a breakdown of the assertion statuses by type. */
  latestAssertionStatusByType?: Maybe<Array<AssertionHealthStatusByType>>;
  /** An optional message describing the resolved health status */
  message?: Maybe<Scalars['String']>;
  /** The timestamp when the health was reported */
  reportedAt?: Maybe<Scalars['Long']>;
  /** An enum representing the resolved Health status of an Asset */
  status: HealthStatus;
  /** An enum representing the type of health indicator */
  type: HealthStatusType;
};

export enum HealthStatus {
  /** The Asset is in a failing (unhealthy) state */
  Fail = 'FAIL',
  /** The Asset is in a healthy state */
  Pass = 'PASS',
  /** The Asset is in a warning state */
  Warn = 'WARN'
}

/** The type of the health status */
export enum HealthStatusType {
  /** Assertions status */
  Assertions = 'ASSERTIONS',
  /** Incidents status */
  Incidents = 'INCIDENTS'
}

/** The params required if the module is type HIERARCHY_VIEW */
export type HierarchyViewModuleParams = {
  __typename?: 'HierarchyViewModuleParams';
  /** The list of assets to show in the module */
  assetUrns: Array<Scalars['String']>;
  /**
   * Optional filters to filter relatedEntities (assetUrns) out
   *
   * The stringified json representing the logical predicate built in the UI to select assets.
   * This predicate is turned into orFilters to send through graphql since graphql doesn't support
   * arbitrary nesting. This string is used to restore the UI for this logical predicate.
   */
  relatedEntitiesFilterJson?: Maybe<Scalars['String']>;
  /** Whether to show related entities in the module */
  showRelatedEntities: Scalars['Boolean'];
};

/** Input for the params required if the module is type HIERARCHY_VIEW */
export type HierarchyViewModuleParamsInput = {
  /** The list of assets to show in the module */
  assetUrns: Array<Scalars['String']>;
  /**
   * Optional filters to filter relatedEntities (assetUrns) out
   *
   * The stringified json representing the logical predicate built in the UI to select assets.
   * This predicate is turned into orFilters to send through graphql since graphql doesn't support
   * arbitrary nesting. This string is used to restore the UI for this logical predicate.
   */
  relatedEntitiesFilterJson?: Maybe<Scalars['String']>;
  /** Whether to show related entities in the module */
  showRelatedEntities: Scalars['Boolean'];
};

/** For consumption by UI only */
export type Highlight = {
  __typename?: 'Highlight';
  body: Scalars['String'];
  title: Scalars['String'];
  value: Scalars['Int'];
};

/** Configurations related to the Search bar */
export type HomePageConfig = {
  __typename?: 'HomePageConfig';
  /** The section that comes first on the personal sidebar on the homepage */
  firstInPersonalSidebar: PersonalSidebarSection;
};

export type HyperParameterMap = {
  __typename?: 'HyperParameterMap';
  key: Scalars['String'];
  value: HyperParameterValueType;
};

export type HyperParameterValueType = BooleanBox | FloatBox | IntBox | StringBox;

export enum IconLibrary {
  /** Icons from the Material UI icon library */
  Material = 'MATERIAL'
}

/** Properties describing an icon associated with an entity */
export type IconProperties = {
  __typename?: 'IconProperties';
  /** The source of the icon: e.g. Antd, Material, etc */
  iconLibrary?: Maybe<IconLibrary>;
  /** The name of the icon */
  name?: Maybe<Scalars['String']>;
  /** Any modifier for the icon, this will be library-specific, e.g. filled/outlined, etc */
  style?: Maybe<Scalars['String']>;
};

/** Input for Properties describing an icon associated with an entity */
export type IconPropertiesInput = {
  /** The source of the icon: e.g. Antd, Material, etc */
  iconLibrary?: Maybe<IconLibrary>;
  /** The name of the icon */
  name?: Maybe<Scalars['String']>;
  /** Any modifier for the icon, this will be library-specific, e.g. filled/outlined, etc */
  style?: Maybe<Scalars['String']>;
};

/** Configurations related to Identity Management */
export type IdentityManagementConfig = {
  __typename?: 'IdentityManagementConfig';
  /** Whether identity management screen is able to be shown in the UI */
  enabled: Scalars['Boolean'];
};

/** An incident represents an active issue on a data asset. */
export type Incident = Entity & {
  __typename?: 'Incident';
  /** The users or groups are assigned to resolve the incident */
  assignees?: Maybe<Array<OwnerType>>;
  /** The time at which the incident was initially created */
  created: AuditStamp;
  /** A custom type of incident. Present only if type is 'CUSTOM' */
  customType?: Maybe<Scalars['String']>;
  /** An optional description associated with the incident */
  description?: Maybe<Scalars['String']>;
  /** The entity that the incident is associated with. */
  entity: Entity;
  /** The status of an incident */
  incidentStatus: IncidentStatus;
  /** The type of incident */
  incidentType: IncidentType;
  /** Optional priority of the incident. */
  priority?: Maybe<IncidentPriority>;
  /** List of relationships between the source Entity and some destination entities with a given types */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The source of the incident, i.e. how it was generated */
  source?: Maybe<IncidentSource>;
  /** An optional time at which the incident actually started (may be before the date it was raised). */
  startedAt?: Maybe<Scalars['Long']>;
  /**
   * The status of an incident
   * @deprecated, use incidentStatus instead
   */
  status: IncidentStatus;
  /** The standard tags for the Incident */
  tags?: Maybe<GlobalTags>;
  /** An optional title associated with the incident */
  title?: Maybe<Scalars['String']>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the Incident */
  urn: Scalars['String'];
};


/** An incident represents an active issue on a data asset. */
export type IncidentRelationshipsArgs = {
  input: RelationshipsInput;
};

/** The priority of the incident */
export enum IncidentPriority {
  /** A critical priority incident (P0) */
  Critical = 'CRITICAL',
  /** A high priority incident (P1) */
  High = 'HIGH',
  /** A low priority incident (P3) */
  Low = 'LOW',
  /** A medium priority incident (P2) */
  Medium = 'MEDIUM'
}

/** Details about the source of an incident, e.g. how it was created. */
export type IncidentSource = {
  __typename?: 'IncidentSource';
  /** The source of the incident. If the source type is ASSERTION_FAILURE, this will have the assertion that generated the incident. */
  source?: Maybe<Entity>;
  /** The type of the incident source */
  type: IncidentSourceType;
};

/** Input required to create an incident source */
export type IncidentSourceInput = {
  /** The type of the incident source */
  type: IncidentSourceType;
};

/** The source type of an incident, implying how it was created. */
export enum IncidentSourceType {
  /** An assertion has failed, triggering the incident. */
  AssertionFailure = 'ASSERTION_FAILURE',
  /** The incident was created manually, from either the API or the UI. */
  Manual = 'MANUAL'
}

/** The lifecycle stage of the incident. */
export enum IncidentStage {
  /** The incident is in the resolved as completed stage. */
  Fixed = 'FIXED',
  /** The incident root cause is being investigated. */
  Investigation = 'INVESTIGATION',
  /**
   * The incident is in the resolved with no action required state, e.g., the
   * incident was a false positive, or was expected.
   */
  NoActionRequired = 'NO_ACTION_REQUIRED',
  /** The impact and priority of the incident is being actively assessed. */
  Triage = 'TRIAGE',
  /** The incident is in the remediation stage. */
  WorkInProgress = 'WORK_IN_PROGRESS'
}

/** The state of an incident. */
export enum IncidentState {
  /** The incident is ongoing, or active. */
  Active = 'ACTIVE',
  /** The incident is resolved. */
  Resolved = 'RESOLVED'
}

/** Details about the status of an asset incident */
export type IncidentStatus = {
  __typename?: 'IncidentStatus';
  /** The time that the status last changed */
  lastUpdated: AuditStamp;
  /** An optional message associated with the status */
  message?: Maybe<Scalars['String']>;
  /** The lifecycle stage of the incident. Null means that no stage has been assigned. */
  stage?: Maybe<IncidentStage>;
  /** The state of the incident */
  state: IncidentState;
};

/** Input required to create an incident status */
export type IncidentStatusInput = {
  /** An optional message associated with the status */
  message?: Maybe<Scalars['String']>;
  /** The lifecycle stage ofthe incident */
  stage?: Maybe<IncidentStage>;
  /** The state of the incident */
  state: IncidentState;
};

/** A specific type of incident */
export enum IncidentType {
  /** A custom type of incident */
  Custom = 'CUSTOM',
  /**
   * A Schema has failed, triggering the incident.
   * Raised on assets where assertions are configured to generate incidents.
   */
  DataSchema = 'DATA_SCHEMA',
  /**
   * A Field Assertion has failed, triggering the incident.
   * Raised on assets where assertions are configured to generate incidents.
   */
  Field = 'FIELD',
  /**
   * A Freshness Assertion has failed, triggering the incident.
   * Raised on assets where assertions are configured to generate incidents.
   */
  Freshness = 'FRESHNESS',
  /** An operational incident, e.g. failure to materialize a dataset, or failure to execute a task / pipeline. */
  Operational = 'OPERATIONAL',
  /**
   * A SQL Assertion has failed, triggering the incident.
   * Raised on assets where assertions are configured to generate incidents.
   */
  Sql = 'SQL',
  /**
   * A Volume Assertion has failed, triggering the incident.
   * Raised on assets where assertions are configured to generate incidents.
   */
  Volume = 'VOLUME'
}

/**
 * The definition of the transformer function that should be applied to a given field / column value in a dataset
 * in order to determine the segment or bucket that it belongs to, which in turn is used to evaluate
 * volume assertions.
 */
export type IncrementingSegmentFieldTransformer = {
  __typename?: 'IncrementingSegmentFieldTransformer';
  /**
   * The 'native' transformer type, useful as a back door if a custom transformer is required.
   * This field is required if the type is NATIVE.
   */
  nativeType?: Maybe<Scalars['String']>;
  /** The 'standard' operator type. Note that not all source systems will support all operators. */
  type: IncrementingSegmentFieldTransformerType;
};

/** The 'standard' transformer type. Note that not all source systems will support all operators. */
export enum IncrementingSegmentFieldTransformerType {
  /** Rounds a numeric value up to the nearest integer. */
  Ceiling = 'CEILING',
  /** Rounds a numeric value down to the nearest integer. */
  Floor = 'FLOOR',
  /**
   * A backdoor to provide a native operator type specific to a given source system like
   * Snowflake, Redshift, BQ, etc.
   */
  Native = 'NATIVE',
  /** Rounds a timestamp (in milliseconds) down to the start of the day. */
  TimestampMsToDate = 'TIMESTAMP_MS_TO_DATE',
  /** Rounds a timestamp (in milliseconds) down to the nearest hour. */
  TimestampMsToHour = 'TIMESTAMP_MS_TO_HOUR',
  /** Rounds a timestamp (in seconds) down to the start of the month. */
  TimestampMsToMinute = 'TIMESTAMP_MS_TO_MINUTE',
  /** Rounds a timestamp (in milliseconds) down to the start of the month */
  TimestampMsToMonth = 'TIMESTAMP_MS_TO_MONTH',
  /** Rounds a timestamp (in milliseconds) down to the start of the year */
  TimestampMsToYear = 'TIMESTAMP_MS_TO_YEAR'
}

/** Attributes defining an INCREMENTING_SEGMENT_ROW_COUNT_CHANGE volume assertion. */
export type IncrementingSegmentRowCountChange = {
  __typename?: 'IncrementingSegmentRowCountChange';
  /**
   * The operator you'd like to apply to the row count value
   * Note that only numeric operators are valid inputs:
   * GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,
   * BETWEEN.
   */
  operator: AssertionStdOperator;
  /**
   * The parameters you'd like to provide as input to the operator.
   * Note that only numeric parameter types are valid inputs: NUMBER.
   */
  parameters: AssertionStdParameters;
  /** A specification of how the 'segment' can be derived using a column and an optional transformer function. */
  segment: IncrementingSegmentSpec;
  /** The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage. */
  type: AssertionValueChangeType;
};

/** Attributes defining an INCREMENTING_SEGMENT_ROW_COUNT_TOTAL volume assertion. */
export type IncrementingSegmentRowCountTotal = {
  __typename?: 'IncrementingSegmentRowCountTotal';
  /**
   * The operator you'd like to apply.
   * Note that only numeric operators are valid inputs:
   * GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,
   * BETWEEN.
   */
  operator: AssertionStdOperator;
  /**
   * The parameters you'd like to provide as input to the operator.
   * Note that only numeric parameter types are valid inputs: NUMBER.
   */
  parameters: AssertionStdParameters;
  /** A specification of how the 'segment' can be derived using a column and an optional transformer function. */
  segment: IncrementingSegmentSpec;
};

/**
 * Core attributes required to identify an incrementing segment in a table. This type is mainly useful
 * for tables that constantly increase with new rows being added on a particular cadence (e.g. fact or event tables).
 *
 * An incrementing segment represents a logical chunk of data which is INSERTED
 * into a dataset on a regular interval, along with the presence of a constantly-incrementing column
 * value such as an event time, date partition, or last modified column.
 *
 * An incrementing segment is principally identified by 2 key attributes combined:
 *
 * 1. A field or column that represents the incrementing value. New rows that are inserted will be identified using this column.
 *    Note that the value of this column may not by itself represent the "bucket" or the "segment" in which the row falls.
 *
 * 2. [Optional] An transformer function that may be applied to the selected column value in order
 *    to obtain the final "segment identifier" or "bucket identifier". Rows that have the same value after applying the transformation
 *    will be grouped into the same segment, using which the final value (e.g. row count) will be determined.
 */
export type IncrementingSegmentSpec = {
  __typename?: 'IncrementingSegmentSpec';
  /** The field to use to generate segments. It must be constantly incrementing as new rows are inserted. */
  field: SchemaFieldSpec;
  /**
   * Optional transformer function to apply to the field in order to obtain the final segment or bucket identifier.
   * If not provided, then no operator will be applied to the field. (identity function)
   */
  transformer?: Maybe<IncrementingSegmentFieldTransformer>;
};

/** A set of configurations for an Ingestion Source */
export type IngestionConfig = {
  __typename?: 'IngestionConfig';
  /** Advanced: Whether or not to run ingestion in debug mode */
  debugMode?: Maybe<Scalars['Boolean']>;
  /** Advanced: The specific executor that should handle the execution request. Defaults to 'default'. */
  executorId: Scalars['String'];
  /** Advanced: Extra arguments for the ingestion run. */
  extraArgs?: Maybe<Array<StringMapEntry>>;
  /** The JSON-encoded recipe to use for ingestion */
  recipe: Scalars['String'];
  /** Advanced: The version of the ingestion framework to use */
  version?: Maybe<Scalars['String']>;
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
  /** The cron-formatted interval to execute the ingestion source on */
  interval: Scalars['String'];
  /** Time Zone abbreviation (e.g. GMT, EDT). Defaults to UTC. */
  timezone?: Maybe<Scalars['String']>;
};

/** An Ingestion Source Entity */
export type IngestionSource = {
  __typename?: 'IngestionSource';
  /** An type-specific set of configurations for the ingestion source */
  config: IngestionConfig;
  /** Previous requests to execute the ingestion source */
  executions?: Maybe<IngestionSourceExecutionRequests>;
  /** The latest successful execution request for this source */
  latestSuccessfulExecution?: Maybe<ExecutionRequest>;
  /** The display name of the Ingestion Source */
  name: Scalars['String'];
  /** Ownership metadata of the ingestion source */
  ownership?: Maybe<Ownership>;
  /** The data platform associated with this ingestion source */
  platform?: Maybe<DataPlatform>;
  /** An optional schedule associated with the Ingestion Source */
  schedule?: Maybe<IngestionSchedule>;
  /** The source or origin of the Ingestion Source */
  source?: Maybe<IngestionSourceSource>;
  /** The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe. */
  type: Scalars['String'];
  /** The primary key of the Ingestion Source */
  urn: Scalars['String'];
};


/** An Ingestion Source Entity */
export type IngestionSourceExecutionsArgs = {
  count?: Maybe<Scalars['Int']>;
  start?: Maybe<Scalars['Int']>;
};

/** Requests for execution associated with an ingestion source */
export type IngestionSourceExecutionRequests = {
  __typename?: 'IngestionSourceExecutionRequests';
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** The execution request objects comprising the result set */
  executionRequests: Array<ExecutionRequest>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** The total number of results in the result set */
  total?: Maybe<Scalars['Int']>;
};

/** Source information for an ingestion source */
export type IngestionSourceSource = {
  __typename?: 'IngestionSourceSource';
  /** The source type of the ingestion source */
  type: IngestionSourceSourceType;
};

/** Input for specifying ingestion source source information */
export type IngestionSourceSourceInput = {
  /** The source type of the ingestion source */
  type: IngestionSourceSourceType;
};

/** The type of ingestion source source */
export enum IngestionSourceSourceType {
  /** A system internal source, e.g. for running search indexing operations, feature computation, etc. */
  System = 'SYSTEM'
}

/** Input field of the chart */
export type InputField = {
  __typename?: 'InputField';
  schemaField?: Maybe<SchemaField>;
  schemaFieldUrn?: Maybe<Scalars['String']>;
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
  /** The author of this metadata */
  actor: ResolvedActor;
  /** Reference back to the owned urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /**
   * The author of this metadata
   * Deprecated! Use actor instead for users or groups.
   * @deprecated Use `actor`
   */
  author: CorpUser;
  /** An AuditStamp corresponding to the creation of this resource */
  created: AuditStamp;
  /**
   * Deprecated, use label instead
   * Description of the resource
   * @deprecated Field no longer supported
   */
  description: Scalars['String'];
  /** Label associated with the URL */
  label: Scalars['String'];
  /** Settings for this record of institutional memory */
  settings?: Maybe<InstitutionalMemoryMetadataSettings>;
  /** An AuditStamp corresponding to the updating of this resource */
  updated?: Maybe<AuditStamp>;
  /** Link to a document or wiki page or another internal resource */
  url: Scalars['String'];
};

/** Settings for an institutional memory record */
export type InstitutionalMemoryMetadataSettings = {
  __typename?: 'InstitutionalMemoryMetadataSettings';
  /** Show record in asset preview like on entity header and search previews */
  showInAssetPreview: Scalars['Boolean'];
};

/**
 * An institutional memory to add to a Metadata Entity
 * TODO Add a USER or GROUP actor enum
 */
export type InstitutionalMemoryMetadataUpdate = {
  /** The corp user urn of the author of the metadata */
  author: Scalars['String'];
  /** The time at which this metadata was created */
  createdAt?: Maybe<Scalars['Long']>;
  /** Description of the resource */
  description?: Maybe<Scalars['String']>;
  /** Link to a document or wiki page or another internal resource */
  url: Scalars['String'];
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

/** An entry in a integer string map represented as a tuple */
export type IntMapEntry = {
  __typename?: 'IntMapEntry';
  /** The key of the map entry */
  key: Scalars['String'];
  /** The value for the map entry */
  value?: Maybe<Scalars['Int']>;
};

export type IntendedUse = {
  __typename?: 'IntendedUse';
  /** Out of scope uses of the MLModel */
  outOfScopeUses?: Maybe<Array<Scalars['String']>>;
  /** Primary Intended Users */
  primaryUsers?: Maybe<Array<IntendedUserType>>;
  /** Primary Use cases for the model */
  primaryUses?: Maybe<Array<Scalars['String']>>;
};

export enum IntendedUserType {
  /** Developed for Enterprise Users */
  Enterprise = 'ENTERPRISE',
  /** Developed for Entertainment Purposes */
  Entertainment = 'ENTERTAINMENT',
  /** Developed for Hobbyists */
  Hobby = 'HOBBY'
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
  /** Downstream, or right-to-left in the lineage visualization */
  Downstream = 'DOWNSTREAM',
  /** Upstream, or left-to-right in the lineage visualization */
  Upstream = 'UPSTREAM'
}

export type LineageEdge = {
  /** Urn of the source entity. This urn is downstream of the destinationUrn. */
  downstreamUrn: Scalars['String'];
  /** Urn of the destination entity. This urn is upstream of the destinationUrn */
  upstreamUrn: Scalars['String'];
};

/** Flags to control lineage behavior */
export type LineageFlags = {
  /** An optional ending time to filter on */
  endTimeMillis?: Maybe<Scalars['Long']>;
  /** Limits the number of results explored per hop, still gets all edges each time a hop happens */
  entitiesExploredPerHopLimit?: Maybe<Scalars['Int']>;
  /**
   * Map of entity types to platforms to ignore when counting hops during graph walk. Note: this can potentially cause
   * a large amount of additional hops to occur and should be used with caution.
   */
  ignoreAsHops?: Maybe<Array<EntityTypeToPlatforms>>;
  /** An optional starting time to filter on */
  startTimeMillis?: Maybe<Scalars['Long']>;
};

/** Input for the list lineage property of an Entity */
export type LineageInput = {
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** The direction of the relationship, either incoming or outgoing from the source entity */
  direction: LineageDirection;
  /** An optional ending time to filter on */
  endTimeMillis?: Maybe<Scalars['Long']>;
  /** If enabled, include entities that do not exist or are soft deleted. */
  includeGhostEntities?: Maybe<Scalars['Boolean']>;
  /** Optional flag to not merge siblings in the response. They are merged by default. */
  separateSiblings?: Maybe<Scalars['Boolean']>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** An optional starting time to filter on */
  startTimeMillis?: Maybe<Scalars['Long']>;
};

/** Metadata about a lineage relationship between two entities */
export type LineageRelationship = {
  __typename?: 'LineageRelationship';
  /** The actor who created this lineage relationship. Could be null. */
  createdActor?: Maybe<Entity>;
  /** Timestamp for when this lineage relationship was created. Could be null. */
  createdOn?: Maybe<Scalars['Long']>;
  /** Degree of relationship (number of hops to get to entity) */
  degree: Scalars['Int'];
  /** Entity that is related via lineage */
  entity?: Maybe<Entity>;
  /** Whether this edge is a manual edge. Could be null. */
  isManual?: Maybe<Scalars['Boolean']>;
  /** The paths traversed for this relationship */
  paths?: Maybe<Array<Maybe<EntityPath>>>;
  /** The type of the relationship */
  type: Scalars['String'];
  /** The actor who last updated this lineage relationship. Could be null. */
  updatedActor?: Maybe<Entity>;
  /** Timestamp for when this lineage relationship was last updated. Could be null. */
  updatedOn?: Maybe<Scalars['Long']>;
};

/** The path taken when doing search across lineage */
export enum LineageSearchPath {
  /** Designates the lightning lineage code path */
  Lightning = 'LIGHTNING',
  /** Designates the tortoise lineage code path */
  Tortoise = 'TORTOISE'
}

/** The params required if the module is type LINK */
export type LinkModuleParams = {
  __typename?: 'LinkModuleParams';
  /** The description of the link */
  description?: Maybe<Scalars['String']>;
  /** The image URL of the link */
  imageUrl?: Maybe<Scalars['String']>;
  /** The URL of the link */
  linkUrl: Scalars['String'];
};

/** Input for the params required if the module is type LINK */
export type LinkModuleParamsInput = {
  /** The description of the link */
  description?: Maybe<Scalars['String']>;
  /** The image URL of the link */
  imageUrl?: Maybe<Scalars['String']>;
  /** The URL of the link */
  linkUrl: Scalars['String'];
};

/** Parameters required to specify the page to land once clicked */
export type LinkParams = {
  __typename?: 'LinkParams';
  /** Context to define the entity profile page */
  entityProfileParams?: Maybe<EntityProfileParams>;
  /** Context to define the search page */
  searchParams?: Maybe<SearchParams>;
};

export type LinkSettingsInput = {
  /** Determines whether this link should be shown in the asset preview like entity header and search result cards */
  showInAssetPreview?: Maybe<Scalars['Boolean']>;
};

/** Input for linking a versioned entity to a Version Set */
export type LinkVersionInput = {
  /** Optional comment about the version */
  comment?: Maybe<Scalars['String']>;
  /** The target versioned entity to link */
  linkedEntity: Scalars['String'];
  /** Optional creator from the source system, will be converted to an Urn */
  sourceCreator?: Maybe<Scalars['String']>;
  /** Optional timestamp from the source system */
  sourceTimestamp?: Maybe<Scalars['Long']>;
  /** Version Tag label for the version, should be unique within a version set (not enforced) */
  version: Scalars['String'];
  /** The target version set */
  versionSet: Scalars['String'];
};

/** Input arguments for listing access tokens */
export type ListAccessTokenInput = {
  /** The number of results to be returned, default is 20 */
  count?: Maybe<Scalars['Int']>;
  /** Facet filters to apply to search results, default is no filters */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** The starting offset of the result set, default is 0 */
  start?: Maybe<Scalars['Int']>;
};

/** Results returned when listing access tokens */
export type ListAccessTokenResult = {
  __typename?: 'ListAccessTokenResult';
  /** The number of results to be returned */
  count: Scalars['Int'];
  /** The starting offset of the result set */
  start: Scalars['Int'];
  /** The token metadata themselves */
  tokens: Array<AccessTokenMetadata>;
  /** The total number of results in the result set */
  total: Scalars['Int'];
};

/** Input provided when listing Business Attribute */
export type ListBusinessAttributesInput = {
  /** The maximum number of Business Attributes to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing Business Attribute */
export type ListBusinessAttributesResult = {
  __typename?: 'ListBusinessAttributesResult';
  /** The Business Attributes */
  businessAttributes: Array<BusinessAttribute>;
  /** The number of Business Attributes in the returned result set */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Business Attributes in the result set */
  total: Scalars['Int'];
};

/** Input required when listing DataHub Domains */
export type ListDomainsInput = {
  /** The maximum number of Domains to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional parent domain */
  parentDomain?: Maybe<Scalars['String']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing DataHub Domains */
export type ListDomainsResult = {
  __typename?: 'ListDomainsResult';
  /** The number of Domains in the returned result set */
  count: Scalars['Int'];
  /** The Domains themselves */
  domains: Array<Domain>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Domains in the result set */
  total: Scalars['Int'];
};

export type ListExecutionRequestsInput = {
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** An optional search query */
  query?: Maybe<Scalars['String']>;
  /** Optional sort order. Defaults to use systemCreated. */
  sort?: Maybe<SortCriterion>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** When defined it adds filter to show executions with only system/non system sources */
  systemSources?: Maybe<Scalars['Boolean']>;
};

/** Input provided when listing DataHub Global Views */
export type ListGlobalViewsInput = {
  /** The maximum number of Views to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** Input required when listing DataHub Groups */
export type ListGroupsInput = {
  /** The maximum number of Policies to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing DataHub Groups */
export type ListGroupsResult = {
  __typename?: 'ListGroupsResult';
  /** The number of Policies in the returned result set */
  count: Scalars['Int'];
  /** The groups themselves */
  groups: Array<CorpGroup>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Policies in the result set */
  total: Scalars['Int'];
};

/** Input arguments for listing Ingestion Sources */
export type ListIngestionSourcesInput = {
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** An optional search query */
  query?: Maybe<Scalars['String']>;
  /** Optional sort order. Defaults to use systemCreated. */
  sort?: Maybe<SortCriterion>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Results returned when listing ingestion sources */
export type ListIngestionSourcesResult = {
  __typename?: 'ListIngestionSourcesResult';
  /** The number of results to be returned */
  count: Scalars['Int'];
  /** The Ingestion Sources themselves */
  ingestionSources: Array<IngestionSource>;
  /** The starting offset of the result set */
  start: Scalars['Int'];
  /** The total number of results in the result set */
  total: Scalars['Int'];
};

/** Input provided when listing DataHub Views */
export type ListMyViewsInput = {
  /** The maximum number of Views to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
  /** Optional - List the type of View to filter for. */
  viewType?: Maybe<DataHubViewType>;
};

/** Input required for listing custom ownership types entities */
export type ListOwnershipTypesInput = {
  /** The maximum number of Custom Ownership Types to be returned in the result set, default is 20 */
  count?: Maybe<Scalars['Int']>;
  /** Optional Facet filters to apply to the result set */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned, default is 0 */
  start?: Maybe<Scalars['Int']>;
};

/** Results when listing custom ownership types. */
export type ListOwnershipTypesResult = {
  __typename?: 'ListOwnershipTypesResult';
  /** The number of results to be returned */
  count: Scalars['Int'];
  /** The Custom Ownership Types themselves */
  ownershipTypes: Array<OwnershipTypeEntity>;
  /** The starting offset of the result set */
  start: Scalars['Int'];
  /** The total number of results in the result set */
  total: Scalars['Int'];
};

/** Input required when listing DataHub Access Policies */
export type ListPoliciesInput = {
  /** The maximum number of Policies to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing DataHub Access Policies */
export type ListPoliciesResult = {
  __typename?: 'ListPoliciesResult';
  /** The number of Policies in the returned result set */
  count: Scalars['Int'];
  /** The Policies themselves */
  policies: Array<Policy>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Policies in the result set */
  total: Scalars['Int'];
};

/** Input provided when listing existing posts */
export type ListPostsInput = {
  /** The maximum number of Roles to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** Optional resource urn */
  resourceUrn?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing Posts */
export type ListPostsResult = {
  __typename?: 'ListPostsResult';
  /** The number of Roles in the returned result set */
  count: Scalars['Int'];
  /** The Posts themselves */
  posts: Array<Post>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Roles in the result set */
  total: Scalars['Int'];
};

/** Input required for listing query entities */
export type ListQueriesInput = {
  /** The maximum number of Queries to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** An optional Urn for the parent dataset that the query is associated with. */
  datasetUrn?: Maybe<Scalars['String']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** A raw search query */
  query?: Maybe<Scalars['String']>;
  /** Optional - Information on how to sort the list queries result */
  sortInput?: Maybe<SortQueriesInput>;
  /** An optional source for the query */
  source?: Maybe<QuerySource>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** Results when listing entity queries */
export type ListQueriesResult = {
  __typename?: 'ListQueriesResult';
  /** The number of results to be returned */
  count: Scalars['Int'];
  /** The Queries themselves */
  queries: Array<QueryEntity>;
  /** The starting offset of the result set */
  start: Scalars['Int'];
  /** The total number of results in the result set */
  total: Scalars['Int'];
};

/** Input arguments for fetching UI recommendations */
export type ListRecommendationsInput = {
  /** Max number of modules to return */
  limit?: Maybe<Scalars['Int']>;
  /** Context provider by the caller requesting recommendations */
  requestContext?: Maybe<RecommendationRequestContext>;
  /** Urn of the actor requesting recommendations */
  userUrn: Scalars['String'];
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

/** Results returned by the ListRecommendations query */
export type ListRecommendationsResult = {
  __typename?: 'ListRecommendationsResult';
  /** List of modules to show */
  modules: Array<RecommendationModule>;
};

/** Input provided when listing existing roles */
export type ListRolesInput = {
  /** The maximum number of Roles to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing DataHub Roles */
export type ListRolesResult = {
  __typename?: 'ListRolesResult';
  /** The number of Roles in the returned result set */
  count: Scalars['Int'];
  /** The Roles themselves */
  roles: Array<DataHubRole>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Roles in the result set */
  total: Scalars['Int'];
};

/** Input for listing DataHub Secrets */
export type ListSecretsInput = {
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** An optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Input for listing DataHub Secrets */
export type ListSecretsResult = {
  __typename?: 'ListSecretsResult';
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** The secrets themselves */
  secrets: Array<Secret>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** The total number of results in the result set */
  total?: Maybe<Scalars['Int']>;
};

/** Input arguments for listing service accounts */
export type ListServiceAccountsInput = {
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query to filter service accounts */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
};

/** Results returned when listing service accounts */
export type ListServiceAccountsResult = {
  __typename?: 'ListServiceAccountsResult';
  /** The number of results to be returned */
  count: Scalars['Int'];
  /** The service accounts */
  serviceAccounts: Array<ServiceAccount>;
  /** The starting offset of the result set */
  start: Scalars['Int'];
  /** The total number of results in the result set */
  total: Scalars['Int'];
};

/** Input required when listing DataHub Tests */
export type ListTestsInput = {
  /** The maximum number of Domains to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional query string to match on */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing DataHub Tests */
export type ListTestsResult = {
  __typename?: 'ListTestsResult';
  /** The number of Tests in the returned result set */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The Tests themselves */
  tests: Array<Test>;
  /** The total number of Tests in the result set */
  total: Scalars['Int'];
};

/** Input required when listing DataHub Users */
export type ListUsersInput = {
  /** The maximum number of Policies to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional search query */
  query?: Maybe<Scalars['String']>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
};

/** The result obtained when listing DataHub Users */
export type ListUsersResult = {
  __typename?: 'ListUsersResult';
  /** The number of Policies in the returned result set */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Policies in the result set */
  total: Scalars['Int'];
  /** The users themselves */
  users: Array<CorpUser>;
};

/** The result obtained when listing DataHub Views */
export type ListViewsResult = {
  __typename?: 'ListViewsResult';
  /** The number of Views in the returned result set */
  count: Scalars['Int'];
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
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
  Or = 'OR'
}


/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeature = Entity & EntityWithRelationships & {
  __typename?: 'MLFeature';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** MLFeature data type */
  dataType?: Maybe<MlFeatureDataType>;
  /** Deprecation */
  deprecation?: Maybe<Deprecation>;
  /** The description about the ML Feature */
  description?: Maybe<Scalars['String']>;
  /** The Domain associated with the entity */
  domain?: Maybe<DomainAssociation>;
  /** An additional set of of read write properties */
  editableProperties?: Maybe<MlFeatureEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** MLFeature featureNamespace */
  featureNamespace: Scalars['String'];
  /**
   * ModelProperties metadata of the MLFeature
   * @deprecated Field no longer supported
   */
  featureProperties?: Maybe<MlFeatureProperties>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the entity */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the MLFeature */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** The display name for the ML Feature */
  name: Scalars['String'];
  /** Ownership metadata of the MLFeature */
  ownership?: Maybe<Ownership>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** ModelProperties metadata of the MLFeature */
  properties?: Maybe<MlFeatureProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the MLFeature */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags applied to entity */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the ML Feature */
  urn: Scalars['String'];
};


/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeatureAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeatureLineageArgs = {
  input: LineageInput;
};


/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeatureRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** An ML Feature Metadata Entity Note that this entity is incubating */
export type MlFeatureRelationshipsArgs = {
  input: RelationshipsInput;
};

/** The data type associated with an individual Machine Learning Feature */
export enum MlFeatureDataType {
  Audio = 'AUDIO',
  Binary = 'BINARY',
  Byte = 'BYTE',
  Continuous = 'CONTINUOUS',
  Count = 'COUNT',
  Image = 'IMAGE',
  Interval = 'INTERVAL',
  Map = 'MAP',
  Nominal = 'NOMINAL',
  Ordinal = 'ORDINAL',
  Sequence = 'SEQUENCE',
  Set = 'SET',
  Text = 'TEXT',
  Time = 'TIME',
  Unknown = 'UNKNOWN',
  Useless = 'USELESS',
  Video = 'VIDEO'
}

export type MlFeatureEditableProperties = {
  __typename?: 'MLFeatureEditableProperties';
  /** The edited description */
  description?: Maybe<Scalars['String']>;
};

export type MlFeatureProperties = {
  __typename?: 'MLFeatureProperties';
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  dataType?: Maybe<MlFeatureDataType>;
  description?: Maybe<Scalars['String']>;
  sources?: Maybe<Array<Maybe<Dataset>>>;
  version?: Maybe<VersionTag>;
};

/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTable = BrowsableEntity & Entity & EntityWithRelationships & {
  __typename?: 'MLFeatureTable';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the ML Feature Table. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Deprecation */
  deprecation?: Maybe<Deprecation>;
  /** MLFeatureTable description */
  description?: Maybe<Scalars['String']>;
  /** The Domain associated with the entity */
  domain?: Maybe<DomainAssociation>;
  /** An additional set of of read write properties */
  editableProperties?: Maybe<MlFeatureTableEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /**
   * Deprecated, use properties field instead
   * ModelProperties metadata of the MLFeature
   * @deprecated Field no longer supported
   */
  featureTableProperties?: Maybe<MlFeatureTableProperties>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the entity */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the MLFeature */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** The display name */
  name: Scalars['String'];
  /** Ownership metadata of the MLFeatureTable */
  ownership?: Maybe<Ownership>;
  /** Standardized platform urn where the MLFeatureTable is defined */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties associated the the ML Feature Table */
  properties?: Maybe<MlFeatureTableProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the MLFeatureTable */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags applied to entity */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the ML Feature Table */
  urn: Scalars['String'];
};


/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTableAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTableLineageArgs = {
  input: LineageInput;
};


/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTableRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** An ML Feature Table Entity Note that this entity is incubating */
export type MlFeatureTableRelationshipsArgs = {
  input: RelationshipsInput;
};

export type MlFeatureTableEditableProperties = {
  __typename?: 'MLFeatureTableEditableProperties';
  /** The edited description */
  description?: Maybe<Scalars['String']>;
};

export type MlFeatureTableProperties = {
  __typename?: 'MLFeatureTableProperties';
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  description?: Maybe<Scalars['String']>;
  mlFeatures?: Maybe<Array<Maybe<MlFeature>>>;
  mlPrimaryKeys?: Maybe<Array<Maybe<MlPrimaryKey>>>;
};

export type MlHyperParam = {
  __typename?: 'MLHyperParam';
  createdAt?: Maybe<Scalars['Long']>;
  description?: Maybe<Scalars['String']>;
  name?: Maybe<Scalars['String']>;
  value?: Maybe<Scalars['String']>;
};

export type MlMetric = {
  __typename?: 'MLMetric';
  /** Timestamp when this metric was recorded */
  createdAt?: Maybe<Scalars['Long']>;
  /** Description of what this metric measures */
  description?: Maybe<Scalars['String']>;
  /** Name of the metric (e.g. accuracy, precision, recall) */
  name?: Maybe<Scalars['String']>;
  /** The computed value of the metric */
  value?: Maybe<Scalars['String']>;
};

/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModel = BrowsableEntity & Entity & EntityWithRelationships & SupportsVersions & {
  __typename?: 'MLModel';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the ML Model. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** Caveats and Recommendations of the mlmodel */
  caveatsAndRecommendations?: Maybe<CaveatsAndRecommendations>;
  /** Cost Aspect of the mlmodel */
  cost?: Maybe<Cost>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Deprecation */
  deprecation?: Maybe<Deprecation>;
  /** Human readable description for mlmodel */
  description?: Maybe<Scalars['String']>;
  /** The Domain associated with the entity */
  domain?: Maybe<DomainAssociation>;
  /** An additional set of of read write properties */
  editableProperties?: Maybe<MlModelEditableProperties>;
  /** Ethical Considerations of the mlmodel */
  ethicalConsiderations?: Maybe<EthicalConsiderations>;
  /** Evaluation Data of the mlmodel */
  evaluationData?: Maybe<Array<BaseData>>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** Factors metadata of the mlmodel */
  factorPrompts?: Maybe<MlModelFactorPrompts>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /**
   * Deprecated, use tags field instead
   * The standard tags for the ML Model
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** The structured glossary terms associated with the entity */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the mlmodel */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** Intended use of the mlmodel */
  intendedUse?: Maybe<IntendedUse>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Metrics metadata of the mlmodel */
  metrics?: Maybe<Metrics>;
  /** ML model display name */
  name: Scalars['String'];
  /** Fabric type where mlmodel belongs to or where it was generated */
  origin: FabricType;
  /** Ownership metadata of the mlmodel */
  ownership?: Maybe<Ownership>;
  /** Standardized platform urn where the MLModel is defined */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only information about the ML Model */
  properties?: Maybe<MlModelProperties>;
  /** Quantitative Analyses of the mlmodel */
  quantitativeAnalyses?: Maybe<QuantitativeAnalyses>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Source Code */
  sourceCode?: Maybe<SourceCode>;
  /** Status metadata of the mlmodel */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** The standard tags for the ML Model */
  tags?: Maybe<GlobalTags>;
  /** Training Data of the mlmodel */
  trainingData?: Maybe<Array<BaseData>>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the ML model */
  urn: Scalars['String'];
  versionProperties?: Maybe<VersionProperties>;
};


/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModelAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModelLineageArgs = {
  input: LineageInput;
};


/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModelRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** An ML Model Metadata Entity Note that this entity is incubating */
export type MlModelRelationshipsArgs = {
  input: RelationshipsInput;
};

export type MlModelEditableProperties = {
  __typename?: 'MLModelEditableProperties';
  /** The edited description */
  description?: Maybe<Scalars['String']>;
};

export type MlModelFactorPrompts = {
  __typename?: 'MLModelFactorPrompts';
  /** Which factors are being reported, and why were these chosen */
  evaluationFactors?: Maybe<Array<MlModelFactors>>;
  /** What are foreseeable salient factors for which MLModel performance may vary, and how were these determined */
  relevantFactors?: Maybe<Array<MlModelFactors>>;
};

export type MlModelFactors = {
  __typename?: 'MLModelFactors';
  /** Environment in which the MLModel is deployed */
  environment?: Maybe<Array<Scalars['String']>>;
  /** Distinct categories with similar characteristics that are present in the evaluation data instances */
  groups?: Maybe<Array<Scalars['String']>>;
  /** Instrumentation used for MLModel */
  instrumentation?: Maybe<Array<Scalars['String']>>;
};

/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroup = BrowsableEntity & Entity & EntityWithRelationships & {
  __typename?: 'MLModelGroup';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the ML Model Group. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Deprecation */
  deprecation?: Maybe<Deprecation>;
  /** Human readable description for MLModelGroup */
  description?: Maybe<Scalars['String']>;
  /** The Domain associated with the entity */
  domain?: Maybe<DomainAssociation>;
  /** An additional set of of read write properties */
  editableProperties?: Maybe<MlModelGroupEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the entity */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the ml model group */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** The display name for the Entity */
  name: Scalars['String'];
  /** Fabric type where MLModelGroup belongs to or where it was generated */
  origin: FabricType;
  /** Ownership metadata of the MLModelGroup */
  ownership?: Maybe<Ownership>;
  /** Standardized platform urn where the MLModelGroup is defined */
  platform: DataPlatform;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties about the ML Model Group */
  properties?: Maybe<MlModelGroupProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the MLModelGroup */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags applied to entity */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the ML Model Group */
  urn: Scalars['String'];
};


/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroupAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroupLineageArgs = {
  input: LineageInput;
};


/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroupRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/**
 * An ML Model Group Metadata Entity
 * Note that this entity is incubating
 */
export type MlModelGroupRelationshipsArgs = {
  input: RelationshipsInput;
};

export type MlModelGroupEditableProperties = {
  __typename?: 'MLModelGroupEditableProperties';
  /** The edited description */
  description?: Maybe<Scalars['String']>;
};

/** Properties describing a group of related ML models */
export type MlModelGroupProperties = {
  __typename?: 'MLModelGroupProperties';
  /** When this model group was created */
  created?: Maybe<AuditStamp>;
  /**
   * Deprecated creation timestamp
   * @deprecated Use the 'created' field instead
   * @deprecated Use `created` instead
   */
  createdAt?: Maybe<Scalars['Long']>;
  /** Custom key-value properties for the model group */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Detailed description of the model group's purpose and contents */
  description?: Maybe<Scalars['String']>;
  /** URL to view this model group in the external system */
  externalUrl?: Maybe<Scalars['String']>;
  /** When this model group was last modified */
  lastModified?: Maybe<AuditStamp>;
  /** Information related to lineage to this model group */
  mlModelLineageInfo?: Maybe<MlModelLineageInfo>;
  /** Display name of the model group */
  name?: Maybe<Scalars['String']>;
  /** Version identifier for this model group */
  version?: Maybe<VersionTag>;
};

/** Represents lineage information for ML entities. */
export type MlModelLineageInfo = {
  __typename?: 'MLModelLineageInfo';
  /** List of jobs or processes that use this model. */
  downstreamJobs?: Maybe<Array<Scalars['String']>>;
  /** List of jobs or processes used to train the model. */
  trainingJobs?: Maybe<Array<Scalars['String']>>;
};

export type MlModelProperties = {
  __typename?: 'MLModelProperties';
  /** When this model was created */
  created?: Maybe<AuditStamp>;
  /** Additional custom properties specific to this model */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /**
   * Deprecated timestamp for model creation
   * @deprecated Use 'created' field instead
   * @deprecated Use `created` instead
   */
  date?: Maybe<Scalars['Long']>;
  /** Detailed description of the model's purpose and characteristics */
  description?: Maybe<Scalars['String']>;
  /** URL to view this model in external system */
  externalUrl?: Maybe<Scalars['String']>;
  /** Model groups this model belongs to */
  groups?: Maybe<Array<Maybe<MlModelGroup>>>;
  /** Mapping of hyperparameter configurations */
  hyperParameters?: Maybe<HyperParameterMap>;
  /** List of hyperparameter settings used to train this model */
  hyperParams?: Maybe<Array<Maybe<MlHyperParam>>>;
  /** When the model was last modified */
  lastModified?: Maybe<AuditStamp>;
  /** Names of ML features used by this model */
  mlFeatures?: Maybe<Array<Scalars['String']>>;
  /** Information related to lineage to this model group */
  mlModelLineageInfo?: Maybe<MlModelLineageInfo>;
  /** The display name of the model used in the UI */
  name?: Maybe<Scalars['String']>;
  /** Tags for categorizing and searching models */
  tags?: Maybe<Array<Scalars['String']>>;
  /** Performance metrics from model training */
  trainingMetrics?: Maybe<Array<Maybe<MlMetric>>>;
  /** The type/category of ML model (e.g. classification, regression) */
  type?: Maybe<Scalars['String']>;
  /** Version identifier for this model */
  version?: Maybe<Scalars['String']>;
};

/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKey = Entity & EntityWithRelationships & {
  __typename?: 'MLPrimaryKey';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** MLPrimaryKey data type */
  dataType?: Maybe<MlFeatureDataType>;
  /** Deprecation */
  deprecation?: Maybe<Deprecation>;
  /** MLPrimaryKey description */
  description?: Maybe<Scalars['String']>;
  /** The Domain associated with the entity */
  domain?: Maybe<DomainAssociation>;
  /** An additional set of of read write properties */
  editableProperties?: Maybe<MlPrimaryKeyEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** MLPrimaryKey featureNamespace */
  featureNamespace: Scalars['String'];
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** The structured glossary terms associated with the entity */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** References to internal resources related to the MLPrimaryKey */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** The timestamp for the last time this entity was ingested */
  lastIngested?: Maybe<Scalars['Long']>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** The display name */
  name: Scalars['String'];
  /** Ownership metadata of the MLPrimaryKey */
  ownership?: Maybe<Ownership>;
  /**
   * Deprecated, use properties field instead
   * MLPrimaryKeyProperties
   * @deprecated Field no longer supported
   */
  primaryKeyProperties?: Maybe<MlPrimaryKeyProperties>;
  /** Privileges given to a user relevant to this entity */
  privileges?: Maybe<EntityPrivileges>;
  /** Additional read only properties of the ML Primary Key */
  properties?: Maybe<MlPrimaryKeyProperties>;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the MLPrimaryKey */
  status?: Maybe<Status>;
  /** Structured properties about this asset */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags applied to entity */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the ML Primary Key */
  urn: Scalars['String'];
};


/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKeyAspectsArgs = {
  input?: Maybe<AspectParams>;
};


/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKeyLineageArgs = {
  input: LineageInput;
};


/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKeyRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
};


/** An ML Primary Key Entity Note that this entity is incubating */
export type MlPrimaryKeyRelationshipsArgs = {
  input: RelationshipsInput;
};

export type MlPrimaryKeyEditableProperties = {
  __typename?: 'MLPrimaryKeyEditableProperties';
  /** The edited description */
  description?: Maybe<Scalars['String']>;
};

export type MlPrimaryKeyProperties = {
  __typename?: 'MLPrimaryKeyProperties';
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  dataType?: Maybe<MlFeatureDataType>;
  description?: Maybe<Scalars['String']>;
  sources?: Maybe<Array<Maybe<Dataset>>>;
  version?: Maybe<VersionTag>;
};

/** Properties specific to an ML model training run instance */
export type MlTrainingRunProperties = {
  __typename?: 'MLTrainingRunProperties';
  /** Hyperparameters used in this training run */
  hyperParams?: Maybe<Array<Maybe<MlHyperParam>>>;
  /** Unique identifier for this training run */
  id?: Maybe<Scalars['String']>;
  /** List of URLs to access training run outputs (e.g. model artifacts, logs) */
  outputUrls?: Maybe<Array<Maybe<Scalars['String']>>>;
  /** Performance metrics recorded during this training run */
  trainingMetrics?: Maybe<Array<Maybe<MlMetric>>>;
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
  /** Entity if the value is an urn */
  entity?: Maybe<Entity>;
  /** Name of the field that matched */
  name: Scalars['String'];
  /** Value of the field that matched */
  value: Scalars['String'];
};

/** Media content */
export type Media = {
  __typename?: 'Media';
  /** The location of the media (a URL) */
  location: Scalars['String'];
  /** The type of media */
  type: MediaType;
};

/** The type of media */
export enum MediaType {
  /** An image */
  Image = 'IMAGE'
}

/** Input to fetch metadata analytics charts */
export type MetadataAnalyticsInput = {
  /** Urn of the domain to fetch analytics for (If empty or GLOBAL, queries across all domains) */
  domain?: Maybe<Scalars['String']>;
  /** Entity type to fetch analytics for (If empty, queries across all entities) */
  entityType?: Maybe<EntityType>;
  /** Search query to filter down result (If empty, does not apply any search query) */
  query?: Maybe<Scalars['String']>;
};

/** Information about who, why, and how this metadata was applied */
export type MetadataAttribution = {
  __typename?: 'MetadataAttribution';
  /** The actor responsible for this metadata application */
  actor: Entity;
  /** The source of this metadata application. If propagated, this will be an action. */
  source?: Maybe<Entity>;
  /** Extra details about how this metadata was applied */
  sourceDetail?: Maybe<Array<StringMapEntry>>;
  /** The time this metadata was applied */
  time: Scalars['Long'];
};

export type Metrics = {
  __typename?: 'Metrics';
  /** Decision Thresholds used if any */
  decisionThreshold?: Maybe<Array<Scalars['String']>>;
  /** Measures of ML Model performance */
  performanceMeasures?: Maybe<Array<Scalars['String']>>;
};

/** Input required to move a Document to a different parent */
export type MoveDocumentInput = {
  /** Optional URN of the new parent document. If null, moves to root level. */
  parentDocument?: Maybe<Scalars['String']>;
  /** The URN of the Document to move */
  urn: Scalars['String'];
};

/** Input for updating the parent domain of a domain. */
export type MoveDomainInput = {
  /** The new parent domain urn. If parentDomain is null, this will remove the parent from this entity */
  parentDomain?: Maybe<Scalars['String']>;
  /** The primary key of the resource to update the parent domain for */
  resourceUrn: Scalars['String'];
};

/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type Mutation = {
  __typename?: 'Mutation';
  /** Accept role using invite token */
  acceptRole?: Maybe<Scalars['Boolean']>;
  /** Add Business Attribute */
  addBusinessAttribute?: Maybe<Scalars['Boolean']>;
  /** Add members to a group */
  addGroupMembers?: Maybe<Scalars['Boolean']>;
  /** Add a link, or institutional memory, for a particular asset */
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
  /**
   * Adds multiple assets to multiple data products (additive - preserves existing memberships).
   * Available when multipleDataProductsPerAsset feature flag is enabled.
   */
  batchAddToDataProducts?: Maybe<Scalars['Boolean']>;
  /**
   * Assign a form to different entities. This will be a patch by adding this form to the list
   * of forms on an entity.
   */
  batchAssignForm?: Maybe<Scalars['Boolean']>;
  /** Batch assign roles to users */
  batchAssignRole?: Maybe<Scalars['Boolean']>;
  /** Remove a form from a given list of entities. */
  batchRemoveForm: Scalars['Boolean'];
  /**
   * Removes multiple assets from multiple data products (does not affect other memberships).
   * Available when multipleDataProductsPerAsset feature flag is enabled.
   */
  batchRemoveFromDataProducts?: Maybe<Scalars['Boolean']>;
  /** Remove owners from multiple Entities */
  batchRemoveOwners?: Maybe<Scalars['Boolean']>;
  /** Remove tags from multiple Entities or subresource */
  batchRemoveTags?: Maybe<Scalars['Boolean']>;
  /** Remove glossary terms from multiple Entities or subresource */
  batchRemoveTerms?: Maybe<Scalars['Boolean']>;
  /** Batch set or unset a Application to a list of entities */
  batchSetApplication?: Maybe<Scalars['Boolean']>;
  /** Batch set or unset a DataProduct to a list of entities */
  batchSetDataProduct?: Maybe<Scalars['Boolean']>;
  /** Set domain for multiple Entities */
  batchSetDomain?: Maybe<Scalars['Boolean']>;
  /** Batch unset a specific Application from a list of entities */
  batchUnsetApplication?: Maybe<Scalars['Boolean']>;
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
  /** Create a new Application */
  createApplication?: Maybe<Application>;
  /** Create Business Attribute Api */
  createBusinessAttribute?: Maybe<BusinessAttribute>;
  /** Create a new DataHub file entity */
  createDataHubFile: CreateDataHubFileResponse;
  /** Create a new Data Product */
  createDataProduct?: Maybe<DataProduct>;
  /**
   * Create a new Document. Returns the urn of the newly created document.
   * Requires the CREATE_ENTITY privilege for documents or MANAGE_DOCUMENTS platform privilege.
   */
  createDocument: Scalars['String'];
  /**
   * Create a new Domain. Returns the urn of the newly created Domain. Requires the 'Create Domains' or 'Manage Domains' Platform Privilege. If a Domain with the provided ID already exists,
   * it will be overwritten.
   */
  createDomain?: Maybe<Scalars['String']>;
  /**
   * Creates a filter for a form to apply it to certain entities. Entities that match this filter will have
   * a given form applied to them.
   * This feature is ONLY supported in DataHub Cloud.
   */
  createDynamicFormAssignment?: Maybe<Scalars['Boolean']>;
  /** Create a ERModelRelationship */
  createERModelRelationship?: Maybe<ErModelRelationship>;
  /** Create a new form based on the input */
  createForm: Form;
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
  /** Create a Custom Ownership Type. This requires the 'Manage Ownership Types' Metadata Privilege. */
  createOwnershipType?: Maybe<OwnershipTypeEntity>;
  /** Create a policy and returns the resulting urn */
  createPolicy?: Maybe<Scalars['String']>;
  /** Create a post */
  createPost?: Maybe<Scalars['Boolean']>;
  /** Create a new Query */
  createQuery?: Maybe<QueryEntity>;
  /** Create a new Secret */
  createSecret?: Maybe<Scalars['String']>;
  /** Creates a new service account. */
  createServiceAccount: ServiceAccount;
  /** Create a new structured property */
  createStructuredProperty: StructuredPropertyEntity;
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
  /** Delete a Application by urn. */
  deleteApplication?: Maybe<Scalars['Boolean']>;
  /** Remove an assertion associated with an entity. Requires the 'Edit Assertions' privilege on the entity. */
  deleteAssertion?: Maybe<Scalars['Boolean']>;
  /** Delete a Business Attribute by urn. */
  deleteBusinessAttribute?: Maybe<Scalars['Boolean']>;
  /** Delete a DataProduct by urn. */
  deleteDataProduct?: Maybe<Scalars['Boolean']>;
  /**
   * Delete a Document.
   * Requires the GET_ENTITY privilege for the document or MANAGE_DOCUMENTS platform privilege.
   */
  deleteDocument: Scalars['Boolean'];
  /** Delete a Domain */
  deleteDomain?: Maybe<Scalars['Boolean']>;
  /** Delete a ERModelRelationship */
  deleteERModelRelationship?: Maybe<Scalars['Boolean']>;
  /** Delete a given form */
  deleteForm: Scalars['Boolean'];
  /** Remove a glossary entity (GlossaryTerm or GlossaryNode). Return boolean whether it was successful or not. */
  deleteGlossaryEntity?: Maybe<Scalars['Boolean']>;
  /** Delete an existing ingestion source */
  deleteIngestionSource?: Maybe<Scalars['String']>;
  /** Delete a Custom Ownership Type by urn. This requires the 'Manage Ownership Types' Metadata Privilege. */
  deleteOwnershipType?: Maybe<Scalars['Boolean']>;
  /** Delete a DataHub page module */
  deletePageModule: Scalars['Boolean'];
  /** Delete a DataHub page template */
  deletePageTemplate: Scalars['Boolean'];
  /** Remove an existing policy and returns the policy urn */
  deletePolicy?: Maybe<Scalars['String']>;
  /** Delete a post */
  deletePost?: Maybe<Scalars['Boolean']>;
  /** Delete a Query by urn. This requires the 'Edit Queries' Metadata Privilege. */
  deleteQuery?: Maybe<Scalars['Boolean']>;
  /** Delete a Secret */
  deleteSecret?: Maybe<Scalars['String']>;
  /** Deletes a service account by URN. */
  deleteServiceAccount: Scalars['Boolean'];
  /** Delete an existing structured property */
  deleteStructuredProperty: Scalars['Boolean'];
  /** Delete a Tag */
  deleteTag?: Maybe<Scalars['Boolean']>;
  /** Delete an existing test - note that this will NOT delete dangling pointers until the next execution of the test. */
  deleteTest?: Maybe<Scalars['Boolean']>;
  /** Delete a DataHub View (Saved Filter) */
  deleteView?: Maybe<Scalars['Boolean']>;
  /** Link the latest versioned entity to a Version Set */
  linkAssetVersion?: Maybe<VersionSet>;
  /**
   * Move a Document to a different parent (or to root level if no parent is specified).
   * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY privilege for the document, or MANAGE_DOCUMENTS platform privilege.
   */
  moveDocument: Scalars['Boolean'];
  /** Moves a domain to be parented under another domain. */
  moveDomain?: Maybe<Scalars['Boolean']>;
  patchEntities: Array<PatchEntityResult>;
  patchEntity: PatchEntityResult;
  /** Create a new incident for a resource (asset) */
  raiseIncident?: Maybe<Scalars['String']>;
  /** Remove Business Attribute */
  removeBusinessAttribute?: Maybe<Scalars['Boolean']>;
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
  /** Upsert structured properties onto a given asset */
  removeStructuredProperties: StructuredProperties;
  /** Remove a tag from a particular Entity or subresource */
  removeTag?: Maybe<Scalars['Boolean']>;
  /** Remove a glossary term from a particular Entity or subresource */
  removeTerm?: Maybe<Scalars['Boolean']>;
  /** Remove a user. Requires Manage Users & Groups Platform Privilege */
  removeUser?: Maybe<Scalars['Boolean']>;
  /** Report result for an assertion */
  reportAssertionResult: Scalars['Boolean'];
  /** Report a new operation for an asset */
  reportOperation?: Maybe<Scalars['String']>;
  /** Revokes access tokens. */
  revokeAccessToken: Scalars['Boolean'];
  /** Rollback a specific ingestion execution run based on its runId */
  rollbackIngestion?: Maybe<Scalars['String']>;
  /** Sets the Domain for a Dataset, Chart, Dashboard, Data Flow (Pipeline), or Data Job (Task). Returns true if the Domain was successfully added, or already exists. Requires the Edit Domains privilege for the Entity. */
  setDomain?: Maybe<Scalars['Boolean']>;
  /** Set or unset an entity's logical parent */
  setLogicalParent?: Maybe<Scalars['Boolean']>;
  /** Set the hex color associated with an existing Tag */
  setTagColor?: Maybe<Scalars['Boolean']>;
  /**
   * Submit a response to a prompt from a form collecting metadata on different entities.
   * Provide the urn of the entity you're submitting a form response as well as the required input.
   */
  submitFormPrompt?: Maybe<Scalars['Boolean']>;
  /** Unlink a versioned entity from a Version Set */
  unlinkAssetVersion?: Maybe<VersionSet>;
  /** Sets the Domain for a Dataset, Chart, Dashboard, Data Flow (Pipeline), or Data Job (Task). Returns true if the Domain was successfully removed, or was already removed. Requires the Edit Domains privilege for an asset. */
  unsetDomain?: Maybe<Scalars['Boolean']>;
  /** Update a Application */
  updateApplication?: Maybe<Application>;
  /** Update the applications settings. */
  updateApplicationsSettings: Scalars['Boolean'];
  /** Update an asset's settings */
  updateAssetSettings: AssetSettings;
  /** Update Business Attribute */
  updateBusinessAttribute?: Maybe<BusinessAttribute>;
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
  /** Update a Data Product */
  updateDataProduct?: Maybe<DataProduct>;
  /** Update the metadata about a particular Dataset */
  updateDataset?: Maybe<Dataset>;
  /** Update the metadata about a batch of Datasets */
  updateDatasets?: Maybe<Array<Maybe<Dataset>>>;
  /** Sets the Deprecation status for a Metadata Entity. Requires the Edit Deprecation status privilege for an entity. */
  updateDeprecation?: Maybe<Scalars['Boolean']>;
  /** Incubating. Updates the description of a resource. Currently only supports Dataset Schema Fields, Containers */
  updateDescription?: Maybe<Scalars['Boolean']>;
  /** Update a particular entity's display properties */
  updateDisplayProperties?: Maybe<Scalars['Boolean']>;
  /** Update the doc propagation settings. */
  updateDocPropagationSettings: Scalars['Boolean'];
  /**
   * Update the title or text of an existing Document.
   * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY privilege for the document, or MANAGE_DOCUMENTS platform privilege.
   */
  updateDocumentContents: Scalars['Boolean'];
  /**
   * Update the related entities (assets and documents) for a Document.
   * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY privilege for the document, or MANAGE_DOCUMENTS platform privilege.
   */
  updateDocumentRelatedEntities: Scalars['Boolean'];
  /**
   * Update the settings of a Document (e.g., visibility in sidebar).
   * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY privilege for the document, or MANAGE_DOCUMENTS platform privilege.
   */
  updateDocumentSettings: Scalars['Boolean'];
  /**
   * Update the status of a Document (published/unpublished).
   * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY privilege for the document, or MANAGE_DOCUMENTS platform privilege.
   */
  updateDocumentStatus: Scalars['Boolean'];
  /**
   * Update the sub-type of a Document (e.g., "FAQ", "Tutorial", "Runbook").
   * Requires the EDIT_ENTITY_DOCS or EDIT_ENTITY privilege for the document, or MANAGE_DOCUMENTS platform privilege.
   */
  updateDocumentSubType: Scalars['Boolean'];
  /** Update a ERModelRelationship */
  updateERModelRelationship?: Maybe<Scalars['Boolean']>;
  /** Update the Embed information for a Dataset, Dashboard, or Chart. */
  updateEmbed?: Maybe<Scalars['Boolean']>;
  /** Update an existing form based on the input */
  updateForm: Form;
  /**
   * Update the global settings related to the Views feature.
   * Requires the 'Manage Global Views' Platform Privilege.
   */
  updateGlobalViewsSettings: Scalars['Boolean'];
  /** Update an existing data incident. Any fields that are omitted will simply not be updated. */
  updateIncident?: Maybe<Scalars['Boolean']>;
  /** Update the status of an existing incident for a resource (asset) */
  updateIncidentStatus?: Maybe<Scalars['Boolean']>;
  /** Update an existing ingestion source */
  updateIngestionSource?: Maybe<Scalars['String']>;
  /** Update lineage for an entity */
  updateLineage?: Maybe<Scalars['Boolean']>;
  /**
   * Update a link, or institutional memory, for a particular asset.
   * A combo of link label and url create a unique identifier for which link to update.
   */
  updateLink?: Maybe<Scalars['Boolean']>;
  /** Updates the name of the entity. */
  updateName?: Maybe<Scalars['Boolean']>;
  /** Update the metadata about a particular Notebook */
  updateNotebook?: Maybe<Notebook>;
  /** Update an existing Query. This requires the 'Manage Ownership Types' Metadata Privilege. */
  updateOwnershipType?: Maybe<OwnershipTypeEntity>;
  /** Updates the parent node of a resource. Currently only GlossaryNodes and GlossaryTerms have parentNodes. */
  updateParentNode?: Maybe<Scalars['Boolean']>;
  /** Update an existing policy and returns the resulting urn */
  updatePolicy?: Maybe<Scalars['String']>;
  /** Update or edit a post */
  updatePost?: Maybe<Scalars['Boolean']>;
  /** Update an existing Query */
  updateQuery?: Maybe<QueryEntity>;
  /** Update a Secret */
  updateSecret?: Maybe<Scalars['String']>;
  /** Update an existing structured property */
  updateStructuredProperty: StructuredPropertyEntity;
  /** Update the information about a particular Entity Tag */
  updateTag?: Maybe<Tag>;
  /** Update an existing test */
  updateTest?: Maybe<Scalars['String']>;
  /** Update the HomePage-related settings for a user. */
  updateUserHomePageSettings?: Maybe<Scalars['Boolean']>;
  /** Update a user setting */
  updateUserSetting?: Maybe<Scalars['Boolean']>;
  /** Change the status of a user. Requires Manage Users & Groups Platform Privilege */
  updateUserStatus?: Maybe<Scalars['String']>;
  /** Delete a DataHub View (Saved Filter) */
  updateView?: Maybe<DataHubView>;
  /**
   * Upsert a particular connection.
   * This requires the 'Manage Connections' platform privilege.
   */
  upsertConnection: DataHubConnection;
  /** Upsert a Custom Assertion */
  upsertCustomAssertion: Assertion;
  /** Create or update a data contract for a given dataset. Requires the "Edit Data Contract" privilege for the provided dataset. */
  upsertDataContract: DataContract;
  /**
   * Upsert a link, or institutional memory, for a particular asset.
   * A combo of link label and url create a unique identifier to update a link,
   * or add a new link if no links with label/url combo are found.
   */
  upsertLink?: Maybe<Scalars['Boolean']>;
  /** Create or update a DataHub page module */
  upsertPageModule: DataHubPageModule;
  /** Batch update the state for a set of steps. */
  upsertPageTemplate: DataHubPageTemplate;
  /** Upsert structured properties onto a given asset */
  upsertStructuredProperties: StructuredProperties;
  /**
   * Verifies a form on an entity when all of the required questions on the form are complete and the form
   * is of type VERIFICATION.
   */
  verifyForm?: Maybe<Scalars['Boolean']>;
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
export type MutationAddBusinessAttributeArgs = {
  input: AddBusinessAttributeInput;
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
export type MutationBatchAddToDataProductsArgs = {
  input: BatchSetDataProductsInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchAssignFormArgs = {
  input: BatchAssignFormInput;
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
export type MutationBatchRemoveFormArgs = {
  input: BatchRemoveFormInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchRemoveFromDataProductsArgs = {
  input: BatchSetDataProductsInput;
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
export type MutationBatchSetApplicationArgs = {
  input: BatchSetApplicationInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationBatchSetDataProductArgs = {
  input: BatchSetDataProductInput;
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
export type MutationBatchUnsetApplicationArgs = {
  input: BatchUnsetApplicationInput;
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
export type MutationCreateApplicationArgs = {
  input: CreateApplicationInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateBusinessAttributeArgs = {
  input: CreateBusinessAttributeInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateDataHubFileArgs = {
  input: CreateDataHubFileInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateDataProductArgs = {
  input: CreateDataProductInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateDocumentArgs = {
  input: CreateDocumentInput;
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
export type MutationCreateDynamicFormAssignmentArgs = {
  input: CreateDynamicFormAssignmentInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateErModelRelationshipArgs = {
  input: ErModelRelationshipUpdateInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateFormArgs = {
  input: CreateFormInput;
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
export type MutationCreateOwnershipTypeArgs = {
  input: CreateOwnershipTypeInput;
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
export type MutationCreateServiceAccountArgs = {
  input: CreateServiceAccountInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationCreateStructuredPropertyArgs = {
  input: CreateStructuredPropertyInput;
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
export type MutationDeleteApplicationArgs = {
  urn: Scalars['String'];
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
export type MutationDeleteBusinessAttributeArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteDataProductArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteDocumentArgs = {
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
export type MutationDeleteErModelRelationshipArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteFormArgs = {
  input: DeleteFormInput;
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
export type MutationDeleteOwnershipTypeArgs = {
  deleteReferences?: Maybe<Scalars['Boolean']>;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeletePageModuleArgs = {
  input: DeletePageModuleInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeletePageTemplateArgs = {
  input: DeletePageTemplateInput;
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
export type MutationDeleteServiceAccountArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationDeleteStructuredPropertyArgs = {
  input: DeleteStructuredPropertyInput;
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
export type MutationLinkAssetVersionArgs = {
  input: LinkVersionInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationMoveDocumentArgs = {
  input: MoveDocumentInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationMoveDomainArgs = {
  input: MoveDomainInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationPatchEntitiesArgs = {
  input: Array<PatchEntityInput>;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationPatchEntityArgs = {
  input: PatchEntityInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRaiseIncidentArgs = {
  input: RaiseIncidentInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationRemoveBusinessAttributeArgs = {
  input: AddBusinessAttributeInput;
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
export type MutationRemoveStructuredPropertiesArgs = {
  input: RemoveStructuredPropertiesInput;
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
export type MutationReportAssertionResultArgs = {
  result: AssertionResultInput;
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
  domainUrn: Scalars['String'];
  entityUrn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationSetLogicalParentArgs = {
  input: SetLogicalParentInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationSetTagColorArgs = {
  colorHex: Scalars['String'];
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationSubmitFormPromptArgs = {
  input: SubmitFormPromptInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUnlinkAssetVersionArgs = {
  input: UnlinkVersionInput;
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
export type MutationUpdateApplicationArgs = {
  input: UpdateApplicationInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateApplicationsSettingsArgs = {
  input: UpdateApplicationsSettingsInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateAssetSettingsArgs = {
  input: UpdateAssetSettingsInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateBusinessAttributeArgs = {
  input: UpdateBusinessAttributeInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateChartArgs = {
  input: ChartUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateCorpGroupPropertiesArgs = {
  input: CorpGroupUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateCorpUserPropertiesArgs = {
  input: CorpUserUpdateInput;
  urn: Scalars['String'];
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
  input: DashboardUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDataFlowArgs = {
  input: DataFlowUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDataJobArgs = {
  input: DataJobUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDataProductArgs = {
  input: UpdateDataProductInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDatasetArgs = {
  input: DatasetUpdateInput;
  urn: Scalars['String'];
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
export type MutationUpdateDisplayPropertiesArgs = {
  input: DisplayPropertiesUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDocPropagationSettingsArgs = {
  input: UpdateDocPropagationSettingsInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDocumentContentsArgs = {
  input: UpdateDocumentContentsInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDocumentRelatedEntitiesArgs = {
  input: UpdateDocumentRelatedEntitiesInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDocumentSettingsArgs = {
  input: UpdateDocumentSettingsInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDocumentStatusArgs = {
  input: UpdateDocumentStatusInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateDocumentSubTypeArgs = {
  input: UpdateDocumentSubTypeInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateErModelRelationshipArgs = {
  input: ErModelRelationshipUpdateInput;
  urn: Scalars['String'];
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
export type MutationUpdateFormArgs = {
  input: UpdateFormInput;
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
export type MutationUpdateIncidentArgs = {
  input: UpdateIncidentInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateIncidentStatusArgs = {
  input: IncidentStatusInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateIngestionSourceArgs = {
  input: UpdateIngestionSourceInput;
  urn: Scalars['String'];
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
export type MutationUpdateLinkArgs = {
  input: UpdateLinkInput;
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
  input: NotebookUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateOwnershipTypeArgs = {
  input: UpdateOwnershipTypeInput;
  urn: Scalars['String'];
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
  input: PolicyUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdatePostArgs = {
  input: UpdatePostInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateQueryArgs = {
  input: UpdateQueryInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateSecretArgs = {
  input: UpdateSecretInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateStructuredPropertyArgs = {
  input: UpdateStructuredPropertyInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateTagArgs = {
  input: TagUpdateInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateTestArgs = {
  input: UpdateTestInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateUserHomePageSettingsArgs = {
  input: UpdateUserHomePageSettingsInput;
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
  status: CorpUserStatus;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpdateViewArgs = {
  input: UpdateViewInput;
  urn: Scalars['String'];
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertConnectionArgs = {
  input: UpsertDataHubConnectionInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertCustomAssertionArgs = {
  input: UpsertCustomAssertionInput;
  urn?: Maybe<Scalars['String']>;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertDataContractArgs = {
  input: UpsertDataContractInput;
  urn?: Maybe<Scalars['String']>;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertLinkArgs = {
  input: UpsertLinkInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertPageModuleArgs = {
  input: UpsertPageModuleInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertPageTemplateArgs = {
  input: UpsertPageTemplateInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationUpsertStructuredPropertiesArgs = {
  input: UpsertStructuredPropertiesInput;
};


/**
 * Root type used for updating DataHub Metadata
 * Coming soon createEntity, addOwner, removeOwner mutations
 */
export type MutationVerifyFormArgs = {
  input: VerifyFormInput;
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
  data: Array<NumericDataPoint>;
  name: Scalars['String'];
};

/** A Notebook Metadata Entity */
export type Notebook = BrowsableEntity & Entity & {
  __typename?: 'Notebook';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /** The browse path V2 corresponding to an entity. If no Browse Paths V2 have been generated before, this will be null. */
  browsePathV2?: Maybe<BrowsePathV2>;
  /** The browse paths corresponding to the Notebook. If no Browse Paths have been generated before, this will be null. */
  browsePaths?: Maybe<Array<BrowsePath>>;
  /** The content of this Notebook */
  content: NotebookContent;
  /** The specific instance of the data platform that this entity belongs to */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** The Domain associated with the Notebook */
  domain?: Maybe<DomainAssociation>;
  /** Additional read write properties about the Notebook */
  editableProperties?: Maybe<NotebookEditableProperties>;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** The structured glossary terms associated with the notebook */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Additional read only information about the Notebook */
  info?: Maybe<NotebookInfo>;
  /** References to internal resources related to the Notebook */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /** An id unique within the Notebook tool */
  notebookId: Scalars['String'];
  /** Ownership metadata of the Notebook */
  ownership?: Maybe<Ownership>;
  /** Standardized platform. */
  platform: DataPlatform;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status metadata of the Notebook */
  status?: Maybe<Status>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** The tags associated with the Notebook */
  tags?: Maybe<GlobalTags>;
  /** The Notebook tool name */
  tool: Scalars['String'];
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the Notebook */
  urn: Scalars['String'];
};


/** A Notebook Metadata Entity */
export type NotebookAspectsArgs = {
  input?: Maybe<AspectParams>;
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
  /** The query cell content. The will be non-null only when all other cell field is null. */
  queryChell?: Maybe<QueryCell>;
  /** The text cell content. The will be non-null only when all other cell field is null. */
  textCell?: Maybe<TextCell>;
  /** The type of this Notebook cell */
  type: NotebookCellType;
};

/** The type for a NotebookCell */
export enum NotebookCellType {
  /** CHART Notebook cell type. The cell content is chart only. */
  ChartCell = 'CHART_CELL',
  /** QUERY Notebook cell type. The cell context is query only. */
  QueryCell = 'QUERY_CELL',
  /** TEXT Notebook cell type. The cell context is text only. */
  TextCell = 'TEXT_CELL'
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
  /** Captures information about who created/last modified/deleted this Notebook and when */
  changeAuditStamps?: Maybe<ChangeAuditStamps>;
  /** A list of platform specific metadata tuples */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** Description of the Notebook */
  description?: Maybe<Scalars['String']>;
  /** Native platform URL of the Notebook */
  externalUrl?: Maybe<Scalars['String']>;
  /** Display of the Notebook */
  title?: Maybe<Scalars['String']>;
};

/** Arguments provided to update a Notebook Entity */
export type NotebookUpdateInput = {
  /** Update to editable properties */
  editableProperties?: Maybe<NotebookEditablePropertiesUpdate>;
  /** Update to ownership */
  ownership?: Maybe<OwnershipUpdate>;
  /** Update to tags */
  tags?: Maybe<GlobalTagsUpdate>;
};

/** Numeric property value */
export type NumberValue = {
  __typename?: 'NumberValue';
  /** The value of a number type property */
  numberValue: Scalars['Float'];
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
  /** Actor who issued this operation. */
  actor?: Maybe<Scalars['String']>;
  /** Which other datasets were affected by this operation. */
  affectedDatasets?: Maybe<Array<Scalars['String']>>;
  /** A custom operation type */
  customOperationType?: Maybe<Scalars['String']>;
  /** Custom operation properties */
  customProperties?: Maybe<Array<StringMapEntry>>;
  /** When time at which the asset was actually updated */
  lastUpdatedTimestamp: Scalars['Long'];
  /** How many rows were affected by this operation. */
  numAffectedRows?: Maybe<Scalars['Long']>;
  /** Operation type of change. */
  operationType: OperationType;
  /** Optional partition identifier */
  partition?: Maybe<Scalars['String']>;
  /** Source of the operation */
  sourceType?: Maybe<OperationSourceType>;
  /** The time at which the operation was reported */
  timestampMillis: Scalars['Long'];
};

/** Enum to define the source/reporter type for an Operation. */
export enum OperationSourceType {
  /** A data platform reported the operation. */
  DataPlatform = 'DATA_PLATFORM',
  /** A data process reported the operation. */
  DataProcess = 'DATA_PROCESS'
}

/** Enum to define the operation type when an entity changes. */
export enum OperationType {
  /** When table is altered */
  Alter = 'ALTER',
  /** When table is created. */
  Create = 'CREATE',
  /** Custom */
  Custom = 'CUSTOM',
  /** When data is deleted. */
  Delete = 'DELETE',
  /** When table is dropped */
  Drop = 'DROP',
  /** When data is inserted. */
  Insert = 'INSERT',
  /** Unknown operation */
  Unknown = 'UNKNOWN',
  /** When data is updated. */
  Update = 'UPDATE'
}

/** An aggregation of Dataset operations statistics */
export type OperationsAggregation = {
  __typename?: 'OperationsAggregation';
  /** The rolled up operations metrics */
  aggregations?: Maybe<OperationsAggregationsResult>;
  /** The time window start time */
  bucket?: Maybe<Scalars['Long']>;
  /** The time window span */
  duration?: Maybe<WindowDuration>;
  /** The resource urn associated with the operations information, eg a Dataset urn */
  resource?: Maybe<Scalars['String']>;
};

/** Rolled up metrics about Dataset operations over time */
export type OperationsAggregationsResult = {
  __typename?: 'OperationsAggregationsResult';
  /** A map from each custom operation type to the total count for that type */
  customOperationsMap?: Maybe<Array<IntMapEntry>>;
  /** The total number of ALTER operations performed within the queried time range */
  totalAlters?: Maybe<Scalars['Int']>;
  /** The total number of CREATE operations performed within the queried time range */
  totalCreates?: Maybe<Scalars['Int']>;
  /** The total number of CUSTOM operations performed within the queried time range */
  totalCustoms?: Maybe<Scalars['Int']>;
  /** The total number of DELETE operations performed within the queried time range */
  totalDeletes?: Maybe<Scalars['Int']>;
  /** The total number of DROP operations performed within the queried time range */
  totalDrops?: Maybe<Scalars['Int']>;
  /** The total number of INSERT operations performed within the queried time range */
  totalInserts?: Maybe<Scalars['Int']>;
  /** The total number of operations performed within the queried time range */
  totalOperations?: Maybe<Scalars['Int']>;
  /** The total number of UPDATE operations performed within the queried time range */
  totalUpdates?: Maybe<Scalars['Int']>;
};

/** The result of a Dataset operations query */
export type OperationsQueryResult = {
  __typename?: 'OperationsQueryResult';
  /** A set of rolled up aggregations about the Dataset operations */
  aggregations?: Maybe<OperationsAggregationsResult>;
  /** A set of relevant time windows for use in displaying operations */
  buckets?: Maybe<Array<Maybe<OperationsAggregation>>>;
};

export type OperationsStatsInput = {
  /** The time range you want to get operations stats for */
  range?: Maybe<TimeRange>;
  /** The optional time zone for aggregating stats. Accepts standard IANA time zone identifier ie. America/New_York */
  timeZone?: Maybe<Scalars['String']>;
};

/** Carries information about where an entity originated from. */
export type Origin = {
  __typename?: 'Origin';
  /** Only populated if type is EXTERNAL. The externalType of the entity, such as the name of the identity provider. */
  externalType?: Maybe<Scalars['String']>;
  /** Where an entity originated from. Either NATIVE or EXTERNAL */
  type: OriginType;
};

/** Enum to define where an entity originated from. */
export enum OriginType {
  /** The entity is external to DataHub. */
  External = 'EXTERNAL',
  /** The entity is native to DataHub. */
  Native = 'NATIVE',
  /** The entity is of unknown origin. */
  Unknown = 'UNKNOWN'
}

/** An owner of a Metadata Entity */
export type Owner = {
  __typename?: 'Owner';
  /** Reference back to the owned urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** Information about who, why, and how this metadata was applied */
  attribution?: Maybe<MetadataAttribution>;
  /** Owner object */
  owner: OwnerType;
  /** Ownership type information */
  ownershipType?: Maybe<OwnershipTypeEntity>;
  /** Source information for the ownership */
  source?: Maybe<OwnershipSource>;
  /**
   * The type of the ownership. Deprecated - Use ownershipType field instead.
   * @deprecated Field no longer supported
   */
  type?: Maybe<OwnershipType>;
};

/** Entities that are able to own other entities */
export enum OwnerEntityType {
  /** A corp group owner */
  CorpGroup = 'CORP_GROUP',
  /** A corp user owner */
  CorpUser = 'CORP_USER'
}

/** Input provided when adding an owner to an asset */
export type OwnerInput = {
  /** The owner type, either a user or group */
  ownerEntityType: OwnerEntityType;
  /** The primary key of the Owner to add or remove */
  ownerUrn: Scalars['String'];
  /** The urn of the ownership type entity. */
  ownershipTypeUrn?: Maybe<Scalars['String']>;
  /**
   * The ownership type for the new owner. If none is provided, then a new NONE will be added.
   * Deprecated - Use ownershipTypeUrn field instead.
   */
  type?: Maybe<OwnershipType>;
};

/** An owner of a Metadata Entity, either a user or group */
export type OwnerType = CorpGroup | CorpUser;

/**
 * An owner to add to a Metadata Entity
 * TODO Add a USER or GROUP actor enum
 */
export type OwnerUpdate = {
  /** The owner URN, either a corpGroup or corpuser */
  owner: Scalars['String'];
  /** The urn of the ownership type entity. */
  ownershipTypeUrn?: Maybe<Scalars['String']>;
  /** The owner type. Deprecated - Use ownershipTypeUrn field instead. */
  type?: Maybe<OwnershipType>;
};

/** Ownership information about a Metadata Entity */
export type Ownership = {
  __typename?: 'Ownership';
  /** Audit stamp containing who last modified the record and when */
  lastModified: AuditStamp;
  /** List of owners of the entity */
  owners?: Maybe<Array<Owner>>;
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
  /** Other sources */
  Other = 'OTHER',
  /** Other ownership like service, eg Nuage, ACL service etc */
  Service = 'SERVICE',
  /** SCM system, eg GIT, SVN */
  SourceControl = 'SOURCE_CONTROL'
}

/**
 * The type of the ownership relationship between a Person and a Metadata Entity
 * Note that this field will soon become deprecated due to low usage
 */
export enum OwnershipType {
  /** A person or group who is responsible for logical, or business related, aspects of the asset. */
  BusinessOwner = 'BUSINESS_OWNER',
  /**
   * A person, group, or service that consumes the data
   * Deprecated! This ownership type is no longer supported.
   */
  Consumer = 'CONSUMER',
  /** Associated ownership type is a custom ownership type. Please check OwnershipTypeEntity urn for custom value. */
  Custom = 'CUSTOM',
  /**
   * A person or group that owns the data.
   * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
   */
  Dataowner = 'DATAOWNER',
  /** A steward, expert, or delegate responsible for the asset. */
  DataSteward = 'DATA_STEWARD',
  /**
   * A person or a group that overseas the operation, eg a DBA or SRE
   * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
   */
  Delegate = 'DELEGATE',
  /**
   * A person or group that is in charge of developing the code
   * Deprecated! This ownership type is no longer supported. Use TECHNICAL_OWNER instead.
   */
  Developer = 'DEVELOPER',
  /** No specific type associated with the owner. */
  None = 'NONE',
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
  /** A person or group who is responsible for technical aspects of the asset. */
  TechnicalOwner = 'TECHNICAL_OWNER'
}

/** A single Custom Ownership Type */
export type OwnershipTypeEntity = Entity & {
  __typename?: 'OwnershipTypeEntity';
  /** Information about the Custom Ownership Type */
  info?: Maybe<OwnershipTypeInfo>;
  /** Granular API for querying edges extending from the Custom Ownership Type */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Status of the Custom Ownership Type */
  status?: Maybe<Status>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the custom ownership type. */
  urn: Scalars['String'];
};


/** A single Custom Ownership Type */
export type OwnershipTypeEntityRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Properties about an individual Custom Ownership Type. */
export type OwnershipTypeInfo = {
  __typename?: 'OwnershipTypeInfo';
  /** An Audit Stamp corresponding to the creation of this resource */
  created?: Maybe<AuditStamp>;
  /** The description of the Custom Ownership Type */
  description?: Maybe<Scalars['String']>;
  /** An Audit Stamp corresponding to the update of this resource */
  lastModified?: Maybe<AuditStamp>;
  /** The name of the Custom Ownership Type */
  name: Scalars['String'];
};

/** An update for the ownership information for a Metadata Entity */
export type OwnershipUpdate = {
  /** The updated list of owners */
  owners: Array<OwnerUpdate>;
};

/** Input for the specific parameters stored for a module */
export type PageModuleParamsInput = {
  /** The params required if the module is type ASSET_COLLECTION */
  assetCollectionParams?: Maybe<AssetCollectionModuleParamsInput>;
  /** The params required if the module is type HIERARCHY_VIEW */
  hierarchyViewParams?: Maybe<HierarchyViewModuleParamsInput>;
  /** The params required if the module is type LINK */
  linkParams?: Maybe<LinkModuleParamsInput>;
  /** The params required if the module is type RICH_TEXT */
  richTextParams?: Maybe<RichTextModuleParamsInput>;
};

/** Different scopes for where this module is relevant */
export enum PageModuleScope {
  /** This module is used across users */
  Global = 'GLOBAL',
  /** This module is used for individual use only */
  Personal = 'PERSONAL'
}

/** The optional info for asset summaries on this template */
export type PageTemplateAssetSummaryInput = {
  /** A list of summary element objects for what to store on asset summaries */
  summaryElements: Array<SummaryElementInput>;
};

/** Input a row of a page template */
export type PageTemplateRowInput = {
  /** A list of Page Module urns that comprise this row */
  modules: Array<Scalars['String']>;
};

/** Different scopes for where this template is relevant */
export enum PageTemplateScope {
  /** This template is used across users */
  Global = 'GLOBAL',
  /** This template is used for individual use only */
  Personal = 'PERSONAL'
}

/** Different surface areas for a page template */
export enum PageTemplateSurfaceType {
  /** This template applies to what to display on asset summary pages */
  AssetSummary = 'ASSET_SUMMARY',
  /** This template applies to what to display on the home page for users. */
  HomePage = 'HOME_PAGE'
}

/** All of the parent containers for a given entity. Returns parents with direct parent first followed by the parent's parent etc. */
export type ParentContainersResult = {
  __typename?: 'ParentContainersResult';
  /** A list of parent containers in order from direct parent, to parent's parent etc. If there are no containers, return an emty list */
  containers: Array<Container>;
  /** The number of containers bubbling up for this entity */
  count: Scalars['Int'];
};

/** All of the parent documents for a given document. Returns parents with direct parent first followed by the parent's parent, etc. */
export type ParentDocumentsResult = {
  __typename?: 'ParentDocumentsResult';
  /** The number of parent documents bubbling up for this document */
  count: Scalars['Int'];
  /** The ordered list of parent documents, starting with the direct parent */
  documents: Array<Document>;
};

/** All of the parent domains starting from a single Domain through all of its ancestors */
export type ParentDomainsResult = {
  __typename?: 'ParentDomainsResult';
  /** The number of parent domains bubbling up for this entity */
  count: Scalars['Int'];
  /** A list of parent domains in order from direct parent, to parent's parent etc. If there are no parents, return an empty list */
  domains: Array<Entity>;
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
  /** The partition identifier */
  partition?: Maybe<Scalars['String']>;
  /** The optional time window partition information - required if type is TIMESTAMP_FIELD. */
  timePartition?: Maybe<TimeWindow>;
  /** The partition type */
  type: PartitionType;
};

export enum PartitionType {
  FullTable = 'FULL_TABLE',
  Partition = 'PARTITION',
  Query = 'QUERY'
}

export type PatchEntityInput = {
  arrayPrimaryKeys?: Maybe<Array<ArrayPrimaryKeyInput>>;
  aspectName: Scalars['String'];
  entityType?: Maybe<Scalars['String']>;
  forceGenericPatch?: Maybe<Scalars['Boolean']>;
  headers?: Maybe<Array<StringMapEntryInput>>;
  patch: Array<PatchOperationInput>;
  systemMetadata?: Maybe<SystemMetadataInput>;
  urn?: Maybe<Scalars['String']>;
};

export type PatchEntityResult = {
  __typename?: 'PatchEntityResult';
  error?: Maybe<Scalars['String']>;
  name?: Maybe<Scalars['String']>;
  success: Scalars['Boolean'];
  urn: Scalars['String'];
};

export type PatchOperationInput = {
  from?: Maybe<Scalars['String']>;
  op: PatchOperationType;
  path: Scalars['String'];
  value?: Maybe<Scalars['String']>;
};

export enum PatchOperationType {
  Add = 'ADD',
  Copy = 'COPY',
  Move = 'MOVE',
  Remove = 'REMOVE',
  Replace = 'REPLACE',
  Test = 'TEST'
}

/** Variants of APIs used in the Search bar to get data */
export enum PersonalSidebarSection {
  /** The section containing assets you own */
  YourAssets = 'YOUR_ASSETS',
  /** The section containing domains you own */
  YourDomains = 'YOUR_DOMAINS',
  /** The section containing glossary nodes you own */
  YourGlossaryNodes = 'YOUR_GLOSSARY_NODES',
  /** The section containing groups you are in */
  YourGroups = 'YOUR_GROUPS',
  /** The section containing tags you own */
  YourTags = 'YOUR_TAGS'
}

/** Input representing A Data Platform */
export type PlatformInput = {
  /** Name of platform */
  name?: Maybe<Scalars['String']>;
  /** Urn of platform */
  urn?: Maybe<Scalars['String']>;
};

/**
 * Deprecated, do not use this type
 * The logical type associated with an individual Dataset
 */
export enum PlatformNativeType {
  /** Bucket in key value store */
  Bucket = 'BUCKET',
  /** Directory in file system */
  Directory = 'DIRECTORY',
  /** Stream */
  Stream = 'STREAM',
  /** Table */
  Table = 'TABLE',
  /** View */
  View = 'VIEW'
}

/** The platform privileges that the currently authenticated user has */
export type PlatformPrivileges = {
  __typename?: 'PlatformPrivileges';
  /** Whether the user can create Business Attributes. */
  createBusinessAttributes: Scalars['Boolean'];
  /** Whether the user should be able to create new Domains */
  createDomains: Scalars['Boolean'];
  /** Whether the user should be able to create new Tags */
  createTags: Scalars['Boolean'];
  /** Whether the user should be able to generate personal access tokens */
  generatePersonalAccessTokens: Scalars['Boolean'];
  /** Whether the user can manage applications. */
  manageApplications: Scalars['Boolean'];
  /** Whether the user can manage Business Attributes. */
  manageBusinessAttributes: Scalars['Boolean'];
  /** Whether the user should be able to manage Documents (Knowledge Articles) */
  manageDocuments: Scalars['Boolean'];
  /** Whether the user should be able to manage Domains */
  manageDomains: Scalars['Boolean'];
  /** Whether the user can manage platform features. */
  manageFeatures: Scalars['Boolean'];
  /** Whether the user can create and delete posts pinned to the home page. */
  manageGlobalAnnouncements: Scalars['Boolean'];
  /** Whether the user should be able to create, update, and delete global views. */
  manageGlobalViews: Scalars['Boolean'];
  /** Whether the user should be able to manage Glossaries */
  manageGlossaries: Scalars['Boolean'];
  /** Whether the user can manage default home page template. */
  manageHomePageTemplates: Scalars['Boolean'];
  /** Whether the user should be able to manage users & groups */
  manageIdentities: Scalars['Boolean'];
  /** Whether the user is able to manage UI-based ingestion */
  manageIngestion: Scalars['Boolean'];
  /** Whether the user should be able to create, update, and delete ownership types. */
  manageOwnershipTypes: Scalars['Boolean'];
  /** Whether the user should be able to manage policies */
  managePolicies: Scalars['Boolean'];
  /** Whether the user is able to manage UI-based secrets */
  manageSecrets: Scalars['Boolean'];
  /** Whether the user can create, update, and delete service accounts. */
  manageServiceAccounts: Scalars['Boolean'];
  /** Whether the user can create, edit, and delete structured properties. */
  manageStructuredProperties: Scalars['Boolean'];
  /** Whether the user should be able to create and delete all Tags */
  manageTags: Scalars['Boolean'];
  /** Whether the user is able to manage Tests */
  manageTests: Scalars['Boolean'];
  /** Whether the user should be able to manage tokens on behalf of other users. */
  manageTokens: Scalars['Boolean'];
  /** Whether the user is able to manage user credentials */
  manageUserCredentials: Scalars['Boolean'];
  /** Whether the user should be able to view analytics */
  viewAnalytics: Scalars['Boolean'];
  /** Whether the user should be able to view the tags management page. */
  viewManageTags: Scalars['Boolean'];
  /** Whether the user can view the manage structured properties page. */
  viewStructuredPropertiesPage: Scalars['Boolean'];
  /** Whether the user is able to view Tests */
  viewTests: Scalars['Boolean'];
};

/** A type of Schema, either a table schema or a key value schema */
export type PlatformSchema = KeyValueSchema | TableSchema;

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
  /** Value for other platforms */
  Others = 'OTHERS',
  /** Value for a query engine */
  QueryEngine = 'QUERY_ENGINE',
  /** Value for a relational database */
  RelationalDb = 'RELATIONAL_DB',
  /** Value for a search engine */
  SearchEngine = 'SEARCH_ENGINE'
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
  /** The actors that the Policy grants privileges to */
  actors: ActorFilter;
  /** The description of the Policy */
  description?: Maybe<Scalars['String']>;
  /** Whether the Policy is editable, ie system policies, or not */
  editable: Scalars['Boolean'];
  /** The name of the Policy */
  name: Scalars['String'];
  /** The privileges that the Policy grants */
  privileges: Array<Scalars['String']>;
  /** The resources that the Policy privileges apply to */
  resources?: Maybe<ResourceFilter>;
  /** The present state of the Policy */
  state: PolicyState;
  /** The type of the Policy */
  type: PolicyType;
  /** The primary key of the Policy */
  urn: Scalars['String'];
};

/** Details about how a policy was evaluated for a given actor and resource */
export type PolicyEvaluationDetail = {
  __typename?: 'PolicyEvaluationDetail';
  /** The policy that was evaluated */
  policyName: Scalars['String'];
  /** The reason for deny for this policy */
  reason: Scalars['String'];
};

/** Match condition */
export enum PolicyMatchCondition {
  /** Whether the field matches the value */
  Equals = 'EQUALS',
  /** Whether the field does not match the value */
  NotEquals = 'NOT_EQUALS',
  /** Whether the field value starts with the value */
  StartsWith = 'STARTS_WITH'
}

/** Criterion to define relationship between field and values */
export type PolicyMatchCriterion = {
  __typename?: 'PolicyMatchCriterion';
  /** The name of the field that the criterion refers to */
  condition: PolicyMatchCondition;
  /**
   * The name of the field that the criterion refers to
   * e.g. entity_type, entity_urn, domain
   */
  field: Scalars['String'];
  /** Values. Matches criterion if any one of the values matches condition (OR-relationship) */
  values: Array<PolicyMatchCriterionValue>;
};

/** Criterion to define relationship between field and values */
export type PolicyMatchCriterionInput = {
  /** The name of the field that the criterion refers to */
  condition: PolicyMatchCondition;
  /**
   * The name of the field that the criterion refers to
   * e.g. entity_type, entity_urn, domain
   */
  field: Scalars['String'];
  /** Values. Matches criterion if any one of the values matches condition (OR-relationship) */
  values: Array<Scalars['String']>;
};

/** Value in PolicyMatchCriterion with hydrated entity if value is urn */
export type PolicyMatchCriterionValue = {
  __typename?: 'PolicyMatchCriterionValue';
  /** Hydrated entities of the above values. Only set if the value is an urn */
  entity?: Maybe<Entity>;
  /** The value of the field to match */
  value: Scalars['String'];
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
  /** A Policy that is active and being enforced */
  Active = 'ACTIVE',
  /**
   * A Policy that has not been officially created, but in progress
   * Currently unused
   */
  Draft = 'DRAFT',
  /** A Policy that is not active or being enforced */
  Inactive = 'INACTIVE'
}

/** The type of the Access Policy */
export enum PolicyType {
  /** An access policy that grants privileges pertaining to Metadata Entities */
  Metadata = 'METADATA',
  /** An access policy that grants top level administrative privileges pertaining to the DataHub Platform itself */
  Platform = 'PLATFORM'
}

/** Input provided when creating or updating an Access Policy */
export type PolicyUpdateInput = {
  /** The set of actors that the Policy privileges are granted to */
  actors: ActorFilterInput;
  /** A Policy description */
  description?: Maybe<Scalars['String']>;
  /** The Policy name */
  name: Scalars['String'];
  /** The set of privileges that the Policy grants */
  privileges: Array<Scalars['String']>;
  /** The set of resources that the Policy privileges apply to */
  resources?: Maybe<ResourceFilterInput>;
  /** The Policy state */
  state: PolicyState;
  /** The Policy Type */
  type: PolicyType;
};

/** Input provided when creating a Post */
export type Post = Entity & {
  __typename?: 'Post';
  /** The content of the post */
  content: PostContent;
  /** When the post was last modified */
  lastModified: AuditStamp;
  /** The type of post */
  postType: PostType;
  /** Granular API for querying edges extending from the Post */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the Post */
  urn: Scalars['String'];
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
  /** Optional content of the post */
  description?: Maybe<Scalars['String']>;
  /** Optional link that the post is associated with */
  link?: Maybe<Scalars['String']>;
  /** Optional media contained in the post */
  media?: Maybe<Media>;
  /** The title of the post */
  title: Scalars['String'];
};

/** The type of post */
export enum PostContentType {
  /** Link content */
  Link = 'LINK',
  /** Text content */
  Text = 'TEXT'
}

/** The type of post */
export enum PostType {
  /** Posts on an entity page */
  EntityAnnouncement = 'ENTITY_ANNOUNCEMENT',
  /** Posts on the home page */
  HomePageAnnouncement = 'HOME_PAGE_ANNOUNCEMENT'
}

/** An individual DataHub Access Privilege */
export type Privilege = {
  __typename?: 'Privilege';
  /** A description of the privilege to display */
  description?: Maybe<Scalars['String']>;
  /** The name to appear when displaying the privilege, eg Edit Entity */
  displayName?: Maybe<Scalars['String']>;
  /** Standardized privilege type, serving as a unique identifier for a privilege eg EDIT_ENTITY */
  type: Scalars['String'];
};

/** Object that encodes the privileges the actor has for a given resource */
export type Privileges = {
  __typename?: 'Privileges';
  /** Details about how each policy was evaluated */
  evaluationDetails?: Maybe<Array<Maybe<PolicyEvaluationDetail>>>;
  /** Granted Privileges */
  privileges: Array<Scalars['String']>;
};

/** Product update information fetched from remote JSON source */
export type ProductUpdate = {
  __typename?: 'ProductUpdate';
  /**
   * Call-to-action link URL, with telemetry client ID appended (deprecated, use primaryCtaLink instead)
   * Kept for backward compatibility
   */
  ctaLink: Scalars['String'];
  /**
   * Call-to-action button text (deprecated, use primaryCtaText instead)
   * Kept for backward compatibility
   */
  ctaText: Scalars['String'];
  /** Optional description text for the update */
  description?: Maybe<Scalars['String']>;
  /** Whether this update is enabled and should be shown */
  enabled: Scalars['Boolean'];
  /** Optional list of features (up to 3) to display with icons and descriptions */
  features?: Maybe<Array<ProductUpdateFeature>>;
  /** Optional header text (displayed instead of title if provided) */
  header?: Maybe<Scalars['String']>;
  /**
   * Unique identifier for this update (e.g., "v1.2.1")
   * Version changes trigger re-display for users who dismissed previous versions
   */
  id: Scalars['String'];
  /** Optional URL to an image to display with the update */
  image?: Maybe<Scalars['String']>;
  /**
   * Primary call-to-action link URL, with telemetry client ID appended (required if primaryCtaText is provided)
   * Relative URLs will be prefixed with baseUrl
   */
  primaryCtaLink?: Maybe<Scalars['String']>;
  /** Primary call-to-action button text (required if primaryCtaLink is provided) */
  primaryCtaText?: Maybe<Scalars['String']>;
  /** Optional minimum required version for this update */
  requiredVersion?: Maybe<Scalars['String']>;
  /** Secondary call-to-action link URL, with telemetry client ID appended (optional) */
  secondaryCtaLink?: Maybe<Scalars['String']>;
  /** Secondary call-to-action button text (optional) */
  secondaryCtaText?: Maybe<Scalars['String']>;
  /** Title of the update notification */
  title: Scalars['String'];
};

/** Product update feature information */
export type ProductUpdateFeature = {
  __typename?: 'ProductUpdateFeature';
  /** Optional availability text (e.g., "Available in DataHub Cloud") */
  availability?: Maybe<Scalars['String']>;
  /** Description text for the feature (bullet text) */
  description: Scalars['String'];
  /**
   * Phosphor icon name (PascalCase, e.g., "Lightning", "Sparkle", "Settings", "Domain")
   * Optional - if not provided, a default bullet will be shown
   */
  icon?: Maybe<Scalars['String']>;
  /** Title of the feature (subheader) */
  title: Scalars['String'];
};

/**
 * The cardinality of a Structured Property determining whether one or multiple values
 * can be applied to the entity from this property.
 */
export enum PropertyCardinality {
  /** Multiple values of this property can applied to an entity */
  Multiple = 'MULTIPLE',
  /** Only one value of this property can applied to an entity */
  Single = 'SINGLE'
}

/** The value of a property */
export type PropertyValue = NumberValue | StringValue;

/** Input for collecting structured property values to apply to entities */
export type PropertyValueInput = {
  /** The number value for this structured property */
  numberValue?: Maybe<Scalars['Float']>;
  /** The string value for this structured property */
  stringValue?: Maybe<Scalars['String']>;
};

/** A quantile along with its corresponding value */
export type Quantile = {
  __typename?: 'Quantile';
  /** Quantile. E.g. "0.25" for the 25th percentile */
  quantile: Scalars['String'];
  /** The value of the quantile */
  value: Scalars['String'];
};

export type QuantitativeAnalyses = {
  __typename?: 'QuantitativeAnalyses';
  /** Link to a dashboard with results showing how the model performed with respect to the intersection of evaluated factors */
  intersectionalResults?: Maybe<ResultsType>;
  /** Link to a dashboard with results showing how the model performed with respect to each factor */
  unitaryResults?: Maybe<ResultsType>;
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
  /** Aggregate across DataHub entities */
  aggregateAcrossEntities?: Maybe<AggregateResults>;
  /**
   * Fetch configurations
   * Used by DataHub UI
   */
  appConfig?: Maybe<AppConfig>;
  /** Fetch an Application by primary key (urn) */
  application?: Maybe<Application>;
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
  /**
   * Browse for different entities by getting organizational groups and their
   * aggregated counts + content. Uses browsePathsV2 aspect and replaces our old
   * browse endpoint.
   */
  browseV2?: Maybe<BrowseResultsV2>;
  /** Fetch a Business Attribute by primary key (urn) */
  businessAttribute?: Maybe<BusinessAttribute>;
  /** Fetch a Chart by primary key (urn) */
  chart?: Maybe<Chart>;
  /**
   * Get a set of connection details by URN.
   * This requires the 'Manage Connections' platform privilege.
   * Returns null if a connection with the provided urn does not exist.
   */
  connection?: Maybe<DataHubConnection>;
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
  /** Fetch a Data Platform Instance by primary key (urn) */
  dataPlatformInstance?: Maybe<DataPlatformInstance>;
  /** Fetch a Data Process Instance by primary key (urn) */
  dataProcessInstance?: Maybe<DataProcessInstance>;
  /** Fetch a DataProduct by primary key (urn) */
  dataProduct?: Maybe<DataProduct>;
  /** Fetch a Dataset by primary key (urn) */
  dataset?: Maybe<Dataset>;
  /**
   * Experimental API to debug Access for users.
   * Backward incompatible changes will be made without notice in the future.
   * Do not build on top of this API.
   */
  debugAccess: DebugAccessResult;
  /** Fetch the global settings related to the docs propagation feature. */
  docPropagationSettings?: Maybe<DocPropagationSettings>;
  /**
   * Get a Document by URN.
   * Requires the GET_ENTITY privilege for the document or MANAGE_DOCUMENTS platform privilege.
   */
  document?: Maybe<Document>;
  /** Fetch a Domain by primary key (urn) */
  domain?: Maybe<Domain>;
  /**
   * Gets entities based on their urns
   * checkForExistence will do n+1 sql calls to check for existence of each entity requested if set to true
   */
  entities?: Maybe<Array<Maybe<Entity>>>;
  /** Gets an entity based on its urn */
  entity?: Maybe<Entity>;
  /** Get whether or not not an entity exists */
  entityExists?: Maybe<Scalars['Boolean']>;
  /** Fetch a ERModelRelationship by primary key (urn) */
  erModelRelationship?: Maybe<ErModelRelationship>;
  /**
   * Get an execution request
   * urn: The primary key associated with the execution request.
   */
  executionRequest?: Maybe<ExecutionRequest>;
  /** Fetch a Form by primary key (urn) */
  form?: Maybe<Form>;
  /**
   * Generates an access token for DataHub APIs for a particular user & of a particular type
   * Deprecated, use createAccessToken instead
   */
  getAccessToken?: Maybe<AccessToken>;
  /**
   * Fetches the metadata of an access token.
   * This is useful to debug when you have a raw token but don't know the actor.
   */
  getAccessTokenMetadata: AccessTokenMetadata;
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
  /** Generates a presigned url to upload files */
  getPresignedUploadUrl: GetPresignedUploadUrlResponse;
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
  /** Get a service account by URN. */
  getServiceAccount?: Maybe<ServiceAccount>;
  /** Returns a list of timeline actions for an entity based on the filter criteria */
  getTimeline?: Maybe<GetTimelineResult>;
  /** Global settings related to the home page for an instance */
  globalHomePageSettings?: Maybe<GlobalHomePageSettings>;
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
  /**
   * Fetch the latest product update information from configured JSON source.
   * Returns null if the JSON source is unavailable or feature is disabled.
   */
  latestProductUpdate?: Maybe<ProductUpdate>;
  /** List access tokens stored in DataHub. */
  listAccessTokens: ListAccessTokenResult;
  /** List Application assets for a given urn */
  listApplicationAssets?: Maybe<SearchResults>;
  /** Fetch all Business Attributes */
  listBusinessAttributes?: Maybe<ListBusinessAttributesResult>;
  /** List Data Product assets for a given urn */
  listDataProductAssets?: Maybe<SearchResults>;
  /** Lists all DataHub Domains belonging to the specified parent. If no parent is specified, lists root domains. */
  listDomains?: Maybe<ListDomainsResult>;
  /** List all execution requests */
  listExecutionRequests?: Maybe<IngestionSourceExecutionRequests>;
  /** List Global DataHub Views */
  listGlobalViews?: Maybe<ListViewsResult>;
  /** List all DataHub Groups */
  listGroups?: Maybe<ListGroupsResult>;
  /** List all ingestion sources */
  listIngestionSources?: Maybe<ListIngestionSourcesResult>;
  /** List DataHub Views owned by the current user */
  listMyViews?: Maybe<ListViewsResult>;
  /** List Custom Ownership Types */
  listOwnershipTypes: ListOwnershipTypesResult;
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
  /** List service accounts stored in DataHub. */
  listServiceAccounts: ListServiceAccountsResult;
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
  /** Fetch a Role by primary key (urn) */
  role?: Maybe<Role>;
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
  /**
   * Search Documents with hybrid semantic search and filtering support.
   * Supports filtering by parent document, types, domains, and query.
   *
   * Results are automatically filtered based on document status:
   * - PUBLISHED documents are shown to all users
   * - UNPUBLISHED documents are only shown if owned by the current calling user or a group they belong to
   *
   * This ensures that unpublished documents remain private to their owners while published
   * documents are discoverable by everyone.
   *
   * Additionally, this API does NOT return documents that have should not be shown in "global" context via
   * their document settings.
   */
  searchDocuments: SearchDocumentsResult;
  /** Search DataHub Users */
  searchUsers?: Maybe<SearchResults>;
  /** Full text semantic search against a specific DataHub Entity Type */
  semanticSearch?: Maybe<SearchResults>;
  /** Semantic search across DataHub entities */
  semanticSearchAcrossEntities?: Maybe<SearchResults>;
  /** Fetch a Structured Property by primary key (urn) */
  structuredProperty?: Maybe<StructuredPropertyEntity>;
  /** Fetch a Tag by primary key (urn) */
  tag?: Maybe<Tag>;
  /** Fetch a Test by primary key (urn) */
  test?: Maybe<Test>;
  /** Fetch a Version Set by its URN */
  versionSet?: Maybe<VersionSet>;
  /** Fetch a Dataset by primary key (urn) at a point in time based on aspect versions (versionStamp) */
  versionedDataset?: Maybe<VersionedDataset>;
  /** Fetch a View by primary key (urn) */
  view?: Maybe<DataHubView>;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryAggregateAcrossEntitiesArgs = {
  input: AggregateAcrossEntitiesInput;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryApplicationArgs = {
  urn: Scalars['String'];
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
export type QueryBrowseV2Args = {
  input: BrowseV2Input;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryBusinessAttributeArgs = {
  urn: Scalars['String'];
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
export type QueryConnectionArgs = {
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
export type QueryDataPlatformInstanceArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDataProcessInstanceArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDataProductArgs = {
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
export type QueryDebugAccessArgs = {
  userUrn: Scalars['String'];
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryDocumentArgs = {
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
  checkForExistence?: Maybe<Scalars['Boolean']>;
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
export type QueryErModelRelationshipArgs = {
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
export type QueryFormArgs = {
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
export type QueryGetAccessTokenMetadataArgs = {
  token: Scalars['String'];
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
export type QueryGetPresignedUploadUrlArgs = {
  input: GetPresignedUploadUrlInput;
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
export type QueryGetServiceAccountArgs = {
  urn: Scalars['String'];
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryGetTimelineArgs = {
  input: GetTimelineInput;
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
export type QueryLatestProductUpdateArgs = {
  refreshCache?: Maybe<Scalars['Boolean']>;
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
export type QueryListApplicationAssetsArgs = {
  input: SearchAcrossEntitiesInput;
  urn: Scalars['String'];
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListBusinessAttributesArgs = {
  input: ListBusinessAttributesInput;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryListDataProductAssetsArgs = {
  input: SearchAcrossEntitiesInput;
  urn: Scalars['String'];
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
export type QueryListExecutionRequestsArgs = {
  input: ListExecutionRequestsInput;
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
export type QueryListOwnershipTypesArgs = {
  input: ListOwnershipTypesInput;
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
export type QueryListServiceAccountsArgs = {
  input: ListServiceAccountsInput;
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
export type QueryRoleArgs = {
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
export type QuerySearchDocumentsArgs = {
  input: SearchDocumentsInput;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QuerySearchUsersArgs = {
  input: SearchAcrossEntitiesInput;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QuerySemanticSearchArgs = {
  input: SearchInput;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QuerySemanticSearchAcrossEntitiesArgs = {
  input: SearchAcrossEntitiesInput;
};


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryStructuredPropertyArgs = {
  urn: Scalars['String'];
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
export type QueryVersionSetArgs = {
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


/**
 * Root type used for fetching DataHub Metadata
 * Coming soon listEntity queries for listing all entities of a given type
 */
export type QueryViewArgs = {
  urn: Scalars['String'];
};

/** A Notebook cell which contains Query as content */
export type QueryCell = {
  __typename?: 'QueryCell';
  /** Unique id for the cell. */
  cellId: Scalars['String'];
  /** Title of the cell */
  cellTitle: Scalars['String'];
  /** Captures information about who created/last modified/deleted this TextCell and when */
  changeAuditStamps?: Maybe<ChangeAuditStamps>;
  /** Captures information about who last executed this query cell and when */
  lastExecuted?: Maybe<AuditStamp>;
  /** Raw query to explain some specific logic in a Notebook */
  rawQuery: Scalars['String'];
};

/** An individual Query */
export type QueryEntity = Entity & {
  __typename?: 'QueryEntity';
  /** Platform from which the Query was detected */
  platform?: Maybe<DataPlatform>;
  /** Properties about the Query */
  properties?: Maybe<QueryProperties>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Subjects for the query */
  subjects?: Maybe<Array<QuerySubject>>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the Query */
  urn: Scalars['String'];
};


/** An individual Query */
export type QueryEntityRelationshipsArgs = {
  input: RelationshipsInput;
};

/** A query language / dialect. */
export enum QueryLanguage {
  /** Standard ANSI SQL */
  Sql = 'SQL'
}

/** Properties about an individual Query */
export type QueryProperties = {
  __typename?: 'QueryProperties';
  /** An Audit Stamp corresponding to the creation of this resource */
  created: AuditStamp;
  /** A Resolved Audit Stamp corresponding to the creation of this resource */
  createdOn: ResolvedAuditStamp;
  /** Custom properties of the Data Product */
  customProperties?: Maybe<Array<CustomPropertiesEntry>>;
  /** The description of the Query */
  description?: Maybe<Scalars['String']>;
  /** An Audit Stamp corresponding to the update of this resource */
  lastModified: AuditStamp;
  /** The name of the Query */
  name?: Maybe<Scalars['String']>;
  /** The asset that this query originated from, e.g. a View, a dbt Model, etc. */
  origin?: Maybe<Entity>;
  /** The source of the Query */
  source: QuerySource;
  /** The Query statement itself */
  statement: QueryStatement;
};

/** The source of the query */
export enum QuerySource {
  /** The query was provided manually, e.g. from the UI. */
  Manual = 'MANUAL',
  /** The query was extracted by the system, e.g. from a dashboard. */
  System = 'SYSTEM'
}

/** An individual Query Statement */
export type QueryStatement = {
  __typename?: 'QueryStatement';
  /** The language for the Query Statement */
  language: QueryLanguage;
  /** The query statement value */
  value: Scalars['String'];
};

/** Input required for creating a Query Statement */
export type QueryStatementInput = {
  /** The query language */
  language: QueryLanguage;
  /** The query text */
  value: Scalars['String'];
};

/** The subject for a Query */
export type QuerySubject = {
  __typename?: 'QuerySubject';
  /** The dataset which is the subject of the Query */
  dataset: Dataset;
  /**
   * The schema field which is the subject of the Query.
   * This will be populated if the subject is specifically a schema field.
   */
  schemaField?: Maybe<SchemaFieldEntity>;
};

/** A quick filter in search and auto-complete */
export type QuickFilter = {
  __typename?: 'QuickFilter';
  /** Entity that the value maps to if any */
  entity?: Maybe<Entity>;
  /** Name of field to filter by */
  field: Scalars['String'];
  /** Value to filter on */
  value: Scalars['String'];
};

/** Input required to create a new incident in the 'Active' state. */
export type RaiseIncidentInput = {
  /** An optional set of user or group assignee urns */
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  /** A custom type of incident. Present only if type is 'CUSTOM' */
  customType?: Maybe<Scalars['String']>;
  /** An optional description associated with the incident */
  description?: Maybe<Scalars['String']>;
  /** An optional priority for the incident. */
  priority?: Maybe<IncidentPriority>;
  /**
   * The resource (dataset, dashboard, chart, dataFlow, etc) that the incident is associated with.
   * This must be present if resourceUrns are not defined.
   */
  resourceUrn?: Maybe<Scalars['String']>;
  /**
   * The resources (dataset, dashboard, chart, dataFlow, etc) that the incident is associated with.
   * This must be present and not empty if resourceUrn is not defined.
   */
  resourceUrns?: Maybe<Array<Scalars['String']>>;
  /** The source of the incident, i.e. how it was generated */
  source?: Maybe<IncidentSourceInput>;
  /** The time at which the incident actually started (may be before the date it was raised). */
  startedAt?: Maybe<Scalars['Long']>;
  /** The status of the incident */
  status?: Maybe<IncidentStatusInput>;
  /** An optional title associated with the incident */
  title?: Maybe<Scalars['String']>;
  /** The type of incident */
  type: IncidentType;
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
  /** Entity being recommended. Empty if the content being recommended is not an entity */
  entity?: Maybe<Entity>;
  /** Additional context required to generate the the recommendation */
  params?: Maybe<RecommendationParams>;
  /** String representation of content */
  value: Scalars['String'];
};

export type RecommendationModule = {
  __typename?: 'RecommendationModule';
  /** List of content to display inside the module */
  content: Array<RecommendationContent>;
  /** Unique id of the module being recommended */
  moduleId: Scalars['String'];
  /** Type of rendering that defines how the module should be rendered */
  renderType: RecommendationRenderType;
  /** Title of the module to display */
  title: Scalars['String'];
};

/** Parameters required to render a recommendation of a given type */
export type RecommendationParams = {
  __typename?: 'RecommendationParams';
  /** Context about the recommendation */
  contentParams?: Maybe<ContentParams>;
  /** Context to define the entity profile page */
  entityProfileParams?: Maybe<EntityProfileParams>;
  /** Context to define the search recommendations */
  searchParams?: Maybe<SearchParams>;
};

/**
 * Enum that defines how the modules should be rendered.
 * There should be two frontend implementation of large and small modules per type.
 */
export enum RecommendationRenderType {
  /** Domain Search List */
  DomainSearchList = 'DOMAIN_SEARCH_LIST',
  /** Simple list of entities */
  EntityNameList = 'ENTITY_NAME_LIST',
  /** Glossary Term search list */
  GlossaryTermSearchList = 'GLOSSARY_TERM_SEARCH_LIST',
  /** List of platforms */
  PlatformSearchList = 'PLATFORM_SEARCH_LIST',
  /** A list of recommended search queries */
  SearchQueryList = 'SEARCH_QUERY_LIST',
  /** Tag search list */
  TagSearchList = 'TAG_SEARCH_LIST'
}

/**
 * Context that defines the page requesting recommendations
 * i.e. for search pages, the query/filters. for entity pages, the entity urn and tab
 */
export type RecommendationRequestContext = {
  /** Additional context for defining the entity page requesting recommendations */
  entityRequestContext?: Maybe<EntityRequestContext>;
  /** Scenario in which the recommendations will be displayed */
  scenario: ScenarioType;
  /** Additional context for defining the search page requesting recommendations */
  searchRequestContext?: Maybe<SearchRequestContext>;
};

/**
 * Input for querying context documents related to an entity.
 * The relatedAssets filter is automatically set to the parent entity's URN.
 * This returns all documents related to the asset, including those hidden from global context.
 */
export type RelatedDocumentsInput = {
  /** The maximum number of Documents to return */
  count?: Maybe<Scalars['Int']>;
  /** Optional list of domain URNs to filter by (ANDed with other filters) */
  domains?: Maybe<Array<Scalars['String']>>;
  /** Optional list of parent document URNs to filter by (for batch child lookups) */
  parentDocuments?: Maybe<Array<Scalars['String']>>;
  /**
   * If true, only returns documents with no parent (root-level documents).
   * If false or not provided, returns all documents regardless of parent.
   */
  rootOnly?: Maybe<Scalars['Boolean']>;
  /** Optional document source type to filter by. */
  sourceType?: Maybe<DocumentSourceType>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** Optional list of document types to filter by (ANDed with other filters) */
  types?: Maybe<Array<Scalars['String']>>;
};

/**
 * Result containing context documents related to an entity.
 * Same structure as SearchDocumentsResult.
 */
export type RelatedDocumentsResult = {
  __typename?: 'RelatedDocumentsResult';
  /** The number of Documents in the returned result set */
  count: Scalars['Int'];
  /** The Documents themselves */
  documents: Array<Document>;
  /** Facets for filtering search results */
  facets?: Maybe<Array<FacetMetadata>>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Documents in the result set */
  total: Scalars['Int'];
};

/** Input provided when adding Terms to an asset */
export type RelatedTermsInput = {
  /** The type of relationship we're adding or removing to/from for a Glossary Term */
  relationshipType: TermRelationshipType;
  /** The primary key of the Glossary Term to add or remove */
  termUrns: Array<Scalars['String']>;
  /** The Glossary Term urn to add or remove this relationship to/from */
  urn: Scalars['String'];
};

/** Direction between a source and destination node */
export enum RelationshipDirection {
  /** A directed edge pointing at the source Entity */
  Incoming = 'INCOMING',
  /** A directed edge pointing at the destination Entity */
  Outgoing = 'OUTGOING'
}

/** ERModelRelationship FieldMap */
export type RelationshipFieldMapping = {
  __typename?: 'RelationshipFieldMapping';
  /** bfield */
  destinationField: Scalars['String'];
  /** left field */
  sourceField: Scalars['String'];
};

/** Details about the ERModelRelationship */
export type RelationshipFieldMappingInput = {
  /** Details about the ERModelRelationship */
  destinationField?: Maybe<Scalars['String']>;
  /** Details about the ERModelRelationship */
  sourceField?: Maybe<Scalars['String']>;
};

/** Input for the list relationships field of an Entity */
export type RelationshipsInput = {
  /** The number of results to be returned */
  count?: Maybe<Scalars['Int']>;
  /** The direction of the relationship, either incoming or outgoing from the source entity */
  direction: RelationshipDirection;
  /** Whether to include soft-deleted, related, entities */
  includeSoftDelete?: Maybe<Scalars['Boolean']>;
  /** The starting offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** The types of relationships to query, representing an OR */
  types: Array<Scalars['String']>;
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
  /** The label of the link to remove, which uniquely identifies the Link */
  label?: Maybe<Scalars['String']>;
  /** The url of the link to remove, which uniquely identifies the Link */
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
  /** The ownership type to remove, optional. By default will remove regardless of ownership type. */
  ownershipTypeUrn?: Maybe<Scalars['String']>;
  /** The urn of the resource or entity to attach or remove the owner from, for example a dataset urn */
  resourceUrn: Scalars['String'];
};

/** Input for removing structured properties on a given asset */
export type RemoveStructuredPropertiesInput = {
  /** The urn of the asset that we are removing properties from */
  assetUrn: Scalars['String'];
  /** The list of structured properties you want to remove from this asset */
  structuredPropertyUrns: Array<Scalars['String']>;
};

/** Input provided to report an asset operation */
export type ReportOperationInput = {
  /** A custom type of operation. Required if operation type is CUSTOM. */
  customOperationType?: Maybe<Scalars['String']>;
  /** A list of key-value parameters to include */
  customProperties?: Maybe<Array<StringMapEntryInput>>;
  /** Optional: The number of affected rows */
  numAffectedRows?: Maybe<Scalars['Long']>;
  /** The type of operation that was performed. Required */
  operationType: OperationType;
  /** An optional partition identifier */
  partition?: Maybe<Scalars['String']>;
  /** The source or reporter of the operation */
  sourceType: OperationSourceType;
  /**
   * Optional: Provide a timestamp associated with the operation. If not provided, one will be generated for you based
   * on the current time.
   */
  timestampMillis?: Maybe<Scalars['Long']>;
  /** The urn of the asset (e.g. dataset) to report the operation for */
  urn: Scalars['String'];
};

/** Token that allows native users to reset their credentials */
export type ResetToken = {
  __typename?: 'ResetToken';
  /** The reset token */
  resetToken: Scalars['String'];
};

export type ResolvedActor = CorpGroup | CorpUser;

/** Audit stamp containing a resolved actor */
export type ResolvedAuditStamp = {
  __typename?: 'ResolvedAuditStamp';
  /** Who performed the audited action */
  actor?: Maybe<CorpUser>;
  /** When the audited action took place */
  time: Scalars['Long'];
};

/** The resources that a DataHub Access Policy applies to */
export type ResourceFilter = {
  __typename?: 'ResourceFilter';
  /**
   * Deprecated, use filter instead
   * Whether of not to apply the filter to all resources of the type
   * @deprecated Field no longer supported
   */
  allResources?: Maybe<Scalars['Boolean']>;
  /** Whether of not to apply the filter to all resources of the type */
  filter?: Maybe<PolicyMatchFilter>;
  /** Constraints on what subresources can be acted upon */
  privilegeConstraints?: Maybe<PolicyMatchFilter>;
  /**
   * Deprecated, use filter instead
   * A list of specific resource urns to apply the filter to
   * @deprecated Field no longer supported
   */
  resources?: Maybe<Array<Scalars['String']>>;
  /**
   * Deprecated, use filter instead
   * The type of the resource the policy should apply to Not required because in the future we want to support filtering by type OR by domain
   * @deprecated Field no longer supported
   */
  type?: Maybe<Scalars['String']>;
};

/** Input required when creating or updating an Access Policies Determines which resources the Policy applies to */
export type ResourceFilterInput = {
  /**
   * Deprecated, use empty filter instead
   * Whether of not to apply the filter to all resources of the type
   */
  allResources?: Maybe<Scalars['Boolean']>;
  /** Whether of not to apply the filter to all resources of the type */
  filter?: Maybe<PolicyMatchFilterInput>;
  /** Constraints on what subresources can be acted upon */
  privilegeConstraints?: Maybe<PolicyMatchFilterInput>;
  /**
   * Deprecated, use filter instead
   * A list of specific resource urns to apply the filter to
   */
  resources?: Maybe<Array<Scalars['String']>>;
  /**
   * Deprecated, use filter field instead
   * The type of the resource the policy should apply to
   * Not required because in the future we want to support filtering by type OR by domain
   */
  type?: Maybe<Scalars['String']>;
};

/**
 * A privilege associated with a particular resource type
 * A resource is most commonly a DataHub Metadata Entity
 */
export type ResourcePrivileges = {
  __typename?: 'ResourcePrivileges';
  /** An optional entity type to use when performing search and navigation to the entity */
  entityType?: Maybe<EntityType>;
  /** A list of privileges that are supported against this resource */
  privileges: Array<Privilege>;
  /** Resource type associated with the Access Privilege, eg dataset */
  resourceType: Scalars['String'];
  /** The name to used for displaying the resourceType */
  resourceTypeDisplayName?: Maybe<Scalars['String']>;
};

/** Reference to a resource to apply an action to */
export type ResourceRefInput = {
  /** The urn of the resource being referenced */
  resourceUrn: Scalars['String'];
  /** An optional sub resource identifier to attach the Tag to */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to attach the Tag to */
  subResourceType?: Maybe<SubResourceType>;
};

/** Spec to identify resource */
export type ResourceSpec = {
  /** Resource type */
  resourceType: EntityType;
  /** Resource urn */
  resourceUrn: Scalars['String'];
};

/**
 * A restricted entity that the user does not have full permissions to view.
 * This entity type does not relate to an entity type in the database.
 */
export type Restricted = Entity & EntityWithRelationships & {
  __typename?: 'Restricted';
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** Edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the restricted entity */
  urn: Scalars['String'];
};


/**
 * A restricted entity that the user does not have full permissions to view.
 * This entity type does not relate to an entity type in the database.
 */
export type RestrictedLineageArgs = {
  input: LineageInput;
};


/**
 * A restricted entity that the user does not have full permissions to view.
 * This entity type does not relate to an entity type in the database.
 */
export type RestrictedRelationshipsArgs = {
  input: RelationshipsInput;
};

export type ResultsType = StringBox;

/** The params required if the module is type RICH_TEXT */
export type RichTextModuleParams = {
  __typename?: 'RichTextModuleParams';
  /** The content of the rich text module */
  content: Scalars['String'];
};

/** Input for the params required if the module is type RICH_TEXT */
export type RichTextModuleParamsInput = {
  /** The content of the rich text module */
  content: Scalars['String'];
};

export type Role = Entity & {
  __typename?: 'Role';
  /** A standard Entity Type */
  actors?: Maybe<Actor>;
  /** Id of the Role */
  id: Scalars['String'];
  isAssignedToMe: Scalars['Boolean'];
  /** Role properties to include Request Access Url */
  properties?: Maybe<RoleProperties>;
  /** List of relationships between the source Entity and some destination entities with a given types */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key of the Metadata Entity */
  urn: Scalars['String'];
};


export type RoleRelationshipsArgs = {
  input: RelationshipsInput;
};

export type RoleAssociation = {
  __typename?: 'RoleAssociation';
  /** Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** The Role entity itself */
  role: Role;
};

export type RoleGroup = {
  __typename?: 'RoleGroup';
  /** Linked corp group of a role */
  group: CorpGroup;
};

export type RoleProperties = {
  __typename?: 'RoleProperties';
  /** Description about the role */
  description?: Maybe<Scalars['String']>;
  /** Name of the Role in an organisation */
  name: Scalars['String'];
  /** Url to request a role for a user in an organisation */
  requestUrl?: Maybe<Scalars['String']>;
  /** Role type can be READ, WRITE or ADMIN */
  type?: Maybe<Scalars['String']>;
};

export type RoleUser = {
  __typename?: 'RoleUser';
  /** Linked corp user of a role */
  user: CorpUser;
};

/** Input for rolling back an ingestion execution */
export type RollbackIngestionInput = {
  /** An ingestion run ID */
  runId: Scalars['String'];
};

/** For consumption by UI only */
export type Row = {
  __typename?: 'Row';
  cells?: Maybe<Array<Cell>>;
  values: Array<Scalars['String']>;
};

/** Attributes defining an ROW_COUNT_CHANGE volume assertion. */
export type RowCountChange = {
  __typename?: 'RowCountChange';
  /**
   * The operator you'd like to apply.
   * Note that only numeric operators are valid inputs:
   * GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,
   * BETWEEN.
   */
  operator: AssertionStdOperator;
  /**
   * The parameters you'd like to provide as input to the operator.
   * Note that only numeric parameter types are valid inputs: NUMBER.
   */
  parameters: AssertionStdParameters;
  /** The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage. */
  type: AssertionValueChangeType;
};

/** Attributes defining an ROW_COUNT_TOTAL volume assertion. */
export type RowCountTotal = {
  __typename?: 'RowCountTotal';
  /**
   * The operator you'd like to apply.
   * Note that only numeric operators are valid inputs:
   * GREATER_THAN, GREATER_THAN_OR_EQUAL_TO, EQUAL_TO, LESS_THAN, LESS_THAN_OR_EQUAL_TO,
   * BETWEEN.
   */
  operator: AssertionStdOperator;
  /**
   * The parameters you'd like to provide as input to the operator.
   * Note that only numeric parameter types are valid inputs: NUMBER.
   */
  parameters: AssertionStdParameters;
};

/** Type of the scenario requesting recommendation */
export enum ScenarioType {
  /** Recommendations to show on an Entity Profile page */
  EntityProfile = 'ENTITY_PROFILE',
  /** Recommendations to show on the users home page */
  Home = 'HOME',
  /** Recommendations to show on the search bar when clicked */
  SearchBar = 'SEARCH_BAR',
  /** Recommendations to show on the search results page */
  SearchResults = 'SEARCH_RESULTS'
}

/**
 * Deprecated, use SchemaMetadata instead
 * Metadata about a Dataset schema
 */
export type Schema = {
  __typename?: 'Schema';
  /** The cluster this schema metadata is derived from */
  cluster?: Maybe<Scalars['String']>;
  /** The time at which the schema metadata information was created */
  createdAt?: Maybe<Scalars['Long']>;
  /** Dataset this schema metadata is associated with */
  datasetUrn?: Maybe<Scalars['String']>;
  /** Client provided a list of fields from value schema */
  fields: Array<SchemaField>;
  /** Client provided list of foreign key constraints */
  foreignKeys?: Maybe<Array<Maybe<ForeignKeyConstraint>>>;
  /** The SHA1 hash of the schema content */
  hash: Scalars['String'];
  /** The time at which the schema metadata information was last ingested */
  lastObserved?: Maybe<Scalars['Long']>;
  /** Schema name */
  name: Scalars['String'];
  /** The native schema in the datasets platform, schemaless if it was not provided */
  platformSchema?: Maybe<PlatformSchema>;
  /** Platform this schema metadata is associated with */
  platformUrn: Scalars['String'];
  /** Client provided list of fields that define primary keys to access record */
  primaryKeys?: Maybe<Array<Scalars['String']>>;
  /** The version of the GMS Schema metadata */
  version: Scalars['Long'];
};

/** Defines the required compatibility level for the schema assertion to pass. */
export enum SchemaAssertionCompatibility {
  /** The schema must be exactly the same as the expected schema. */
  ExactMatch = 'EXACT_MATCH',
  /** The schema must be a subset of the expected schema. */
  Subset = 'SUBSET',
  /** The schema must be a superset of the expected schema. */
  Superset = 'SUPERSET'
}

/** Defines a schema field, each with a specified path and type. */
export type SchemaAssertionField = {
  __typename?: 'SchemaAssertionField';
  /** Optional: The specific native or standard type of the field. */
  nativeType?: Maybe<Scalars['String']>;
  /** The standard V1 path of the field within the schema. */
  path: Scalars['String'];
  /** The std type of the field */
  type: SchemaFieldDataType;
};

/** Information about an Schema assertion */
export type SchemaAssertionInfo = {
  __typename?: 'SchemaAssertionInfo';
  /** The compatibility level required for the assertion to pass. */
  compatibility: SchemaAssertionCompatibility;
  /** The entity targeted by this schema assertion. */
  entityUrn: Scalars['String'];
  /** A single field in the schema assertion. */
  fields: Array<SchemaAssertionField>;
  /**
   * A definition of the expected structure for the asset
   * Deprecated! Use the simpler 'fields' instead.
   */
  schema?: Maybe<SchemaMetadata>;
};

export type SchemaContract = {
  __typename?: 'SchemaContract';
  /** The assertion representing the schema contract. */
  assertion: Assertion;
};

/** Input required to create a schema contract */
export type SchemaContractInput = {
  /** The assertion monitoring this part of the data contract. Assertion must be of type Data Schema. */
  assertionUrn: Scalars['String'];
};

/** Information about an individual field in a Dataset schema */
export type SchemaField = {
  __typename?: 'SchemaField';
  /** Description of the field */
  description?: Maybe<Scalars['String']>;
  /** Flattened name of the field computed from jsonPath field */
  fieldPath: Scalars['String'];
  /**
   * Deprecated, use tags field instead
   * Tags associated with the field
   * @deprecated Field no longer supported
   */
  globalTags?: Maybe<GlobalTags>;
  /** Glossary terms associated with the field */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Whether the field is part of a key schema */
  isPartOfKey?: Maybe<Scalars['Boolean']>;
  /** Whether the field is part of a partitioning key schema */
  isPartitioningKey?: Maybe<Scalars['Boolean']>;
  /** Flattened name of a field in JSON Path notation */
  jsonPath?: Maybe<Scalars['String']>;
  /** For schema fields that have other properties that are not modeled explicitly, represented as a JSON string. */
  jsonProps?: Maybe<Scalars['String']>;
  /** Human readable label for the field. Not supplied by all data sources */
  label?: Maybe<Scalars['String']>;
  /** The native type of the field in the datasets platform as declared by platform schema */
  nativeDataType?: Maybe<Scalars['String']>;
  /** Indicates if this field is optional or nullable */
  nullable: Scalars['Boolean'];
  /** Whether the field references its own type recursively */
  recursive: Scalars['Boolean'];
  /** Schema field entity that exist in the database for this schema field */
  schemaFieldEntity?: Maybe<SchemaFieldEntity>;
  /** Tags associated with the field */
  tags?: Maybe<GlobalTags>;
  /** Platform independent field type of the field */
  type: SchemaFieldDataType;
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
  /** The type of the change */
  changeType: ChangeOperationType;
  /** Last column update, such as Added/Modified/Removed in v1.2.3. */
  lastSchemaFieldChange?: Maybe<Scalars['String']>;
  /** The last semantic version that this schema was changed in */
  lastSemanticVersion: Scalars['String'];
  /** The time at which the schema was updated */
  timestampMillis: Scalars['Long'];
  /** Version stamp of the change */
  versionStamp: Scalars['String'];
};

/** The type associated with a single Dataset schema field */
export enum SchemaFieldDataType {
  /** An array collection type */
  Array = 'ARRAY',
  /** A boolean type */
  Boolean = 'BOOLEAN',
  /** A string of bytes */
  Bytes = 'BYTES',
  /** A datestrings type */
  Date = 'DATE',
  /** An enum type */
  Enum = 'ENUM',
  /** A fixed bytestring type */
  Fixed = 'FIXED',
  /** A map collection type */
  Map = 'MAP',
  /** A NULL type */
  Null = 'NULL',
  /** A number, including integers, floats, and doubles */
  Number = 'NUMBER',
  /** A string type */
  String = 'STRING',
  /** An complex struct type */
  Struct = 'STRUCT',
  /** A timestamp type */
  Time = 'TIME',
  /** An union type */
  Union = 'UNION'
}

/**
 * Standalone schema field entity. Differs from the SchemaField struct because it is not directly nested inside a
 * schema field
 */
export type SchemaFieldEntity = Entity & EntityWithRelationships & HasLogicalParent & {
  __typename?: 'SchemaFieldEntity';
  /** Business Attribute associated with the field */
  businessAttributes?: Maybe<BusinessAttributes>;
  /** deprecation status of the schema field */
  deprecation?: Maybe<Deprecation>;
  /** Documentation aspect for this schema field */
  documentation?: Maybe<Documentation>;
  /** Field path identifying the field in its dataset */
  fieldPath: Scalars['String'];
  /** The forms associated with the Dataset */
  forms?: Maybe<Forms>;
  /** Glossary terms associated with the field */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Edges extending from this entity grouped by direction in the lineage graph */
  lineage?: Maybe<EntityLineageResult>;
  /** If this entity represents a physical asset, this is its logical parent, from which metadata can propagate. */
  logicalParent?: Maybe<Entity>;
  /** The field's parent. */
  parent: Entity;
  /** Get context documents related to this entity */
  relatedDocuments?: Maybe<RelatedDocumentsResult>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The status of the SchemaFieldEntity */
  status?: Maybe<Status>;
  /** Structured properties on this schema field */
  structuredProperties?: Maybe<StructuredProperties>;
  /** Tags associated with the field */
  tags?: Maybe<GlobalTags>;
  /** A standard Entity Type */
  type: EntityType;
  /** Primary key of the schema field */
  urn: Scalars['String'];
};


/**
 * Standalone schema field entity. Differs from the SchemaField struct because it is not directly nested inside a
 * schema field
 */
export type SchemaFieldEntityLineageArgs = {
  input: LineageInput;
};


/**
 * Standalone schema field entity. Differs from the SchemaField struct because it is not directly nested inside a
 * schema field
 */
export type SchemaFieldEntityRelatedDocumentsArgs = {
  input: RelatedDocumentsInput;
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
  /** A schema field path */
  path: Scalars['String'];
  /** A schema field urn */
  urn: Scalars['String'];
};

/** Information about the field to use in an assertion */
export type SchemaFieldSpec = {
  __typename?: 'SchemaFieldSpec';
  /** The native field type */
  nativeType: Scalars['String'];
  /** The field path */
  path: Scalars['String'];
  /** The DataHub standard schema field type. */
  type: Scalars['String'];
};

/** Metadata about a Dataset schema */
export type SchemaMetadata = Aspect & {
  __typename?: 'SchemaMetadata';
  /**
   * The logical version of the schema metadata, where zero represents the latest version
   * with otherwise monotonic ordering starting at one
   */
  aspectVersion?: Maybe<Scalars['Long']>;
  /** The cluster this schema metadata is derived from */
  cluster?: Maybe<Scalars['String']>;
  /** The time at which the schema metadata information was created */
  createdAt?: Maybe<Scalars['Long']>;
  /** Dataset this schema metadata is associated with */
  datasetUrn?: Maybe<Scalars['String']>;
  /** Client provided a list of fields from value schema */
  fields: Array<SchemaField>;
  /** Client provided list of foreign key constraints */
  foreignKeys?: Maybe<Array<Maybe<ForeignKeyConstraint>>>;
  /** The SHA1 hash of the schema content */
  hash: Scalars['String'];
  /** Schema name */
  name: Scalars['String'];
  /** The native schema in the datasets platform, schemaless if it was not provided */
  platformSchema?: Maybe<PlatformSchema>;
  /** Platform this schema metadata is associated with */
  platformUrn: Scalars['String'];
  /** Client provided list of fields that define primary keys to access record */
  primaryKeys?: Maybe<Array<Scalars['String']>>;
  /** The version of the GMS Schema metadata */
  version: Scalars['Long'];
};

/** Input arguments for a full text search query across entities, specifying a starting pointer. Allows paging beyond 10k results */
export type ScrollAcrossEntitiesInput = {
  /** The number of elements included in the results */
  count?: Maybe<Scalars['Int']>;
  /** The amount of time to keep the point in time snapshot alive, takes a time unit based string ex: 5m or 30s */
  keepAlive?: Maybe<Scalars['String']>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The query string */
  query: Scalars['String'];
  /** The starting point of paginated results, an opaque ID the backend understands as a pointer */
  scrollId?: Maybe<Scalars['String']>;
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** Optional - Information on how to sort this search result */
  sortInput?: Maybe<SearchSortInput>;
  /** Entity types to be searched. If this is not provided, all entities will be searched. */
  types?: Maybe<Array<EntityType>>;
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

/** Input arguments for a search query over the results of a multi-hop graph query, uses scroll API */
export type ScrollAcrossLineageInput = {
  /** The number of elements included in the results */
  count?: Maybe<Scalars['Int']>;
  /** The direction of the relationship, either incoming or outgoing from the source entity */
  direction: LineageDirection;
  /** An optional ending time to filter on */
  endTimeMillis?: Maybe<Scalars['Long']>;
  /** The amount of time to keep the point in time snapshot alive, takes a time unit based string ex: 5m or 30s */
  keepAlive?: Maybe<Scalars['String']>;
  /** Flags controlling the lineage query */
  lineageFlags?: Maybe<LineageFlags>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The query string */
  query?: Maybe<Scalars['String']>;
  /** The starting point of paginated results, an opaque ID the backend understands as a pointer */
  scrollId?: Maybe<Scalars['String']>;
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** An optional starting time to filter on */
  startTimeMillis?: Maybe<Scalars['Long']>;
  /** Entity types to be searched. If this is not provided, all entities will be searched. */
  types?: Maybe<Array<EntityType>>;
  /** Urn of the source node */
  urn?: Maybe<Scalars['String']>;
};

/** Results returned by issuing a search across relationships query using scroll API */
export type ScrollAcrossLineageResults = {
  __typename?: 'ScrollAcrossLineageResults';
  /** The number of entities included in the result set */
  count: Scalars['Int'];
  /** Candidate facet aggregations used for search filtering */
  facets?: Maybe<Array<FacetMetadata>>;
  /** Optional freshness characteristics of this query (cached, staleness etc.) */
  freshness?: Maybe<FreshnessStats>;
  /** Indicates whether the results are partial due to reaching the maxRelations limit or timeout */
  isPartial?: Maybe<Scalars['Boolean']>;
  /** Opaque ID to pass to the next request to the server */
  nextScrollId?: Maybe<Scalars['String']>;
  /** The search result entities */
  searchResults: Array<SearchAcrossLineageResult>;
  /** The total number of search results matching the query and filters */
  total: Scalars['Int'];
};

/** Results returned by issuing a search query */
export type ScrollResults = {
  __typename?: 'ScrollResults';
  /** The number of entities included in the result set */
  count: Scalars['Int'];
  /** Candidate facet aggregations used for search filtering */
  facets?: Maybe<Array<FacetMetadata>>;
  /** Opaque ID to pass to the next request to the server */
  nextScrollId?: Maybe<Scalars['String']>;
  /** The search result entities for a scroll request */
  searchResults: Array<SearchResult>;
  /** The total number of search results matching the query and filters */
  total: Scalars['Int'];
};

/** Input arguments for a full text search query across entities */
export type SearchAcrossEntitiesInput = {
  /** The number of elements included in the results */
  count?: Maybe<Scalars['Int']>;
  /**
   * Deprecated in favor of the more expressive orFilters field
   * Facet filters to apply to search results. These will be 'AND'-ed together.
   */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The query string */
  query: Scalars['String'];
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** Optional - Information on how to sort this search result */
  sortInput?: Maybe<SearchSortInput>;
  /** The starting point of paginated results */
  start?: Maybe<Scalars['Int']>;
  /** Entity types to be searched. If this is not provided, all entities will be searched. */
  types?: Maybe<Array<EntityType>>;
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

/** Input arguments for a search query over the results of a multi-hop graph query */
export type SearchAcrossLineageInput = {
  /** The number of elements included in the results */
  count?: Maybe<Scalars['Int']>;
  /** The direction of the relationship, either incoming or outgoing from the source entity */
  direction: LineageDirection;
  /** An optional ending time to filter on */
  endTimeMillis?: Maybe<Scalars['Long']>;
  /**
   * Deprecated in favor of the more expressive orFilters field
   * Facet filters to apply to search results. These will be 'AND'-ed together.
   */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Flags controlling the lineage query */
  lineageFlags?: Maybe<LineageFlags>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The query string */
  query?: Maybe<Scalars['String']>;
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** Optional - Information on how to sort this search result */
  sortInput?: Maybe<SearchSortInput>;
  /** The starting point of paginated results */
  start?: Maybe<Scalars['Int']>;
  /** An optional starting time to filter on */
  startTimeMillis?: Maybe<Scalars['Long']>;
  /** Entity types to be searched. If this is not provided, all entities will be searched. */
  types?: Maybe<Array<EntityType>>;
  /** Urn of the source node */
  urn?: Maybe<Scalars['String']>;
  /** Optional - A View to apply when generating results */
  viewUrn?: Maybe<Scalars['String']>;
};

/** Individual search result from a search across relationships query (has added metadata about the path) */
export type SearchAcrossLineageResult = {
  __typename?: 'SearchAcrossLineageResult';
  /** Degree of relationship (number of hops to get to entity) */
  degree: Scalars['Int'];
  /** Degrees of relationship (for entities discoverable at multiple degrees) */
  degrees?: Maybe<Array<Scalars['Int']>>;
  /** The resolved DataHub Metadata Entity matching the search query */
  entity: Entity;
  /** Marks whether or not this entity was explored further for lineage */
  explored: Scalars['Boolean'];
  /** Whether this relationship was ignored as a hop */
  ignoredAsHop: Scalars['Boolean'];
  /** Insights about why the search result was matched */
  insights?: Maybe<Array<SearchInsight>>;
  /** Matched field hint */
  matchedFields: Array<MatchedField>;
  /** Optional list of entities between the source and destination node */
  paths?: Maybe<Array<Maybe<EntityPath>>>;
  /** Indicates this destination node has additional unexplored child relationships */
  truncatedChildren: Scalars['Boolean'];
};

/** Results returned by issuing a search across relationships query */
export type SearchAcrossLineageResults = {
  __typename?: 'SearchAcrossLineageResults';
  /** The number of entities included in the result set */
  count: Scalars['Int'];
  /** Candidate facet aggregations used for search filtering */
  facets?: Maybe<Array<FacetMetadata>>;
  /** Optional freshness characteristics of this query (cached, staleness etc.) */
  freshness?: Maybe<FreshnessStats>;
  /** Indicates whether the results are partial due to reaching the maxRelations limit or timeout */
  isPartial?: Maybe<Scalars['Boolean']>;
  /** The path taken when doing search across lineage */
  lineageSearchPath?: Maybe<LineageSearchPath>;
  /** The search result entities */
  searchResults: Array<SearchAcrossLineageResult>;
  /** The offset of the result set */
  start: Scalars['Int'];
  /** The total number of search results matching the query and filters */
  total: Scalars['Int'];
};

/** Variants of APIs used in the Search bar to get data */
export enum SearchBarApi {
  AutocompleteForMultiple = 'AUTOCOMPLETE_FOR_MULTIPLE',
  SearchAcrossEntities = 'SEARCH_ACROSS_ENTITIES'
}

/** Configurations related to the Search bar */
export type SearchBarConfig = {
  __typename?: 'SearchBarConfig';
  /** API variant */
  apiVariant: SearchBarApi;
};

/** Configurations related to the Search card */
export type SearchCardConfig = {
  __typename?: 'SearchCardConfig';
  /** Whether the search card should show description */
  showDescription: Scalars['Boolean'];
};

/**
 * Input required when searching Documents.
 *
 * Search results are automatically filtered based on document visibility and ownership:
 * - PUBLISHED documents are returned for all users
 * - UNPUBLISHED documents are only returned if owned by the current user or a group they belong to
 *
 * This ensures private/unpublished documents remain visible only to their owners.
 */
export type SearchDocumentsInput = {
  /** The maximum number of Documents to be returned in the result set */
  count?: Maybe<Scalars['Int']>;
  /** Optional list of domain URNs to filter by (ANDed with other filters) */
  domains?: Maybe<Array<Scalars['String']>>;
  /** Optional list of parent document URNs to filter by (for batch child lookups) */
  parentDocuments?: Maybe<Array<Scalars['String']>>;
  /** Optional semantic search query to search across document contents and metadata */
  query?: Maybe<Scalars['String']>;
  /**
   * Optional list of related asset URNs to filter by (ANDed with other filters).
   * Returns documents that are related to ANY of the provided asset URNs.
   */
  relatedAssets?: Maybe<Array<Scalars['String']>>;
  /**
   * If true, only returns documents with no parent (root-level documents).
   * If false or not provided, returns all documents regardless of parent.
   */
  rootOnly?: Maybe<Scalars['Boolean']>;
  /**
   * Optional document source type to filter by.
   * If not provided, searches all documents regardless of source.
   */
  sourceType?: Maybe<DocumentSourceType>;
  /** The starting offset of the result set returned */
  start?: Maybe<Scalars['Int']>;
  /** Optional list of document types to filter by (ANDed with other filters) */
  types?: Maybe<Array<Scalars['String']>>;
};

/** The result obtained when searching Documents */
export type SearchDocumentsResult = {
  __typename?: 'SearchDocumentsResult';
  /** The number of Documents in the returned result set */
  count: Scalars['Int'];
  /** The Documents themselves */
  documents: Array<Document>;
  /** Facets for filtering search results */
  facets?: Maybe<Array<FacetMetadata>>;
  /** The starting offset of the result set returned */
  start: Scalars['Int'];
  /** The total number of Documents in the result set */
  total: Scalars['Int'];
};

/** Set of flags to control search behavior */
export type SearchFlags = {
  /** fields to include for custom Highlighting */
  customHighlightingFields?: Maybe<Array<Scalars['String']>>;
  /** Determines whether to filter out any non-latest entity version if entity is part of a Version Set, default true */
  filterNonLatestVersions?: Maybe<Scalars['Boolean']>;
  /** Structured or unstructured fulltext query */
  fulltext?: Maybe<Scalars['Boolean']>;
  /** Whether to request for search suggestions on the _entityName virtualized field */
  getSuggestions?: Maybe<Scalars['Boolean']>;
  /**
   * Additional grouping specifications to apply to the search results
   * Grouping specifications will control how search results are grouped together
   * in the response. This is currently being used to group schema fields (columns)
   * as datasets, and in the future will be used to group other entities as well.
   * Note: This is an experimental feature and is subject to change.
   */
  groupingSpec?: Maybe<GroupingSpec>;
  /** Whether to include restricted entities */
  includeRestricted?: Maybe<Scalars['Boolean']>;
  /** Whether to include soft deleted entities */
  includeSoftDeleted?: Maybe<Scalars['Boolean']>;
  /** Whether or not to fetch and request for structured property facets when doing a search */
  includeStructuredPropertyFacets?: Maybe<Scalars['Boolean']>;
  /** The maximum number of values in an facet aggregation */
  maxAggValues?: Maybe<Scalars['Int']>;
  /** Whether to skip aggregates/facets */
  skipAggregates?: Maybe<Scalars['Boolean']>;
  /** Whether to skip cache */
  skipCache?: Maybe<Scalars['Boolean']>;
  /** Whether to skip highlighting */
  skipHighlighting?: Maybe<Scalars['Boolean']>;
};

/** Configurations related the Search Flags */
export type SearchFlagsConfig = {
  __typename?: 'SearchFlagsConfig';
  /** Default value for skipHighlighting search flag. Currently used in Search Page and Search Bar */
  defaultSkipHighlighting: Scalars['Boolean'];
};

/** Input arguments for a full text search query */
export type SearchInput = {
  /** The number of entities to include in result set */
  count?: Maybe<Scalars['Int']>;
  /**
   * Deprecated in favor of the more expressive orFilters field
   * Facet filters to apply to search results. These will be 'AND'-ed together.
   */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** A list of disjunctive criterion for the filter. (or operation to combine filters) */
  orFilters?: Maybe<Array<AndFilterInput>>;
  /** The raw query string */
  query: Scalars['String'];
  /** Flags controlling search options */
  searchFlags?: Maybe<SearchFlags>;
  /** The offset of the result set */
  start?: Maybe<Scalars['Int']>;
  /** The Metadata Entity type to be searched against */
  type: EntityType;
};

/** Insights about why a search result was returned or ranked in the way that it was */
export type SearchInsight = {
  __typename?: 'SearchInsight';
  /** An optional emoji to display in front of the text */
  icon?: Maybe<Scalars['String']>;
  /** The insight to display */
  text: Scalars['String'];
};

/** Context to define the search recommendations */
export type SearchParams = {
  __typename?: 'SearchParams';
  /** Filters */
  filters?: Maybe<Array<FacetFilter>>;
  /** Search query */
  query: Scalars['String'];
  /** Entity types to be searched. If this is not provided, all entities will be searched. */
  types?: Maybe<Array<EntityType>>;
};

/** Context that defines a search page requesting recommendatinos */
export type SearchRequestContext = {
  /** Faceted filters applied to search results */
  filters?: Maybe<Array<FacetFilterInput>>;
  /** Search query */
  query: Scalars['String'];
};

/** An individual search result hit */
export type SearchResult = {
  __typename?: 'SearchResult';
  /** The resolved DataHub Metadata Entity matching the search query */
  entity: Entity;
  /** Additional properties about the search result. Used for rendering in the UI */
  extraProperties?: Maybe<Array<ExtraProperty>>;
  /** Insights about why the search result was matched */
  insights?: Maybe<Array<SearchInsight>>;
  /** Matched field hint */
  matchedFields: Array<MatchedField>;
};

/** Results returned by issuing a search query */
export type SearchResults = {
  __typename?: 'SearchResults';
  /** The number of entities included in the result set */
  count: Scalars['Int'];
  /** Candidate facet aggregations used for search filtering */
  facets?: Maybe<Array<FacetMetadata>>;
  /** The search result entities */
  searchResults: Array<SearchResult>;
  /** The offset of the result set */
  start: Scalars['Int'];
  /** Search suggestions based on the query provided for alternate query texts */
  suggestions?: Maybe<Array<SearchSuggestion>>;
  /** The total number of search results matching the query and filters */
  total: Scalars['Int'];
};

/** Configuration for a search result */
export type SearchResultsVisualConfig = {
  __typename?: 'SearchResultsVisualConfig';
  /** Whether a search result should highlight the name/description if it was matched on those fields. */
  enableNameHighlight?: Maybe<Scalars['Boolean']>;
};

/** Input required in order to sort search results */
export type SearchSortInput = {
  /** A list of values to sort search results on */
  sortCriteria?: Maybe<Array<SortCriterion>>;
  /** A criterion to sort search results on */
  sortCriterion?: Maybe<SortCriterion>;
};

/**
 * A suggestion for an alternate search query given an original query compared to all
 * of the entity names in our search index.
 */
export type SearchSuggestion = {
  __typename?: 'SearchSuggestion';
  /** The number of entities that would match on the name field given the suggested text */
  frequency?: Maybe<Scalars['Int']>;
  /**
   * The "edit distance" for this suggestion. The closer this number is to 1, the
   * closer the suggested text is to the original text. The closer it is to 0, the
   * further from the original text it is.
   */
  score?: Maybe<Scalars['Float']>;
  /**
   * The suggested text based on the provided query text compared to
   * the entity name field in the search index.
   */
  text: Scalars['String'];
};

/** A referencible secret stored in DataHub's system. Notice that we do not return the actual secret value. */
export type Secret = {
  __typename?: 'Secret';
  /** An optional description for the secret */
  description?: Maybe<Scalars['String']>;
  /** The name of the secret */
  name: Scalars['String'];
  /** The urn of the secret */
  urn: Scalars['String'];
};

/** A plaintext secret value */
export type SecretValue = {
  __typename?: 'SecretValue';
  /** The name of the secret */
  name: Scalars['String'];
  /** The plaintext value of the secret. */
  value: Scalars['String'];
};

/** Configuration for semantic search including embedding generation settings */
export type SemanticSearchConfig = {
  __typename?: 'SemanticSearchConfig';
  /** Embedding generation configuration (nullable when semantic search is disabled) */
  embeddingConfig?: Maybe<EmbeddingConfig>;
  /** Whether semantic search is enabled on the server */
  enabled: Scalars['Boolean'];
  /** Entity types that support semantic search (e.g., ["document"]) */
  enabledEntities: Array<Scalars['String']>;
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

/** A service account represents a non-human identity for programmatic API access. */
export type ServiceAccount = Entity & {
  __typename?: 'ServiceAccount';
  /**
   * The time when the service account was created.
   * May be null in list operations where full entity details are not fetched.
   */
  createdAt?: Maybe<Scalars['Long']>;
  /** The actor URN that created this service account. */
  createdBy?: Maybe<Scalars['String']>;
  /** An optional description of the service account. */
  description?: Maybe<Scalars['String']>;
  /** The display name of the service account. */
  displayName?: Maybe<Scalars['String']>;
  /** The unique name/identifier of the service account. */
  name: Scalars['String'];
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The time when the service account was last updated. */
  updatedAt?: Maybe<Scalars['Long']>;
  /** The primary key of the service account */
  urn: Scalars['String'];
};


/** A service account represents a non-human identity for programmatic API access. */
export type ServiceAccountRelationshipsArgs = {
  input: RelationshipsInput;
};

export type SetLogicalParentInput = {
  parentUrn?: Maybe<Scalars['String']>;
  resourceUrn: Scalars['String'];
};

/** Metadata about the entity's siblings */
export type SiblingProperties = {
  __typename?: 'SiblingProperties';
  /** If this entity is the primary sibling among the sibling set */
  isPrimary?: Maybe<Scalars['Boolean']>;
  /** The sibling entities */
  siblings?: Maybe<Array<Maybe<Entity>>>;
};

/** A single sorting criterion for sorting search. */
export type SortCriterion = {
  /** A field upon which we'll do sorting on. */
  field: Scalars['String'];
  /** The order in which we will be sorting */
  sortOrder: SortOrder;
};

/** Order for sorting */
export enum SortOrder {
  Ascending = 'ASCENDING',
  Descending = 'DESCENDING'
}

export type SortQueriesInput = {
  /** A criterion to sort query results on */
  sortCriterion: SortCriterion;
};

export type SourceCode = {
  __typename?: 'SourceCode';
  /** Source Code along with types */
  sourceCode?: Maybe<Array<SourceCodeUrl>>;
};

export type SourceCodeUrl = {
  __typename?: 'SourceCodeUrl';
  /** Source Code Url */
  sourceCodeUrl: Scalars['String'];
  /** Source Code Url Types */
  type: SourceCodeUrlType;
};

export enum SourceCodeUrlType {
  /** Evaluation Pipeline Source Code */
  EvaluationPipelineSourceCode = 'EVALUATION_PIPELINE_SOURCE_CODE',
  /** MLModel Source Code */
  MlModelSourceCode = 'ML_MODEL_SOURCE_CODE',
  /** Training Pipeline Source Code */
  TrainingPipelineSourceCode = 'TRAINING_PIPELINE_SOURCE_CODE'
}

/** Attributes defining a SQL Assertion */
export type SqlAssertionInfo = {
  __typename?: 'SqlAssertionInfo';
  /**
   * The type of the value used to evaluate the assertion: a fixed absolute value or a relative percentage.
   * Required if the type is METRIC_CHANGE.
   */
  changeType?: Maybe<AssertionValueChangeType>;
  /** The entity targeted by this SQL check. */
  entityUrn: Scalars['String'];
  /** The operator you'd like to apply to the result of the SQL query. */
  operator: AssertionStdOperator;
  /** The parameters you'd like to provide as input to the operator. */
  parameters: AssertionStdParameters;
  /** The SQL statement to be executed when evaluating the assertion. */
  statement: Scalars['String'];
  /** The type of the SQL assertion being monitored. */
  type: SqlAssertionType;
};

/** The type of the SQL assertion being monitored. */
export enum SqlAssertionType {
  /** A SQL Metric Assertion, e.g. one based on a numeric value returned by an arbitrary SQL query. */
  Metric = 'METRIC',
  /** A SQL assertion that is evaluated against the CHANGE in a metric assertion over time. */
  MetricChange = 'METRIC_CHANGE'
}

/** The status of a particular Metadata Entity */
export type Status = {
  __typename?: 'Status';
  /** Whether the entity is removed or not */
  removed: Scalars['Boolean'];
};

/** A well-supported, standard DataHub Data Type. */
export enum StdDataType {
  /** Date data type in format YYYY-MM-DD */
  Date = 'DATE',
  /** Number data type */
  Number = 'NUMBER',
  /** Any other data type - refer to a provided data type urn. */
  Other = 'OTHER',
  /** Rich text data type. Right now this is markdown only. */
  RichText = 'RICH_TEXT',
  /** String data type */
  String = 'STRING',
  /** Urn data type */
  Urn = 'URN'
}

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

/** String property value */
export type StringValue = {
  __typename?: 'StringValue';
  /** The value of a string type property */
  stringValue: Scalars['String'];
};

/** Object containing structured properties for an entity */
export type StructuredProperties = {
  __typename?: 'StructuredProperties';
  /** Structured properties on this entity */
  properties?: Maybe<Array<StructuredPropertiesEntry>>;
};

/** An entry in an structured properties list represented as a tuple */
export type StructuredPropertiesEntry = {
  __typename?: 'StructuredPropertiesEntry';
  /** The urn of the entity this property came from for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** Information about who, why, and how this metadata was applied */
  attribution?: Maybe<MetadataAttribution>;
  /** The key of the map entry */
  structuredProperty: StructuredPropertyEntity;
  /** The optional entities associated with the values if the values are entity urns */
  valueEntities?: Maybe<Array<Maybe<Entity>>>;
  /** The values of the structured property for this entity */
  values: Array<Maybe<PropertyValue>>;
};

/** Properties about an individual Query */
export type StructuredPropertyDefinition = {
  __typename?: 'StructuredPropertyDefinition';
  /** A list of allowed values that the property is allowed to take. */
  allowedValues?: Maybe<Array<AllowedValue>>;
  /**
   * The cardinality of a Structured Property determining whether one or multiple values
   * can be applied to the entity from this property.
   */
  cardinality?: Maybe<PropertyCardinality>;
  /** Audit stamp for when this structured property was created */
  created?: Maybe<ResolvedAuditStamp>;
  /** The description of this property */
  description?: Maybe<Scalars['String']>;
  /** The display name of this structured property */
  displayName?: Maybe<Scalars['String']>;
  /** Entity types that this structured property can be applied to */
  entityTypes: Array<EntityTypeEntity>;
  /** Whether or not this structured property is immutable */
  immutable: Scalars['Boolean'];
  /** Audit stamp for when this structured property was last modified */
  lastModified?: Maybe<ResolvedAuditStamp>;
  /** The fully qualified name of the property. This includes its namespace */
  qualifiedName: Scalars['String'];
  /**
   * Allows for type specialization of the valueType to be more specific about which
   * entity types are allowed, for example.
   */
  typeQualifier?: Maybe<TypeQualifier>;
  /** The type of this structured property */
  valueType: DataTypeEntity;
};

/** A structured property that can be shared between different entities */
export type StructuredPropertyEntity = Entity & {
  __typename?: 'StructuredPropertyEntity';
  /** Definition of this structured property including its name */
  definition: StructuredPropertyDefinition;
  /** Whether or not this entity exists on DataHub */
  exists?: Maybe<Scalars['Boolean']>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Definition of this structured property including its name */
  settings?: Maybe<StructuredPropertySettings>;
  /** A standard Entity Type */
  type: EntityType;
  /** A primary key associated with the structured property */
  urn: Scalars['String'];
};


/** A structured property that can be shared between different entities */
export type StructuredPropertyEntityRelationshipsArgs = {
  input: RelationshipsInput;
};

/** A prompt shown to the user to collect metadata about an entity */
export type StructuredPropertyInputParams = {
  /** The urn of the structured property being applied to an entity */
  structuredPropertyUrn: Scalars['String'];
  /** The list of values you want to apply on this structured property to an entity */
  values: Array<PropertyValueInput>;
};

/** A prompt shown to the user to collect metadata about an entity */
export type StructuredPropertyParams = {
  __typename?: 'StructuredPropertyParams';
  /** The structured property required for the prompt on this entity */
  structuredProperty: StructuredPropertyEntity;
};

/** Input for a structured property type prompt */
export type StructuredPropertyParamsInput = {
  /** The urn of the structured property for a given form prompt */
  urn: Scalars['String'];
};

/** Settings specific to a structured property entity */
export type StructuredPropertySettings = {
  __typename?: 'StructuredPropertySettings';
  /**
   * Whether or not this asset should be hidden in the asset sidebar (showInAssetSummary should be enabled)
   * when its value is empty
   */
  hideInAssetSummaryWhenEmpty: Scalars['Boolean'];
  /** Whether or not this asset should be hidden in the main application */
  isHidden: Scalars['Boolean'];
  /** Whether or not this asset should be displayed as an asset badge on other asset's headers */
  showAsAssetBadge: Scalars['Boolean'];
  /** Whether or not this asset should be displayed in the asset sidebar */
  showInAssetSummary: Scalars['Boolean'];
  /** Whether or not this asset should be displayed as a column in the schema field table in a Dataset's "Columns" tab. */
  showInColumnsTable: Scalars['Boolean'];
  /** Whether or not this asset should be displayed as a search filter */
  showInSearchFilters: Scalars['Boolean'];
};

/** Settings for a structured property */
export type StructuredPropertySettingsInput = {
  /**
   * Whether or not this asset should be hidden in the asset sidebar (showInAssetSummary should be enabled)
   * when its value is empty
   */
  hideInAssetSummaryWhenEmpty?: Maybe<Scalars['Boolean']>;
  /** Whether or not this asset should be hidden in the main application */
  isHidden?: Maybe<Scalars['Boolean']>;
  /** Whether or not this asset should be displayed as an asset badge on other asset's headers */
  showAsAssetBadge?: Maybe<Scalars['Boolean']>;
  /** Whether or not this asset should be displayed in the asset sidebar */
  showInAssetSummary?: Maybe<Scalars['Boolean']>;
  /** Whether or not this asset should be displayed as a column in the schema field table in a Dataset's "Columns" tab. */
  showInColumnsTable?: Maybe<Scalars['Boolean']>;
  /** Whether or not this asset should be displayed as a search filter */
  showInSearchFilters?: Maybe<Scalars['Boolean']>;
};

/** A flexible carrier for structured results of an execution request. */
export type StructuredReport = {
  __typename?: 'StructuredReport';
  /** The content-type of the serialized value (e.g. application/json, application/json;gzip etc.) */
  contentType: Scalars['String'];
  /** The serialized value of the structured report */
  serializedValue: Scalars['String'];
  /** The type of the structured report. (e.g. INGESTION_REPORT, TEST_CONNECTION_REPORT, etc.) */
  type: Scalars['String'];
};

/** A type of Metadata Entity sub resource */
export enum SubResourceType {
  /** A Dataset field or column */
  DatasetField = 'DATASET_FIELD'
}

export type SubTypes = {
  __typename?: 'SubTypes';
  /** The sub-types that this entity implements. e.g. Datasets that are views will implement the "view" subtype */
  typeNames?: Maybe<Array<Scalars['String']>>;
};

/** Input for responding to a singular prompt in a form */
export type SubmitFormPromptInput = {
  /**
   * The fieldPath on a schema field that this prompt submission is association with.
   * This should be provided when the prompt is type FIELDS_STRUCTURED_PROPERTY
   */
  fieldPath?: Maybe<Scalars['String']>;
  /** The urn of the form that this prompt is a part of */
  formUrn: Scalars['String'];
  /** The unique ID of the prompt this input is responding to */
  promptId: Scalars['String'];
  /** The structured property required for the prompt on this entity */
  structuredPropertyParams?: Maybe<StructuredPropertyInputParams>;
  /** The type of prompt that this input is responding to */
  type: FormPromptType;
};

/** Info for a given asset summary element */
export type SummaryElement = {
  __typename?: 'SummaryElement';
  /** The type of element/property */
  elementType: SummaryElementType;
  /** The structured property associated with this summary element if it is a STRUCTURED_PROPERTY type */
  structuredProperty?: Maybe<StructuredPropertyEntity>;
};

/** A summary element object for what to store on asset summaries */
export type SummaryElementInput = {
  /** The summary element type */
  elementType: SummaryElementType;
  /** The optional urn of the structured property for this element if elementType is STRUCTURED_PROPERTY */
  structuredPropertyUrn?: Maybe<Scalars['String']>;
};

/** Different types of elements in asset summaries */
export enum SummaryElementType {
  Created = 'CREATED',
  DocumentStatus = 'DOCUMENT_STATUS',
  DocumentType = 'DOCUMENT_TYPE',
  Domain = 'DOMAIN',
  GlossaryTerms = 'GLOSSARY_TERMS',
  LastModified = 'LAST_MODIFIED',
  Owners = 'OWNERS',
  StructuredProperty = 'STRUCTURED_PROPERTY',
  Tags = 'TAGS'
}

export type SupportsVersions = {
  /** Indicates that this entity is versioned and provides information about the version. */
  versionProperties?: Maybe<VersionProperties>;
};

export type SystemFreshness = {
  __typename?: 'SystemFreshness';
  /**
   * The latest timestamp in millis of the system that was used to respond to this query
   * In case a cache was consulted, this reflects the freshness of the cache
   * In case an index was consulted, this reflects the freshness of the index
   */
  freshnessMillis: Scalars['Long'];
  /** Name of the system */
  systemName: Scalars['String'];
};

export type SystemMetadataInput = {
  lastObserved?: Maybe<Scalars['Long']>;
  properties?: Maybe<Array<StringMapEntryInput>>;
  runId?: Maybe<Scalars['String']>;
};

/** For consumption by UI only */
export type TableChart = {
  __typename?: 'TableChart';
  columns: Array<Scalars['String']>;
  rows: Array<Row>;
  title: Scalars['String'];
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
  /**
   * Experimental API.
   * For fetching extra entities that do not have custom UI code yet
   */
  aspects?: Maybe<Array<RawAspect>>;
  /**
   * Deprecated, use properties.description field instead
   * @deprecated Field no longer supported
   */
  description?: Maybe<Scalars['String']>;
  /**
   * Additional read write properties about the Tag
   * Deprecated! Use 'properties' field instead.
   * @deprecated Field no longer supported
   */
  editableProperties?: Maybe<EditableTagProperties>;
  /**
   * A unique identifier for the Tag. Deprecated - Use properties.name field instead.
   * @deprecated Field no longer supported
   */
  name: Scalars['String'];
  /** Ownership metadata of the dataset */
  ownership?: Maybe<Ownership>;
  /** Additional properties about the Tag */
  properties?: Maybe<TagProperties>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** A standard Entity Type */
  type: EntityType;
  /** The primary key of the TAG */
  urn: Scalars['String'];
};


/** A Tag Entity, which can be associated with other Metadata Entities and subresources */
export type TagAspectsArgs = {
  input?: Maybe<AspectParams>;
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
  /** Reference back to the tagged urn for tracking purposes e.g. when sibling nodes are merged together */
  associatedUrn: Scalars['String'];
  /** Information about who, why, and how this metadata was applied */
  attribution?: Maybe<MetadataAttribution>;
  /** The context of how/why this tag is associated */
  context?: Maybe<Scalars['String']>;
  /** The tag itself */
  tag: Tag;
};

/** Input provided when updating the association between a Metadata Entity and a Tag */
export type TagAssociationInput = {
  /** The target Metadata Entity to add or remove the Tag to */
  resourceUrn: Scalars['String'];
  /** An optional sub resource identifier to attach the Tag to */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to attach the Tag to */
  subResourceType?: Maybe<SubResourceType>;
  /** The primary key of the Tag to add or remove */
  tagUrn: Scalars['String'];
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
  /** An optional RGB hex code for a Tag color, e.g. #FFFFFF */
  colorHex?: Maybe<Scalars['String']>;
  /** A description of the Tag */
  description?: Maybe<Scalars['String']>;
  /** A display name for the Tag */
  name: Scalars['String'];
};

/**
 * Deprecated, use addTag or removeTag mutations instead
 * An update for a particular Tag entity
 */
export type TagUpdateInput = {
  /** Description of the tag */
  description?: Maybe<Scalars['String']>;
  /** The display name of a Tag */
  name: Scalars['String'];
  /** Ownership metadata of the tag */
  ownership?: Maybe<OwnershipUpdate>;
  /** The primary key of the Tag */
  urn: Scalars['String'];
};

/** Configurations related to tracking users in the app */
export type TelemetryConfig = {
  __typename?: 'TelemetryConfig';
  /** Env variable for whether or not third party logging should be enabled for this instance */
  enableThirdPartyLogging?: Maybe<Scalars['Boolean']>;
};

/** Input provided when updating the association between a Metadata Entity and a Glossary Term */
export type TermAssociationInput = {
  /** The target Metadata Entity to add or remove the Glossary Term from */
  resourceUrn: Scalars['String'];
  /** An optional sub resource identifier to attach the Glossary Term to */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to attach the Glossary Term to */
  subResourceType?: Maybe<SubResourceType>;
  /** The primary key of the Glossary Term to add or remove */
  termUrn: Scalars['String'];
};

/** A type of Metadata Entity sub resource */
export enum TermRelationshipType {
  /** When a Term contains, or has a 'Has A' relationship with another Term */
  HasA = 'hasA',
  /** When a Term inherits from, or has an 'Is A' relationship with another Term */
  IsA = 'isA'
}

/** A metadata entity representing a DataHub Test */
export type Test = Entity & {
  __typename?: 'Test';
  /** The category of the Test (user defined) */
  category: Scalars['String'];
  /** Definition for the test */
  definition: TestDefinition;
  /** Description of the test */
  description?: Maybe<Scalars['String']>;
  /** The name of the Test */
  name: Scalars['String'];
  /** Unused for tests */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the Test itself */
  urn: Scalars['String'];
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
  /**
   * The string representation of the Test
   * Deprecated! JSON representation is no longer supported.
   */
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
  /** The test failed. */
  Failure = 'FAILURE',
  /** The test succeeded. */
  Success = 'SUCCESS'
}

/** A set of test results */
export type TestResults = {
  __typename?: 'TestResults';
  /** The tests failing */
  failing: Array<TestResult>;
  /** The tests passing */
  passing: Array<TestResult>;
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
  /** Unique id for the cell. */
  cellId: Scalars['String'];
  /** Title of the cell */
  cellTitle: Scalars['String'];
  /** Captures information about who created/last modified/deleted this TextCell and when */
  changeAuditStamps?: Maybe<ChangeAuditStamps>;
  /** The actual text in a TextCell in a Notebook */
  text: Scalars['String'];
};

/** Configuration for any custom theme-ing */
export type ThemeConfig = {
  __typename?: 'ThemeConfig';
  /** The optional custom theme ID to determine which theme config we use in the frontend */
  themeId?: Maybe<Scalars['String']>;
};

/** A time range used in fetching Usage statistics */
export enum TimeRange {
  /** All time */
  All = 'ALL',
  /** Last day */
  Day = 'DAY',
  /** Last half year */
  HalfYear = 'HALF_YEAR',
  /** Last month */
  Month = 'MONTH',
  /** Last quarter */
  Quarter = 'QUARTER',
  /** Last week */
  Week = 'WEEK',
  /** Last year */
  Year = 'YEAR'
}

/** A time series aspect, or a group of related metadata associated with an Entity and corresponding to a particular timestamp */
export type TimeSeriesAspect = {
  /** The timestamp associated with the time series aspect in milliseconds */
  timestampMillis: Scalars['Long'];
};

/** For consumption by UI only */
export type TimeSeriesChart = {
  __typename?: 'TimeSeriesChart';
  dateRange: DateRange;
  interval: DateInterval;
  lines: Array<NamedLine>;
  title: Scalars['String'];
};

/** A time window with a finite start and end time */
export type TimeWindow = {
  __typename?: 'TimeWindow';
  /** The end time of the time window */
  durationMillis: Scalars['Long'];
  /** The start time of the time window */
  startTimeMillis: Scalars['Long'];
};

/** A timeline parameter entry */
export type TimelineParameterEntry = {
  __typename?: 'TimelineParameterEntry';
  /** The key of the parameter */
  key?: Maybe<Scalars['String']>;
  /** The value of the parameter */
  value?: Maybe<Scalars['String']>;
};

/** A set of capabilities regarding our timerseries indices */
export type TimeseriesCapabilitiesResult = {
  __typename?: 'TimeseriesCapabilitiesResult';
  /** Information regarding asset stats */
  assetStats?: Maybe<AssetStatsResult>;
};

/**
 * Allows for type specialization of the valueType to be more specific about which
 * entity types are allowed, for example.
 */
export type TypeQualifier = {
  __typename?: 'TypeQualifier';
  /** The list of allowed entity types */
  allowedTypes?: Maybe<Array<EntityTypeEntity>>;
};

/** Input for specifying specific entity types as values */
export type TypeQualifierInput = {
  /** The list of allowed entity types as urns (ie. ["urn:li:entityType:datahub.corpuser"]) */
  allowedTypes?: Maybe<Array<Scalars['String']>>;
};

/** Input for unlinking a versioned entity from a Version Set */
export type UnlinkVersionInput = {
  /** The target versioned entity to unlink */
  unlinkedEntity?: Maybe<Scalars['String']>;
  /** The target version set */
  versionSet?: Maybe<Scalars['String']>;
};

/** Input properties required for update a Application */
export type UpdateApplicationInput = {
  /** An optional description for the Application */
  description?: Maybe<Scalars['String']>;
  /** A display name for the Application */
  name?: Maybe<Scalars['String']>;
};

/** Input required to update global applications settings. */
export type UpdateApplicationsSettingsInput = {
  /** Whether the Applications feature is enabled */
  enabled?: Maybe<Scalars['Boolean']>;
};

export type UpdateAssetSettingsInput = {
  /** Input related to the summary page of this asset */
  summary?: Maybe<UpdateAssetSummaryInput>;
  /** Urn of the asset you are updating */
  urn: Scalars['String'];
};

/** Input related to the summary page of this asset */
export type UpdateAssetSummaryInput = {
  /** The urn of the template you want to set for this asset summary page */
  template: Scalars['String'];
};

/** Input required to update Business Attribute */
export type UpdateBusinessAttributeInput = {
  /** business attribute description */
  description?: Maybe<Scalars['String']>;
  /** name of the business attribute */
  name?: Maybe<Scalars['String']>;
  /** type */
  type?: Maybe<SchemaFieldDataType>;
};

/** Input required to update a users settings. */
export type UpdateCorpUserViewsSettingsInput = {
  /**
   * The URN of the View that serves as this user's personal default.
   * If not provided, any existing default view will be removed.
   */
  defaultView?: Maybe<Scalars['String']>;
};

/** Input properties required for update a DataProduct */
export type UpdateDataProductInput = {
  /** An optional description for the DataProduct */
  description?: Maybe<Scalars['String']>;
  /** A display name for the DataProduct */
  name?: Maybe<Scalars['String']>;
};

/** Input provided when setting the Deprecation status for an Entity. */
export type UpdateDeprecationInput = {
  /** Optional - The time user plan to decommission this entity */
  decommissionTime?: Maybe<Scalars['Long']>;
  /** Whether the Entity is marked as deprecated. */
  deprecated: Scalars['Boolean'];
  /** Optional - Additional information about the entity deprecation plan */
  note?: Maybe<Scalars['String']>;
  /** Optional - URN to replace the entity with */
  replacement?: Maybe<Scalars['String']>;
  /** An optional sub resource identifier to set the deprecation for */
  subResource?: Maybe<Scalars['String']>;
  /** An optional type of a sub resource to set the deprecation for */
  subResourceType?: Maybe<SubResourceType>;
  /** The urn of the Entity to set deprecation for. */
  urn: Scalars['String'];
};

/** Input required to update doc propagation settings. */
export type UpdateDocPropagationSettingsInput = {
  /** The default doc propagation setting for the platform. */
  docColumnPropagation?: Maybe<Scalars['Boolean']>;
};

/** Input required to update the contents of a Document */
export type UpdateDocumentContentsInput = {
  /** The new text contents for the Document. If not provided, the existing contents will not be updated. */
  contents?: Maybe<DocumentContentInput>;
  /** Optional updated sub-type for the document (e.g., "FAQ", "Tutorial", "Reference") */
  subType?: Maybe<Scalars['String']>;
  /** Optional updated title for the document. If not provided, the existing title will not be updated. */
  title?: Maybe<Scalars['String']>;
  /** The URN of the Document to update */
  urn: Scalars['String'];
};

/** Input required to update the related entities of a Document */
export type UpdateDocumentRelatedEntitiesInput = {
  /** Optional URNs of related assets (will replace existing) */
  relatedAssets?: Maybe<Array<Scalars['String']>>;
  /** Optional URNs of related documents (will replace existing) */
  relatedDocuments?: Maybe<Array<Scalars['String']>>;
  /** The URN of the Document to update */
  urn: Scalars['String'];
};

/** Input required to update the settings of a Document */
export type UpdateDocumentSettingsInput = {
  /** Whether or not this document should be visible in the global context */
  showInGlobalContext: Scalars['Boolean'];
  /** The URN of the Document to update */
  urn: Scalars['String'];
};

/** Input required to update the status of a Document */
export type UpdateDocumentStatusInput = {
  /** The new state for the document */
  state: DocumentState;
  /** The URN of the Document to update */
  urn: Scalars['String'];
};

/** Input required to update the sub-type of a Document */
export type UpdateDocumentSubTypeInput = {
  /** The new sub-type for the document (e.g., "FAQ", "Tutorial", "Runbook"). Set to null to clear the sub-type. */
  subType?: Maybe<Scalars['String']>;
  /** The URN of the Document to update */
  urn: Scalars['String'];
};

/** Input required to set or clear information related to rendering a Data Asset inside of DataHub. */
export type UpdateEmbedInput = {
  /** Set or clear a URL used to render an embedded asset. */
  renderUrl?: Maybe<Scalars['String']>;
  /** The URN associated with the Data Asset to update. Only dataset, dashboard, and chart urns are currently supported. */
  urn: Scalars['String'];
};

/** Input for updating a form */
export type UpdateFormInput = {
  /** Information on how this form should be assigned to users/groups */
  actors?: Maybe<FormActorAssignmentUpdateInput>;
  /** The new description of the form */
  description?: Maybe<Scalars['String']>;
  /** The new name of the form */
  name?: Maybe<Scalars['String']>;
  /** The new prompts being added to this form */
  promptsToAdd?: Maybe<Array<CreatePromptInput>>;
  /** The IDs of the prompts to remove from this form */
  promptsToRemove?: Maybe<Array<Scalars['String']>>;
  /** The new type of the form */
  type?: Maybe<FormType>;
  /** The urn of the form being updated */
  urn: Scalars['String'];
};

/** Input required to update Global View Settings. */
export type UpdateGlobalViewsSettingsInput = {
  /**
   * The URN of the View that serves as the Global, or organization-wide, default.
   * If this field is not provided, the existing Global Default will be cleared.
   */
  defaultView?: Maybe<Scalars['String']>;
};

/** Input required to update an existing incident. */
export type UpdateIncidentInput = {
  /** An optional set of user or group assignee urns */
  assigneeUrns?: Maybe<Array<Scalars['String']>>;
  /** An optional description associated with the incident */
  description?: Maybe<Scalars['String']>;
  /** An optional priority for the incident. */
  priority?: Maybe<IncidentPriority>;
  /**
   * An optional set of resources that the incident is assigned to.
   * If defined, there must be at least one in the list.
   */
  resourceUrns?: Maybe<Array<Scalars['String']>>;
  /** An optional time at which the incident actually started (may be before the date it was raised). */
  startedAt?: Maybe<Scalars['Long']>;
  /** The status of the incident */
  status?: Maybe<IncidentStatusInput>;
  /** An optional title associated with the incident */
  title?: Maybe<Scalars['String']>;
};

/** Input required to update status of an existing incident */
export type UpdateIncidentStatusInput = {
  /** An optional message associated with the new state */
  message?: Maybe<Scalars['String']>;
  /** Optional - The new lifecycle stage of the incident */
  stage?: Maybe<IncidentStage>;
  /** The new state of the incident */
  state: IncidentState;
};

/** Input parameters for creating / updating an Ingestion Source */
export type UpdateIngestionSourceConfigInput = {
  /** Whether or not to run ingestion in debug mode */
  debugMode?: Maybe<Scalars['Boolean']>;
  /** The id of the executor to use for executing the recipe */
  executorId: Scalars['String'];
  /** Extra arguments for the ingestion run. */
  extraArgs?: Maybe<Array<StringMapEntryInput>>;
  /** A JSON-encoded recipe */
  recipe: Scalars['String'];
  /** The version of DataHub Ingestion Framework to use when executing the recipe. */
  version?: Maybe<Scalars['String']>;
};

/** Input arguments for creating / updating an Ingestion Source */
export type UpdateIngestionSourceInput = {
  /** A set of type-specific ingestion source configurations */
  config: UpdateIngestionSourceConfigInput;
  /** An optional description associated with the ingestion source */
  description?: Maybe<Scalars['String']>;
  /** A name associated with the ingestion source */
  name: Scalars['String'];
  /** An optional schedule for the ingestion source. If not provided, the source is only available for run on-demand. */
  schedule?: Maybe<UpdateIngestionSourceScheduleInput>;
  /** Optionally specify source */
  source?: Maybe<IngestionSourceSourceInput>;
  /** The type of the source itself, e.g. mysql, bigquery, bigquery-usage. Should match the recipe. */
  type: Scalars['String'];
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

/** Input provided when updating the association between a Metadata Entity and a Link */
export type UpdateLinkInput = {
  /** Current label of the link */
  currentLabel: Scalars['String'];
  /** Current url of the link */
  currentUrl: Scalars['String'];
  /** The new label of the link */
  label: Scalars['String'];
  /** The new url of the link */
  linkUrl: Scalars['String'];
  /** The urn of the resource or entity to attach the link to, for example a dataset urn */
  resourceUrn: Scalars['String'];
  /** The new optional settings input for the link */
  settings?: Maybe<LinkSettingsInput>;
};

/** Input provided for filling in a post content */
export type UpdateMediaInput = {
  /** The location of the media (a URL) */
  location: Scalars['String'];
  /** The type of media */
  type: MediaType;
};

/** Input for updating the name of an entity */
export type UpdateNameInput = {
  /** The new name */
  name: Scalars['String'];
  /** The primary key of the resource to update the name for */
  urn: Scalars['String'];
};

export type UpdateOwnershipTypeInput = {
  /** The description of the Custom Ownership Type */
  description?: Maybe<Scalars['String']>;
  /** The name of the Custom Ownership Type */
  name?: Maybe<Scalars['String']>;
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
  /** Optional content of the post */
  description?: Maybe<Scalars['String']>;
  /** Optional link that the post is associated with */
  link?: Maybe<Scalars['String']>;
  /** Optional media contained in the post */
  media?: Maybe<UpdateMediaInput>;
  /** The title of the post */
  title: Scalars['String'];
};

/** Input provided when creating a Post */
export type UpdatePostInput = {
  /** The content of the post */
  content: UpdatePostContentInput;
  /** The type of post */
  postType: PostType;
  /** The urn of the post to edit or update */
  urn: Scalars['String'];
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
  /** An optional description for the Query */
  description?: Maybe<Scalars['String']>;
  /** An optional display name for the Query */
  name?: Maybe<Scalars['String']>;
  /** The Query contents */
  statement?: Maybe<QueryStatementInput>;
};

/** Input required for creating a Query. For now, only datasets are supported. */
export type UpdateQuerySubjectInput = {
  /** The urn of the dataset that is the subject of the query */
  datasetUrn: Scalars['String'];
};

/** Input arguments for updating a Secret */
export type UpdateSecretInput = {
  /** An optional description for the secret */
  description?: Maybe<Scalars['String']>;
  /** The name of the secret for reference in ingestion recipes */
  name: Scalars['String'];
  /** The primary key of the Secret to update */
  urn: Scalars['String'];
  /** The value of the secret, to be encrypted and stored */
  value: Scalars['String'];
};

/** Result returned when fetching step state */
export type UpdateStepStateResult = {
  __typename?: 'UpdateStepStateResult';
  /** Id of the step */
  id: Scalars['String'];
  /** Whether the update succeeded. */
  succeeded: Scalars['Boolean'];
};

/** Input for updating an existing structured property entity */
export type UpdateStructuredPropertyInput = {
  /** The optional description for this property */
  description?: Maybe<Scalars['String']>;
  /** The optional display name for this property */
  displayName?: Maybe<Scalars['String']>;
  /** Whether the property will be mutable once it is applied or not. Default is false. */
  immutable?: Maybe<Scalars['Boolean']>;
  /**
   * Append to the list of allowed values for this property.
   * For backwards compatibility, this is append only.
   */
  newAllowedValues?: Maybe<Array<AllowedValueInput>>;
  /**
   * Append to the list of entity types that this property can be applied to.
   * For backwards compatibility, this is append only.
   */
  newEntityTypes?: Maybe<Array<Scalars['String']>>;
  /**
   * Set to true if you want to change the cardinality of this structured property
   * to multiple. Cannot change from multiple to single for backwards compatibility reasons.
   */
  setCardinalityAsMultiple?: Maybe<Scalars['Boolean']>;
  /** Settings for this structured property */
  settings?: Maybe<StructuredPropertySettingsInput>;
  /** The optional input for specifying specific entity types as values */
  typeQualifier?: Maybe<UpdateTypeQualifierInput>;
  /** The urn of the structured property being updated */
  urn: Scalars['String'];
};

export type UpdateTestInput = {
  /** The category of the Test (user defined) */
  category: Scalars['String'];
  /** The test definition */
  definition: TestDefinitionInput;
  /** Description of the test */
  description?: Maybe<Scalars['String']>;
  /** The name of the Test */
  name: Scalars['String'];
};

/** Input for updating specifying specific entity types as values */
export type UpdateTypeQualifierInput = {
  /**
   * Append to the list of allowed entity types as urns for this property (ie. ["urn:li:entityType:datahub.corpuser"])
   * For backwards compatibility, this is append only.
   */
  newAllowedTypes?: Maybe<Array<Scalars['String']>>;
};

/** Input required to update a user's home page settings. */
export type UpdateUserHomePageSettingsInput = {
  /** The list of urns of announcement posts dismissed by the user. */
  newDismissedAnnouncements?: Maybe<Array<Maybe<Scalars['String']>>>;
  /** The URN of the page template to be rendered on the home page for the user. */
  pageTemplate?: Maybe<Scalars['String']>;
  /** Whether to remove the page template for the user. */
  removePageTemplate?: Maybe<Scalars['Boolean']>;
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
  /** The view definition itself */
  definition?: Maybe<DataHubViewDefinitionInput>;
  /** An optional description of the View */
  description?: Maybe<Scalars['String']>;
  /** The name of the View */
  name?: Maybe<Scalars['String']>;
};

/** Enum to specify the context of the upload. */
export enum UploadDownloadScenario {
  /** Upload for asset documentation. */
  AssetDocumentation = 'ASSET_DOCUMENTATION',
  /** Upload for asset documentation links. */
  AssetDocumentationLinks = 'ASSET_DOCUMENTATION_LINKS'
}

/** Input for upserting a Custom Assertion. */
export type UpsertCustomAssertionInput = {
  /** The description of this assertion. */
  description: Scalars['String'];
  /** The entity targeted by this assertion. */
  entityUrn: Scalars['String'];
  /** Native platform URL of the Assertion */
  externalUrl?: Maybe<Scalars['String']>;
  /** The dataset field targeted by this assertion, if any. */
  fieldPath?: Maybe<Scalars['String']>;
  /** Logic comprising a raw, unstructured assertion. for example - custom SQL query for the assertion. */
  logic?: Maybe<Scalars['String']>;
  /** The external Platform associated with the assertion */
  platform: PlatformInput;
  /** The type of the custom assertion. */
  type: Scalars['String'];
};

/** Input required to upsert a Data Contract entity for an asset */
export type UpsertDataContractInput = {
  /** The data quality portion of the contract. If not provided, this will be set to none. */
  dataQuality?: Maybe<Array<DataQualityContractInput>>;
  /** The urn of the related entity. Dataset is the only entity type supported today. */
  entityUrn: Scalars['String'];
  /**
   * The Freshness / Freshness portion of the contract. If not provided, this will be set to none.
   * For Dataset Contracts, it is expected that there will not be more than 1 Freshness contract. If there are, only the first will be displayed.
   */
  freshness?: Maybe<Array<FreshnessContractInput>>;
  /**
   * Optional ID of the contract you want to create. Only applicable if this is a create operation. If not provided, a random
   * id will be generated for you.
   */
  id?: Maybe<Scalars['String']>;
  /**
   * The schema / structural portion of the contract. If not provided, this will be set to none.
   * For Dataset Contracts, it is expected that there will not be more than 1 Schema contract. If there are, only the first will be displayed.
   */
  schema?: Maybe<Array<SchemaContractInput>>;
  /** The state of the data contract. If not provided, it will be in ACTIVE mode by default. */
  state?: Maybe<DataContractState>;
};

/** Input required to upsert a new DataHub connection. */
export type UpsertDataHubConnectionInput = {
  /**
   * An optional ID to use when creating the URN of the connection. If none is provided,
   * a random UUID will be generated automatically.
   */
  id?: Maybe<Scalars['String']>;
  /** A JSON-encoded connection. This must be present when type is JSON. */
  json?: Maybe<DataHubJsonConnectionInput>;
  /** An optional name for this connection entity */
  name?: Maybe<Scalars['String']>;
  /** Urn of the associated platform */
  platformUrn: Scalars['String'];
  /** The type or format of connection */
  type: DataHubConnectionDetailsType;
};

/** Input provided when upsert the association between a Metadata Entity and a Link */
export type UpsertLinkInput = {
  /** The url of the link to add or update */
  label: Scalars['String'];
  /** The url of the link to add or update */
  linkUrl: Scalars['String'];
  /** The urn of the resource or entity to attach the link to, for example a dataset urn */
  resourceUrn: Scalars['String'];
  /** Optional settings input for this link */
  settings?: Maybe<LinkSettingsInput>;
};

/** Input for creating or updating a DataHub page module */
export type UpsertPageModuleInput = {
  /** The display name of this module */
  name: Scalars['String'];
  /** The specific parameters stored for this module */
  params: PageModuleParamsInput;
  /** The scope of this module and who can use/see it */
  scope: PageModuleScope;
  /** The type of this module */
  type: DataHubPageModuleType;
  /** The URN of the page module to update. If not provided, a new module will be created. */
  urn?: Maybe<Scalars['String']>;
};

/** Input for adding or updating a DataHubPageTemplate entity */
export type UpsertPageTemplateInput = {
  /** The optional info for asset summaries on this template */
  assetSummary?: Maybe<PageTemplateAssetSummaryInput>;
  /** The rows of a page template */
  rows: Array<PageTemplateRowInput>;
  /** The scope of the template ie. is it personal or global */
  scope: PageTemplateScope;
  /** The area that this template is used in */
  surfaceType: PageTemplateSurfaceType;
  /** The urn of the page template if updating, empty if creating a new page template */
  urn?: Maybe<Scalars['String']>;
};

/** Input for upserting structured properties on a given asset */
export type UpsertStructuredPropertiesInput = {
  /** The urn of the asset that we are updating */
  assetUrn: Scalars['String'];
  /** The list of structured properties you want to upsert on this asset */
  structuredPropertyInputParams: Array<StructuredPropertyInputParams>;
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
  /** The rolled up usage metrics */
  metrics?: Maybe<UsageAggregationMetrics>;
  /** The resource urn associated with the usage information, eg a Dataset urn */
  resource?: Maybe<Scalars['String']>;
};

/** Rolled up metrics about Dataset usage over time */
export type UsageAggregationMetrics = {
  __typename?: 'UsageAggregationMetrics';
  /** Per field usage statistics within the time range */
  fields?: Maybe<Array<Maybe<FieldUsageCounts>>>;
  /** A set of common queries issued against the dataset within the time range */
  topSqlQueries?: Maybe<Array<Maybe<Scalars['String']>>>;
  /** The total number of queries issued against the dataset within the time range */
  totalSqlQueries?: Maybe<Scalars['Int']>;
  /** The unique number of users who have queried the dataset within the time range */
  uniqueUserCount?: Maybe<Scalars['Int']>;
  /** Usage statistics within the time range by user */
  users?: Maybe<Array<Maybe<UserUsageCounts>>>;
};

/** The result of a Dataset usage query */
export type UsageQueryResult = {
  __typename?: 'UsageQueryResult';
  /** A set of rolled up aggregations about the Dataset usage */
  aggregations?: Maybe<UsageQueryResultAggregations>;
  /** A set of relevant time windows for use in displaying usage statistics */
  buckets?: Maybe<Array<Maybe<UsageAggregation>>>;
};

/** A set of rolled up aggregations about the Dataset usage */
export type UsageQueryResultAggregations = {
  __typename?: 'UsageQueryResultAggregations';
  /** The specific per field usage counts within the queried time range */
  fields?: Maybe<Array<Maybe<FieldUsageCounts>>>;
  /**
   * The total number of queries executed within the queried time range
   * Note that this field will likely be deprecated in favor of a totalQueries field
   */
  totalSqlQueries?: Maybe<Scalars['Int']>;
  /** The count of unique Dataset users within the queried time range */
  uniqueUserCount?: Maybe<Scalars['Int']>;
  /** The specific per user usage counts within the queried time range */
  users?: Maybe<Array<Maybe<UserUsageCounts>>>;
};

/** An individual setting type for a Corp User. */
export enum UserSetting {
  /** Show simplified homepage */
  ShowSimplifiedHomepage = 'SHOW_SIMPLIFIED_HOMEPAGE',
  /** Show theme v2 */
  ShowThemeV2 = 'SHOW_THEME_V2'
}

/** Information about individual user usage of a Dataset */
export type UserUsageCounts = {
  __typename?: 'UserUsageCounts';
  /** The number of queries issued by the user */
  count?: Maybe<Scalars['Int']>;
  /** The user of the Dataset */
  user?: Maybe<CorpUser>;
  /**
   * The extracted user email
   * Note that this field will soon be deprecated and merged with user
   */
  userEmail?: Maybe<Scalars['String']>;
};

/** A frequency distribution of a specific value within a dataset */
export type ValueFrequency = {
  __typename?: 'ValueFrequency';
  /** Volume of the value */
  frequency: Scalars['Long'];
  /** Specific value. For numeric columns, the value will contain a stringified value */
  value: Scalars['String'];
};

/** Input for verifying forms on entities */
export type VerifyFormInput = {
  /** The urn of the entity that is having a form verified on it */
  entityUrn: Scalars['String'];
  /** The urn of the form being verified on an entity */
  formUrn: Scalars['String'];
};

export type VersionProperties = {
  __typename?: 'VersionProperties';
  /** Additional version identifiers for this versioned asset. */
  aliases: Array<VersionTag>;
  /** Comment documenting what this version was created for, changes, or represents */
  comment?: Maybe<Scalars['String']>;
  /** Timestamp reflecting when the metadata for this version was created in DataHub */
  created?: Maybe<ResolvedAuditStamp>;
  /** Timestamp reflecting when the metadata for this version was created in DataHub */
  createdInSource?: Maybe<ResolvedAuditStamp>;
  /** Whether this version is currently the latest in its verison set */
  isLatest: Scalars['Boolean'];
  /** Label for this versioned asset, should be unique within a version set (not enforced) */
  version: VersionTag;
  /** The linked Version Set entity that ties multiple versioned assets together */
  versionSet: VersionSet;
};

export type VersionSet = Entity & {
  __typename?: 'VersionSet';
  /** The latest versioned entity linked to in this version set */
  latestVersion?: Maybe<Entity>;
  /** Granular API for querying edges extending from this entity */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the VersionSet */
  urn: Scalars['String'];
  /**
   * Executes a search on all versioned entities linked to this version set
   * By default sorts by sortId in descending order
   */
  versionsSearch?: Maybe<SearchResults>;
};


export type VersionSetRelationshipsArgs = {
  input: RelationshipsInput;
};


export type VersionSetVersionsSearchArgs = {
  input: SearchAcrossEntitiesInput;
};

/** The technical version associated with a given Metadata Entity */
export type VersionTag = {
  __typename?: 'VersionTag';
  versionTag?: Maybe<Scalars['String']>;
};

/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type VersionedDataset = Entity & {
  __typename?: 'VersionedDataset';
  /**
   * Deprecated, use applications instead
   * The application associated with the entity
   * @deprecated Use applications instead
   */
  application?: Maybe<ApplicationAssociation>;
  /** The applications associated with the entity */
  applications?: Maybe<Array<ApplicationAssociation>>;
  /** The parent container in which the entity resides */
  container?: Maybe<Container>;
  /** The deprecation status of the dataset */
  deprecation?: Maybe<Deprecation>;
  /** The Domain associated with the Dataset */
  domain?: Maybe<DomainAssociation>;
  /** An additional set of of read write properties */
  editableProperties?: Maybe<DatasetEditableProperties>;
  /** Editable schema metadata of the dataset */
  editableSchemaMetadata?: Maybe<EditableSchemaMetadata>;
  /** The structured glossary terms associated with the dataset */
  glossaryTerms?: Maybe<GlossaryTerms>;
  /** Experimental! The resolved health status of the asset */
  health?: Maybe<Array<Health>>;
  /** References to internal resources related to the dataset */
  institutionalMemory?: Maybe<InstitutionalMemory>;
  /**
   * Unique guid for dataset
   * No longer to be used as the Dataset display name. Use properties.name instead
   */
  name: Scalars['String'];
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
  /** An additional set of read only properties */
  properties?: Maybe<DatasetProperties>;
  /** No-op, has to be included due to model */
  relationships?: Maybe<EntityRelationshipsResult>;
  /** Schema metadata of the dataset */
  schema?: Maybe<Schema>;
  /** Status of the Dataset */
  status?: Maybe<Status>;
  /** Sub Types that this entity implements */
  subTypes?: Maybe<SubTypes>;
  /** Tags used for searching dataset */
  tags?: Maybe<GlobalTags>;
  /** The standard Entity Type */
  type: EntityType;
  /** The primary key of the Dataset */
  urn: Scalars['String'];
  /** View related properties. Only relevant if subtypes field contains view. */
  viewProperties?: Maybe<ViewProperties>;
};


/** A Dataset entity, which encompasses Relational Tables, Document store collections, streaming topics, and other sets of data having an independent lifecycle */
export type VersionedDatasetRelationshipsArgs = {
  input: RelationshipsInput;
};

/** Properties about a Dataset of type view */
export type ViewProperties = {
  __typename?: 'ViewProperties';
  /**
   * A formatted version of the logic associated with the view.
   * For dbt, this contains the compiled SQL.
   */
  formattedLogic?: Maybe<Scalars['String']>;
  /** The language in which the view logic is written, for example SQL */
  language: Scalars['String'];
  /** The logic associated with the view, most commonly a SQL statement */
  logic: Scalars['String'];
  /** Whether the view is materialized or not */
  materialized: Scalars['Boolean'];
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
  /** Custom app title to show in the browser tab */
  appTitle?: Maybe<Scalars['String']>;
  /** Configuration for the application sidebar section */
  application?: Maybe<ApplicationConfig>;
  /** Configuration for the queries tab */
  entityProfiles?: Maybe<EntityProfilesConfig>;
  /** Custom favicon url for the homepage & top banner */
  faviconUrl?: Maybe<Scalars['String']>;
  /** Boolean flag disabling viewing the Business Glossary page for users without the 'Manage Glossaries' privilege */
  hideGlossary?: Maybe<Scalars['Boolean']>;
  /** Custom logo url for the homepage & top banner */
  logoUrl?: Maybe<Scalars['String']>;
  /** Configuration for the queries tab */
  queriesTab?: Maybe<QueriesTabConfig>;
  /** Configuration for search results */
  searchResult?: Maybe<SearchResultsVisualConfig>;
  /** Show full title in lineage view by default */
  showFullTitleInLineage?: Maybe<Scalars['Boolean']>;
  /** Configuration for custom theme-ing */
  theme?: Maybe<ThemeConfig>;
};

/** A definition of a Volume (row count) assertion. */
export type VolumeAssertionInfo = {
  __typename?: 'VolumeAssertionInfo';
  /** The entity targeted by this Volume check. */
  entityUrn: Scalars['String'];
  /**
   * A definition of the specific filters that should be applied, when performing monitoring.
   * If not provided, there is no filter, and the full table is under consideration.
   */
  filter?: Maybe<DatasetFilter>;
  /**
   * Produce FAILURE Assertion Result if the incrementing segment row count delta of the asset
   * does not meet specific requirements. Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_CHANGE'.
   */
  incrementingSegmentRowCountChange?: Maybe<IncrementingSegmentRowCountChange>;
  /**
   * Produce FAILURE Assertion Result if the latest incrementing segment row count total of the asset
   * does not meet specific requirements. Required if type is 'INCREMENTING_SEGMENT_ROW_COUNT_TOTAL'.
   */
  incrementingSegmentRowCountTotal?: Maybe<IncrementingSegmentRowCountTotal>;
  /**
   * Produce FAILURE Assertion Result if the row count delta of the asset does not meet specific requirements.
   * Required if type is 'ROW_COUNT_CHANGE'.
   */
  rowCountChange?: Maybe<RowCountChange>;
  /**
   * Produce FAILURE Assertion Result if the row count of the asset does not meet specific requirements.
   * Required if type is 'ROW_COUNT_TOTAL'.
   */
  rowCountTotal?: Maybe<RowCountTotal>;
  /** The type of the freshness assertion being monitored. */
  type: VolumeAssertionType;
};

/** A type of volume (row count) assertion */
export enum VolumeAssertionType {
  /**
   * A volume assertion that compares the row counts in neighboring "segments" or "partitions"
   * of an incrementing column. This can be used to track changes between subsequent date partition
   * in a table, for example.
   */
  IncrementingSegmentRowCountChange = 'INCREMENTING_SEGMENT_ROW_COUNT_CHANGE',
  /**
   * A volume assertion that checks the latest "segment" in a table based on an incrementing
   * column to check whether it's row count falls into a particular range.
   * This can be used to monitor the row count of an incrementing date-partition column segment.
   */
  IncrementingSegmentRowCountTotal = 'INCREMENTING_SEGMENT_ROW_COUNT_TOTAL',
  /**
   * A volume assertion that is evaluated against an incremental row count of a dataset,
   * or a row count change.
   */
  RowCountChange = 'ROW_COUNT_CHANGE',
  /** A volume assertion that is evaluated against the total row count of a dataset. */
  RowCountTotal = 'ROW_COUNT_TOTAL'
}

/** The duration of a fixed window of time */
export enum WindowDuration {
  /** A one day window */
  Day = 'DAY',
  /** A one month window */
  Month = 'MONTH',
  /** A one week window */
  Week = 'WEEK',
  /** A one year window */
  Year = 'YEAR'
}
