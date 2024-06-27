/**
 * The "log level" for log items reported from ingestion
 */
export enum StructuredReportItemLevel {
    /**
     * An error log
     */
    ERROR,
    /**
     * A warn log
     */
    WARN,
    /**
     * An info log - generally unused.
     */
    INFO,
}

/**
 * A set of standard / well supported warnings or error types
 */
export enum StructuredReportItemType {
    /**
     * Unauthorized to scan a specific part of the source - database, schema, project, or etc or a specific asset.
     */
    SCAN_UNAUTHORIZED,
    /**
     * Unauthorized to access lineage information.
     */
    LINEAGE_UNAUTHORIZED,
    /**
     * Unauthorized to access usage information - recent queries.
     */
    USAGE_UNAUTHORIZED,
    /**
     * Unauthorized to profile some tables.
     */
    PROFILING_UNAUTHORIZED,
    /**
     * Failure to parse some queries to extract column or asset-level lineage.
     */
    LINEAGE_QUERY_PARSING_FAILED,
    /**
     * Failure to parse some queries
     */
    USAGE_QUERY_PARSING_FAILED,
    /**
     * Failure to connect to the data source due to malformed connection details
     */
    CONNECTION_FAILED_COORDINATES,
    /**
     * Failure to connect to the data source due to bad credentials
     */
    CONNECTION_FAILED_CREDENTIALS,
    /**
     * Failure to connect to the data source due to unavailability of 3rd party service.
     */
    CONNECTION_FAILED_SERVICE_UNAVAILABLE,
    /**
     * Failure to connect to the data source due to a client-side timeout.
     */
    CONNECTION_FAILED_SERVICE_TIMEOUT,
    /**
     * Failure to connect to the data source for an unknown reason.
     */
    CONNECTION_FAILED_UNKNOWN,
    /**
     * Fallback type for unrecognized structured report lines.
     */
    UNKNOWN,
}

/**
 * A type describing an individual warning / failure item in a structured report.
 *
 * TODO: Determine whether we need a message field to be reported!
 */
export interface StructuredReportItem {
    level: StructuredReportItemLevel; // The "log level"
    title: string; // The "well-supported" or standardized title
    message: string; // The message to display associated with the error.
    context: string[]; // The context of WHERE the issue was encountered, as a string.
    rawType: string; // The "raw type" string received from the ingestion backend.
}

/**
 * A type describing a structured ingestion report.
 */
export interface StructuredReport {
    infoCount: number;
    errorCount: number;
    warnCount: number;
    items: StructuredReportItem[];
}

/**
 * A mapping of the frontend standardized error types to their messages and the raw backend error types that they are mapped from.
 */
export const STRUCTURED_REPORT_ITEM_DISPLAY_DETAILS = [
    {
        type: StructuredReportItemType.UNKNOWN,
        title: 'An unexpected issue occurred',
    },
    {
        type: StructuredReportItemType.SCAN_UNAUTHORIZED,
        title: 'Unauthorized to scan some assets',
        message: 'The provided credential details were unauthorized to scan some assets in the data source.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.LINEAGE_UNAUTHORIZED,
        title: 'Unauthorized to extract some lineage',
        message:
            'The provided credential details were unauthorized to extract some asset lineage from the data source.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.USAGE_UNAUTHORIZED,
        title: 'Unauthorized to extract some usage',
        message:
            'The provided credential details were unauthorized to extract some asset usage information from the data source.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.PROFILING_UNAUTHORIZED,
        title: 'Unauthorized to extract some data statistics',
        message:
            'The provided credential details were unauthorized to extract some asset profiles or statistics from the data source.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.LINEAGE_QUERY_PARSING_FAILED,
        title: 'Failed to extract some lineage',
        message: 'Failed to extract lineage for some assets due to failed query parsing.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.USAGE_QUERY_PARSING_FAILED,
        title: 'Failed to extract some usage',
        message: 'Failed to extract usage or popularity for some assets due to failed query parsing.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.CONNECTION_FAILED_COORDINATES,
        title: 'Failed to connect using provided details',
        message:
            'Failed to connect to data source. Unable to establish a connection to the specified service. Please check the connection details.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.CONNECTION_FAILED_CREDENTIALS,
        title: 'Failed to connect using provided credentials',
        message:
            'Failed to connect to data source. Unable to authenticate with the specified service using the provided credentials. Please check the connection credentials.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.CONNECTION_FAILED_SERVICE_UNAVAILABLE,
        title: 'Service unavailable',
        message: 'Failed to connect to the data source. The service is currently unavailable.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.CONNECTION_FAILED_SERVICE_TIMEOUT,
        title: 'Service timeout',
        message:
            'Failed to connect to the data source. A timeout was encountered when attempting to extract data from the data source.',
        rawTypes: [],
    },
    {
        type: StructuredReportItemType.CONNECTION_FAILED_UNKNOWN,
        title: 'Unknown connection error',
        message: 'Failed to connect to the data source for an unknown reason. Please check the connection details.',
        rawTypes: [],
    },
];

/**
 * Map raw type to details associated above.
 */
export const STRUCTURED_REPORT_ITEM_RAW_TYPE_TO_DETAILS = new Map();
STRUCTURED_REPORT_ITEM_DISPLAY_DETAILS.forEach((details) => {
    const rawTypes = details.rawTypes || [];
    rawTypes.forEach((rawType) => {
        STRUCTURED_REPORT_ITEM_RAW_TYPE_TO_DETAILS.set(rawType, details);
    });
});

/**
 * Map std type to details associated above.
 */
export const STRUCTURED_REPORT_ITEM_TYPE_TO_DETAILS = new Map();
STRUCTURED_REPORT_ITEM_DISPLAY_DETAILS.forEach((details) => {
    STRUCTURED_REPORT_ITEM_TYPE_TO_DETAILS.set(details.type, details);
});
