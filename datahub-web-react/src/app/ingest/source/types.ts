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
 * A type describing an individual warning / failure item in a structured report.
 *
 * TODO: Determine whether we need a message field to be reported!
 */
export interface StructuredReportLogEntry {
    level: StructuredReportItemLevel; // The "log level"
    title?: string; // The "well-supported" or standardized title
    message: string; // The message to display associated with the error.
    context: string[]; // The context of WHERE the issue was encountered, as a string.
}

/**
 * A type describing a structured ingestion report.
 */
export interface StructuredReport {
    infoCount: number;
    errorCount: number;
    warnCount: number;
    items: StructuredReportLogEntry[];
}
