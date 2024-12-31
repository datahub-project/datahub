import {
    CheckCircleOutlined,
    ClockCircleOutlined,
    CloseCircleOutlined,
    LoadingOutlined,
    StopOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import YAML from 'yamljs';
import { ListIngestionSourcesDocument, ListIngestionSourcesQuery } from '../../../graphql/ingestion.generated';
import { EntityType, ExecutionRequestResult, FacetMetadata } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entity/shared/constants';
import { capitalizeFirstLetterOnly, pluralize } from '../../shared/textUtil';
import { SourceConfig } from './builder/types';
import { StructuredReport, StructuredReportLogEntry, StructuredReportItemLevel } from './types';

export const getSourceConfigs = (ingestionSources: SourceConfig[], sourceType: string) => {
    const sourceConfigs = ingestionSources.find((source) => source.name === sourceType);
    if (!sourceConfigs) {
        console.error(`Failed to find source configs with source type ${sourceType}`);
    }
    return sourceConfigs;
};

export const yamlToJson = (yaml: string): string => {
    const obj = YAML.parse(yaml);
    const jsonStr = JSON.stringify(obj);
    return jsonStr;
};

export const jsonToYaml = (json: string): string => {
    const obj = JSON.parse(json);
    const yamlStr = YAML.stringify(obj, 6);
    return yamlStr;
};

export function getPlaceholderRecipe(ingestionSources: SourceConfig[], type?: string) {
    const selectedSource = ingestionSources.find((source) => source.name === type);
    return selectedSource?.recipe || '';
}

export const RUNNING = 'RUNNING';
export const SUCCESS = 'SUCCESS';
export const SUCCEEDED_WITH_WARNINGS = 'SUCCEEDED_WITH_WARNINGS';
export const WARNING = 'WARNING';
export const FAILURE = 'FAILURE';
export const CONNECTION_FAILURE = 'CONNECTION_FAILURE';
export const CANCELLED = 'CANCELLED';
export const ABORTED = 'ABORTED';
export const UP_FOR_RETRY = 'UP_FOR_RETRY';
export const ROLLING_BACK = 'ROLLING_BACK';
export const ROLLED_BACK = 'ROLLED_BACK';
export const ROLLBACK_FAILED = 'ROLLBACK_FAILED';

export const CLI_EXECUTOR_ID = '__datahub_cli_';
export const MANUAL_INGESTION_SOURCE = 'MANUAL_INGESTION_SOURCE';
export const SCHEDULED_INGESTION_SOURCE = 'SCHEDULED_INGESTION_SOURCE';
export const CLI_INGESTION_SOURCE = 'CLI_INGESTION_SOURCE';

export const getExecutionRequestStatusIcon = (status: string) => {
    return (
        (status === RUNNING && LoadingOutlined) ||
        (status === SUCCESS && CheckCircleOutlined) ||
        (status === SUCCEEDED_WITH_WARNINGS && CheckCircleOutlined) ||
        (status === FAILURE && CloseCircleOutlined) ||
        (status === CANCELLED && StopOutlined) ||
        (status === UP_FOR_RETRY && ClockCircleOutlined) ||
        (status === ROLLED_BACK && WarningOutlined) ||
        (status === ROLLING_BACK && LoadingOutlined) ||
        (status === ROLLBACK_FAILED && CloseCircleOutlined) ||
        (status === ABORTED && CloseCircleOutlined) ||
        ClockCircleOutlined
    );
};

export const getExecutionRequestStatusDisplayText = (status: string) => {
    return (
        (status === RUNNING && 'Running') ||
        (status === SUCCESS && 'Succeeded') ||
        (status === SUCCEEDED_WITH_WARNINGS && 'Succeeded With Warnings') ||
        (status === FAILURE && 'Failed') ||
        (status === CANCELLED && 'Cancelled') ||
        (status === UP_FOR_RETRY && 'Up for Retry') ||
        (status === ROLLED_BACK && 'Rolled Back') ||
        (status === ROLLING_BACK && 'Rolling Back') ||
        (status === ROLLBACK_FAILED && 'Rollback Failed') ||
        (status === ABORTED && 'Aborted') ||
        status
    );
};

export const getExecutionRequestSummaryText = (status: string) => {
    switch (status) {
        case RUNNING:
            return 'Ingestion is running...';
        case SUCCESS:
            return 'Ingestion completed with no errors or warnings.';
        case SUCCEEDED_WITH_WARNINGS:
            return 'Ingestion completed with some warnings.';
        case FAILURE:
            return 'Ingestion failed to complete, or completed with errors.';
        case CANCELLED:
            return 'Ingestion was cancelled.';
        case ROLLED_BACK:
            return 'Ingestion was rolled back.';
        case ROLLING_BACK:
            return 'Ingestion is in the process of rolling back.';
        case ROLLBACK_FAILED:
            return 'Ingestion rollback failed.';
        case ABORTED:
            return 'Ingestion job got aborted due to worker restart.';
        default:
            return 'Ingestion status not recognized.';
    }
};

export const getExecutionRequestStatusDisplayColor = (status: string) => {
    return (
        (status === RUNNING && REDESIGN_COLORS.BLUE) ||
        (status === SUCCESS && 'green') ||
        (status === SUCCEEDED_WITH_WARNINGS && '#b88806') ||
        (status === FAILURE && 'red') ||
        (status === UP_FOR_RETRY && 'orange') ||
        (status === CANCELLED && ANTD_GRAY[9]) ||
        (status === ROLLED_BACK && 'orange') ||
        (status === ROLLING_BACK && 'orange') ||
        (status === ROLLBACK_FAILED && 'red') ||
        (status === ABORTED && 'red') ||
        ANTD_GRAY[7]
    );
};

export const validateURL = (fieldName: string) => {
    return {
        validator(_, value) {
            const URLPattern = new RegExp(
                /^(?:http(s)?:\/\/)?[\w.-]+(?:\.[a-zA-Z0-9.-]{2,})+[\w\-._~:/?#[\]@!$&'()*+,;=.]+$/,
            );
            const isURLValid = URLPattern.test(value);
            if (!value || isURLValid) {
                return Promise.resolve();
            }
            return Promise.reject(new Error(`A valid ${fieldName} is required.`));
        },
    };
};

const createStructuredReport = (items: StructuredReportLogEntry[]): StructuredReport => {
    const errorCount = items.filter((item) => item.level === StructuredReportItemLevel.ERROR).length;
    const warnCount = items.filter((item) => item.level === StructuredReportItemLevel.WARN).length;
    const infoCount = items.filter((item) => item.level === StructuredReportItemLevel.INFO).length;
    return {
        errorCount,
        warnCount,
        infoCount,
        items,
    };
};

const transformToStructuredReport = (structuredReportObj: any): StructuredReport | null => {
    if (!structuredReportObj) {
        return null;
    }

    /* Legacy helper function to map backend failure or warning ingestion objects into StructuredReportLogEntry[] */
    const mapItemObject = (
        items: { [key: string]: string[] },
        level: StructuredReportItemLevel,
    ): StructuredReportLogEntry[] => {
        return Object.entries(items).map(([rawMessage, context]) => ({
            level,
            title: 'An unexpected issue occurred',
            message: rawMessage,
            context,
        }));
    };

    /* V2 helper function to map backend failure or warning lists into StructuredReportLogEntry[] */
    const mapItemArray = (items, level: StructuredReportItemLevel): StructuredReportLogEntry[] => {
        return items
            .map((item) => {
                if (typeof item === 'string') {
                    // Handle "sampled from" case..
                    return null;
                }

                return {
                    level,
                    title: item.title || 'An unexpected issue occurred',
                    message: item.message,
                    context: item.context,
                };
            })
            .filter((item) => item != null);
    };

    try {
        const sourceReport = structuredReportObj.source?.report;

        if (!sourceReport) {
            return null;
        }

        // Else fallback to using the legacy fields
        const failures = Array.isArray(sourceReport.failures)
            ? /* Use V2 failureList if present */
              mapItemArray(sourceReport.failures || [], StructuredReportItemLevel.ERROR)
            : /* Else use the legacy object type */
              mapItemObject(sourceReport.failures || {}, StructuredReportItemLevel.ERROR);

        const warnings = Array.isArray(sourceReport.warnings)
            ? /* Use V2 warning if present */
              mapItemArray(sourceReport.warnings || [], StructuredReportItemLevel.WARN)
            : /* Else use the legacy object type */
              mapItemObject(sourceReport.warnings || {}, StructuredReportItemLevel.WARN);

        const infos = Array.isArray(sourceReport.infos)
            ? /* Use V2 infos if present */
              mapItemArray(sourceReport.infos || [], StructuredReportItemLevel.INFO)
            : /* Else use the legacy object type */
              mapItemObject(sourceReport.infos || {}, StructuredReportItemLevel.INFO);

        return createStructuredReport([...failures, ...warnings, ...infos]);
    } catch (e) {
        console.warn('Failed to extract structured report from ingestion report!', e);
        return null;
    }
};

export const getStructuredReport = (result: Partial<ExecutionRequestResult>): StructuredReport | null => {
    // 1. Extract Serialized Structured Report
    const structuredReportStr = result?.structuredReport?.serializedValue;

    if (!structuredReportStr) {
        return null;
    }

    // 2. Convert into JSON
    const structuredReportObject = JSON.parse(structuredReportStr);

    // 3. Transform into the typed model that we have.
    const structuredReport = transformToStructuredReport(structuredReportObject);

    // 4. Return JSON report
    return structuredReport;
};

export const getIngestionSourceStatus = (result?: Partial<ExecutionRequestResult> | null) => {
    if (!result) {
        return undefined;
    }

    const { status } = result;
    const structuredReport = getStructuredReport(result);

    /**
     * Simply map SUCCESS in the presence of warnings to SUCCEEDED_WITH_WARNINGS
     *
     * This is somewhat of a hack - ideally the ingestion source should report this status back to us.
     */
    if (status === SUCCESS && (structuredReport?.warnCount || 0) > 0) {
        return SUCCEEDED_WITH_WARNINGS;
    }
    // Else return the raw status.
    return status;
};

const ENTITIES_WITH_SUBTYPES = new Set([
    EntityType.Dataset.toLowerCase(),
    EntityType.Container.toLowerCase(),
    EntityType.Notebook.toLowerCase(),
    EntityType.Dashboard.toLowerCase(),
]);

type EntityTypeCount = {
    count: number;
    displayName: string;
};

/**
 * Extract entity type counts to display in the ingestion summary.
 *
 * @param entityTypeFacets the filter facets for entity type.
 * @param subTypeFacets the filter facets for sub types.
 */
export const extractEntityTypeCountsFromFacets = (
    entityRegistry: EntityRegistry,
    entityTypeFacets: FacetMetadata,
    subTypeFacets?: FacetMetadata | null,
): EntityTypeCount[] => {
    const finalCounts: EntityTypeCount[] = [];

    if (subTypeFacets) {
        subTypeFacets.aggregations
            .filter((agg) => agg.count > 0)
            .forEach((agg) =>
                finalCounts.push({
                    count: agg.count,
                    displayName: pluralize(agg.count, capitalizeFirstLetterOnly(agg.value) || ''),
                }),
            );
        entityTypeFacets.aggregations
            .filter((agg) => agg.count > 0)
            .filter((agg) => !ENTITIES_WITH_SUBTYPES.has(agg.value.toLowerCase()))
            .forEach((agg) =>
                finalCounts.push({
                    count: agg.count,
                    displayName: entityRegistry.getCollectionName(agg.value as EntityType),
                }),
            );
    } else {
        // Only use Entity Types- no subtypes.
        entityTypeFacets.aggregations
            .filter((agg) => agg.count > 0)
            .forEach((agg) =>
                finalCounts.push({
                    count: agg.count,
                    displayName: entityRegistry.getCollectionName(agg.value as EntityType),
                }),
            );
    }

    return finalCounts;
};

/**
 * Add an entry to the ListIngestionSources cache.
 */
export const addToListIngestionSourcesCache = (client, newSource, pageSize, query) => {
    // Read the data from our cache for this query.
    const currData: ListIngestionSourcesQuery | null = client.readQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query,
            },
        },
    });

    // Add our new source into the existing list.
    const newSources = [newSource, ...(currData?.listIngestionSources?.ingestionSources || [])];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query,
            },
        },
        data: {
            listIngestionSources: {
                start: 0,
                count: (currData?.listIngestionSources?.count || 0) + 1,
                total: (currData?.listIngestionSources?.total || 0) + 1,
                ingestionSources: newSources,
            },
        },
    });
};

/**
 * Remove an entry from the ListIngestionSources cache.
 */
export const removeFromListIngestionSourcesCache = (client, urn, page, pageSize, query) => {
    // Read the data from our cache for this query.
    const currData: ListIngestionSourcesQuery | null = client.readQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
            },
        },
    });

    // Remove the source from the existing sources set.
    const newSources = [
        ...(currData?.listIngestionSources?.ingestionSources || []).filter((source) => source.urn !== urn),
    ];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListIngestionSourcesDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
            },
        },
        data: {
            listIngestionSources: {
                start: currData?.listIngestionSources?.start || 0,
                count: (currData?.listIngestionSources?.count || 1) - 1,
                total: (currData?.listIngestionSources?.total || 1) - 1,
                ingestionSources: newSources,
            },
        },
    });
};
