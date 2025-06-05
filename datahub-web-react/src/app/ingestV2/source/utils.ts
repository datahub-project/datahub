import YAML from 'yamljs';

import { SortingState } from '@components/components/Table/types';

import EntityRegistry from '@app/entity/EntityRegistry';
import { SYSTEM_INTERNAL_SOURCE_TYPE } from '@app/ingestV2/constants';
import {
    StructuredReport,
    StructuredReportItemLevel,
    StructuredReportLogEntry,
} from '@app/ingestV2/executions/components/reporting/types';
import {
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import { capitalizeFirstLetterOnly, pluralize } from '@app/shared/textUtil';

import { ListIngestionSourcesDocument, ListIngestionSourcesQuery } from '@graphql/ingestion.generated';
import { EntityType, ExecutionRequestResult, FacetFilterInput, FacetMetadata, SortCriterion, SortOrder } from '@types';

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

export const MANUAL_INGESTION_SOURCE = 'MANUAL_INGESTION_SOURCE';
export const SCHEDULED_INGESTION_SOURCE = 'SCHEDULED_INGESTION_SOURCE';
export const CLI_INGESTION_SOURCE = 'CLI_INGESTION_SOURCE';

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

const extractStructuredReportPOJO = (result: Partial<ExecutionRequestResult>): any | null => {
    const structuredReportStr = result?.structuredReport?.serializedValue;
    if (!structuredReportStr) {
        return null;
    }
    try {
        return JSON.parse(structuredReportStr);
    } catch (e) {
        console.error(`Caught exception while parsing structured report!`, e);
        return null;
    }
};

export const getStructuredReport = (result: Partial<ExecutionRequestResult>): StructuredReport | null => {
    // 1. Extract Serialized Structured Report
    const structuredReportObject = extractStructuredReportPOJO(result);
    if (!structuredReportObject) {
        return null;
    }

    // 3. Transform into the typed model that we have.
    const structuredReport = transformToStructuredReport(structuredReportObject);

    // 4. Return JSON report
    return structuredReport;
};

/**
 * This function is used to get the total number of entities ingested from the structured report.
 *
 * @param result - The result of the execution request.
 * @returns {number | null}
 */
export const getTotalEntitiesIngested = (result: Partial<ExecutionRequestResult>) => {
    const structuredReportObject = extractStructuredReportPOJO(result);
    if (!structuredReportObject) {
        return null;
    }

    try {
        return structuredReportObject.sink.report.total_records_written;
    } catch (e) {
        console.error(`Caught exception while parsing structured report!`, e);
        return null;
    }
};

/** *
 * This function is used to get the entities ingested by type from the structured report.
 * It returns an array of objects with the entity type and the count of entities ingested.
 *
 * Example input:
 *
 * {
 *     "source": {
 *         "report": {
 *             "aspects": {
 *                 "container": {
 *                     "containerProperties": 156,
 *                     ...
 *                     "container": 117
 *                 },
 *                 "dataset": {
 *                     "datasetProperties": 1505,
 *                     ...
 *                     "operation": 1521
 *                 },
 *                 ...
 *             }
 *             ...
 *         }
 *     }
 *     ...
 * }
 *
 * Example output:
 *
 * [
 *     {
 *         "count": 156,
 *         "displayName": "container"
 *     },
 *     ...
 * ]
 *
 * @param result - The result of the execution request.
 * @returns {EntityTypeCount[] | null}
 */
export const getEntitiesIngestedByType = (result: Partial<ExecutionRequestResult>): EntityTypeCount[] | null => {
    const structuredReportObject = extractStructuredReportPOJO(result);
    if (!structuredReportObject) {
        return null;
    }

    try {
        /**
         * This is what the aspects object looks like in the structured report:
         *
         * "aspects": {
         *     "container": {
         *         "containerProperties": 156,
         *         ...
         *         "container": 117
         *     },
         *     "dataset": {
         *         "status": 1505,
         *         "schemaMetadata": 1505,
         *         "datasetProperties": 1505,
         *         "container": 1505,
         *         ...
         *         "operation": 1521
         *     },
         *     ...
         * }
         */
        const entities = structuredReportObject.source.report.aspects;
        const entitiesIngestedByType: { [key: string]: number } = {};
        Object.entries(entities).forEach(([entityName, aspects]) => {
            // Get the max count of all the sub-aspects for this entity type.
            entitiesIngestedByType[entityName] = Math.max(...(Object.values(aspects as object) as number[]));
        });

        if (Object.keys(entitiesIngestedByType).length === 0) {
            return null;
        }

        return Object.entries(entitiesIngestedByType).map(([entityName, count]) => ({
            count,
            displayName: entityName,
        }));
    } catch (e) {
        console.error(`Caught exception while parsing structured report!`, e);
        return null;
    }
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
    if (status === EXECUTION_REQUEST_STATUS_SUCCESS && (structuredReport?.warnCount || 0) > 0) {
        return EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS;
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

export type EntityTypeCount = {
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

export function getSortInput(field: string, order: SortingState): SortCriterion | undefined {
    if (order === SortingState.ORIGINAL) return undefined;

    return {
        sortOrder: order === SortingState.ASCENDING ? SortOrder.Ascending : SortOrder.Descending,
        field,
    };
}

export const getIngestionSourceSystemFilter = (hideSystemSources: boolean): FacetFilterInput => {
    return hideSystemSources
        ? { field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE], negated: true }
        : { field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE] };
};
