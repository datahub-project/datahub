import dayjs from 'dayjs';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import localizedFormat from 'dayjs/plugin/localizedFormat';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';
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
    EXECUTION_REQUEST_STATUS_LOADING,
    EXECUTION_REQUEST_STATUS_PENDING,
    EXECUTION_REQUEST_STATUS_SUCCEEDED_WITH_WARNINGS,
    EXECUTION_REQUEST_STATUS_SUCCESS,
} from '@app/ingestV2/executions/constants';
import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState, SourceConfig } from '@app/ingestV2/source/builder/types';
import { capitalizeFirstLetterOnly, pluralize } from '@app/shared/textUtil';

import {
    Entity,
    EntityType,
    ExecutionRequestResult,
    FacetFilterInput,
    FacetMetadata,
    IngestionSource,
    OwnershipTypeEntity,
    SortCriterion,
    SortOrder,
    StringMapEntryInput,
} from '@types';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);
dayjs.extend(localizedFormat);

export const CUSTOM_SOURCE_NAME = 'custom';
export const CUSTOM_SOURCE_DISPLAY_NAME = 'Custom';

export const getSourceConfigs = (ingestionSources: SourceConfig[], sourceType: string) => {
    const sourceConfigs = ingestionSources.find((source) => source.name === sourceType);
    if (!sourceConfigs) {
        return ingestionSources.find((source) => source.name === CUSTOM_SOURCE_NAME);
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

    /* Extract items from a report (source or sink) */
    const extractItemsFromReport = (report: any): StructuredReportLogEntry[] => {
        if (!report) {
            return [];
        }

        const failures = Array.isArray(report.failures)
            ? mapItemArray(report.failures || [], StructuredReportItemLevel.ERROR)
            : mapItemObject(report.failures || {}, StructuredReportItemLevel.ERROR);

        const warnings = Array.isArray(report.warnings)
            ? mapItemArray(report.warnings || [], StructuredReportItemLevel.WARN)
            : mapItemObject(report.warnings || {}, StructuredReportItemLevel.WARN);

        const infos = Array.isArray(report.infos)
            ? mapItemArray(report.infos || [], StructuredReportItemLevel.INFO)
            : mapItemObject(report.infos || {}, StructuredReportItemLevel.INFO);

        return [...failures, ...warnings, ...infos];
    };

    try {
        const sourceReport = structuredReportObj.source?.report;
        const sinkReport = structuredReportObj.sink?.report;

        if (!sourceReport && !sinkReport) {
            return null;
        }

        const sourceItems = extractItemsFromReport(sourceReport);
        const sinkItems = extractItemsFromReport(sinkReport);

        return createStructuredReport([...sourceItems, ...sinkItems]);
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

export const getAspectsBySubtypes = (structuredReportObject: any, entityRegistry: EntityRegistry) => {
    const searchEntityTypesInCamelCase = new Set(entityRegistry.getSearchEntityTypesAsCamelCase());

    const aspectsBySubtypes = structuredReportObject?.source?.report?.aspects_by_subtypes;
    if (!aspectsBySubtypes) {
        return null;
    }
    Object.keys(aspectsBySubtypes).forEach((entityName) => {
        if (!searchEntityTypesInCamelCase.has(entityName)) {
            // We are doing this otherwise in the UI we will show a total number
            // On clicking view all the number will not match
            delete aspectsBySubtypes[entityName];
        }
    });
    return aspectsBySubtypes;
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
export const getEntitiesIngestedByTypeOrSubtype = (
    result: Partial<ExecutionRequestResult>,
    entityRegistry: EntityRegistry,
): EntityTypeCount[] | null => {
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
        const entities = getAspectsBySubtypes(structuredReportObject, entityRegistry);
        const entitiesIngestedByType: { [key: string]: number } = {};
        Object.entries(entities).forEach(([entityName, aspectsBySubtypes]) => {
            // Use the status aspect count instead of max count
            Object.entries(aspectsBySubtypes as any)?.forEach(([subtype, aspects]) => {
                const statusCount = (aspects as any)?.status;
                if (statusCount !== undefined) {
                    entitiesIngestedByType[subtype !== 'unknown' ? subtype : entityName] = statusCount;
                } else {
                    // Get the max count of all the sub-aspects for this entity type if status is not present.
                    entitiesIngestedByType[subtype !== 'unknown' ? subtype : entityName] = Math.max(
                        ...(Object.values(aspects as object) as number[]),
                    );
                }
            });
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

/**
 * This function is used to get the total number of entities ingested from the structured report.
 *
 * @param result - The result of the execution request.
 * @returns {number | null}
 */
export const getTotalEntitiesIngested = (result: Partial<ExecutionRequestResult>, entityRegistry: EntityRegistry) => {
    const entityTypeCounts = getEntitiesIngestedByTypeOrSubtype(result, entityRegistry);
    if (!entityTypeCounts) {
        return null;
    }

    return entityTypeCounts.reduce((total, entityType) => total + entityType.count, 0);
};

export const getOtherIngestionContents = (
    executionResult: Partial<ExecutionRequestResult>,
    entityRegistry: EntityRegistry,
) => {
    const structuredReportObject = extractStructuredReportPOJO(executionResult);
    if (!structuredReportObject) {
        return null;
    }
    const aspectsBySubtypes = getAspectsBySubtypes(structuredReportObject, entityRegistry);

    if (!aspectsBySubtypes || Object.keys(aspectsBySubtypes).length === 0) {
        return null;
    }

    let totalStatusCount = 0;
    let totalDatasetProfileCount = 0;
    let totalDatasetUsageStatisticsCount = 0;

    Object.entries(aspectsBySubtypes).forEach(([entityType, subtypes]) => {
        if (entityType !== 'dataset') {
            // temporary for now - we have not decided on the design for non dataset entity types
            return;
        }
        Object.entries(subtypes as Record<string, any>).forEach(([_, aspects]) => {
            const statusCount = (aspects as any)?.status || 0;
            if (statusCount === 0) {
                return;
            }
            const dataSetProfileCount = (aspects as any)?.datasetProfile || 0;
            const dataSetUsageStatisticsCount = (aspects as any)?.datasetUsageStatistics || 0;

            totalStatusCount += statusCount;
            totalDatasetProfileCount += dataSetProfileCount;
            totalDatasetUsageStatisticsCount += dataSetUsageStatisticsCount;
        });
    });

    if (totalStatusCount === 0) {
        return null;
    }

    const result: Array<{ type: string; count: number; percent: string }> = [];

    if (totalDatasetProfileCount > 0) {
        const datasetProfilePercent = `${((totalDatasetProfileCount / totalStatusCount) * 100).toFixed(0)}%`;
        result.push({
            type: 'Profiling',
            count: totalDatasetProfileCount,
            percent: datasetProfilePercent,
        });
    }

    if (totalDatasetUsageStatisticsCount > 0) {
        const datasetUsageStatisticsPercent = `${((totalDatasetUsageStatisticsCount / totalStatusCount) * 100).toFixed(0)}%`;
        result.push({
            type: 'Usage',
            count: totalDatasetUsageStatisticsCount,
            percent: datasetUsageStatisticsPercent,
        });
    } else {
        result.push({
            type: 'Usage',
            count: 0,
            percent: '0%',
        });
    }

    if (result.length === 0) {
        return null;
    }

    return result;
};

export const getIngestionContents = (
    executionResult: Partial<ExecutionRequestResult>,
    entityRegistry: EntityRegistry,
) => {
    const structuredReportObject = extractStructuredReportPOJO(executionResult);
    if (!structuredReportObject) {
        return null;
    }
    const aspectsBySubtypes = getAspectsBySubtypes(structuredReportObject, entityRegistry);

    if (!aspectsBySubtypes || Object.keys(aspectsBySubtypes).length === 0) {
        return null;
    }

    const result: Array<{ title: string; count: number; percent: string }> = [];
    Object.entries(aspectsBySubtypes).forEach(([entityType, subtypes]) => {
        if (entityType !== 'dataset') {
            // temporary for now - we have not decided on the design for non dataset entity types
            return;
        }
        Object.entries(subtypes as Record<string, any>).forEach(([subtype, aspects]) => {
            const statusCount = (aspects as any)?.status || 0;
            const upstreamLineage = (aspects as any)?.upstreamLineage || 0;
            if (statusCount === 0) {
                return;
            }
            const percent = `${((upstreamLineage / statusCount) * 100).toFixed(0)}%`;
            if (percent === '0%') {
                return;
            }
            result.push({
                title: subtype,
                count: upstreamLineage,
                percent,
            });
        });
    });
    if (result.length === 0) {
        return null;
    }

    return result;
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

export function getSortInput(field: string, order: SortingState): SortCriterion | undefined {
    if (order === SortingState.ORIGINAL) return undefined;

    return {
        sortOrder: order === SortingState.ASCENDING ? SortOrder.Ascending : SortOrder.Descending,
        field,
    };
}

export const DEFAULT_SOURCE_SORT_CRITERION: SortCriterion = {
    sortOrder: SortOrder.Ascending,
    field: 'type',
};

export const getIngestionSourceSystemFilter = (hideSystemSources: boolean): FacetFilterInput => {
    return hideSystemSources
        ? { field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE], negated: true }
        : { field: 'sourceType', values: [SYSTEM_INTERNAL_SOURCE_TYPE] };
};

export function formatTimezone(timezoneVal: string | null | undefined): string | undefined {
    return timezoneVal ? dayjs().tz(timezoneVal).format('z') : undefined;
}

export function capitalizeMonthsAndDays(scheduleText: string): string {
    const dayNames = Array.from({ length: 7 }, (_, i) => dayjs().day(i).format('dddd').toLowerCase());
    const monthNames = Array.from({ length: 12 }, (_, i) => dayjs().month(i).format('MMMM').toLowerCase());

    const capitalizableWords = new Set([...dayNames, ...monthNames]);

    return scheduleText.replace(/\b[a-z]+\b/g, (word) =>
        capitalizableWords.has(word) ? word.charAt(0).toUpperCase() + word.slice(1) : word,
    );
}

export const getSourceStatus = (
    source: IngestionSource,
    sourcesToRefetch: Set<string>,
    executedUrns: Set<string>,
): string => {
    const isPolling = sourcesToRefetch.has(source.urn);
    const hasRequests = !!source.executions?.executionRequests?.length;
    const hasActiveRequest = source.executions?.executionRequests?.some(isExecutionRequestActive);
    const executedNow = executedUrns.has(source.urn);

    if (executedNow && !hasActiveRequest) return EXECUTION_REQUEST_STATUS_LOADING;
    if (!isPolling && !hasRequests) return EXECUTION_REQUEST_STATUS_PENDING;

    return (
        getIngestionSourceStatus(source.executions?.executionRequests?.[0]?.result) ?? EXECUTION_REQUEST_STATUS_PENDING
    );
};

export const buildOwnerEntities = (urn: string, owners?: Entity[], defaultOwnerType?: OwnershipTypeEntity) => {
    return (
        owners?.map((owner: any) => ({
            owner: {
                ...owner,
                editableProperties: {
                    email: '',
                    displayName: '',
                    title: '',
                    pictureLink: '',
                    ...owner.editableProperties,
                },
                properties: {
                    displayName: '',
                    email: '',
                    active: true,
                    firstName: '',
                    lastName: '',
                    fullName: '',
                    title: '',
                    ...owner.properties,
                },
                info: {
                    email: '',
                    admins: [],
                    members: [],
                    groups: [],
                    active: true,
                    displayName: '',
                    firstName: '',
                    lastName: '',
                    fullName: '',
                    title: '',
                    ...owner.info,
                },
            },
            attribution: owner.attribution ?? null,
            associatedUrn: urn,
            type: owner.type,
            ownershipType: defaultOwnerType ?? null,
            __typename: 'Owner' as const,
        })) || []
    );
};

export const mapSourceTypeAliases = <T extends { type: string }>(source?: T): T | undefined => {
    if (source) {
        let { type } = source;
        if (type === 'unity-catalog') {
            type = 'databricks';
        }
        return { ...source, type };
    }
    return undefined;
};

export const removeExecutionsFromIngestionSource = (source) => {
    if (source) {
        return {
            name: source.name,
            type: source.type,
            schedule: source.schedule,
            config: source.config,
            source: source.source,
        };
    }
    return undefined;
};

export const formatExtraArgs = (extraArgs: StringMapEntryInput[] | null | undefined): StringMapEntryInput[] => {
    if (extraArgs === null || extraArgs === undefined) return [];
    return extraArgs
        .filter((entry) => entry.value !== null && entry.value !== undefined && entry.value !== '')
        .map((entry) => ({ key: entry.key, value: entry.value }));
};

export const getNewIngestionSourcePlaceholder = (
    urn: string,
    data: SourceBuilderState,
    defaultOwnershipType: OwnershipTypeEntity | undefined,
) => {
    const newSource = {
        urn,
        name: data.name as string,
        type: data.type as string,
        config: { executorId: '', recipe: '', version: null, debugMode: null, extraArgs: null },
        schedule: {
            interval: data.schedule?.interval || '',
            timezone: data.schedule?.timezone || null,
        },
        platform: null,
        executions: null,
        source: null,
        ownership: {
            owners: buildOwnerEntities(urn, data.owners, defaultOwnershipType),
            lastModified: {
                time: 0,
            },
            __typename: 'Ownership' as const,
        },
        __typename: 'IngestionSource' as const,
    };

    return newSource;
};

export const getIngestionSourceMutationInput = (data: SourceBuilderState, source?: IngestionSource) => {
    return {
        type: data.type as string,
        name: data.name as string,
        config: {
            recipe: data.config?.recipe as string,
            version: (data.config?.version?.length && (data.config?.version as string)) || undefined,
            executorId: (data.config?.executorId?.length && (data.config?.executorId as string)) || DEFAULT_EXECUTOR_ID,
            debugMode: data.config?.debugMode || false,
            extraArgs: formatExtraArgs(data.config?.extraArgs || []),
        },
        schedule: data.schedule && {
            interval: data.schedule?.interval as string,
            timezone: data.schedule?.timezone as string,
        },
        // Preserve source field when editing existing sources (especially system sources)
        source: source?.source
            ? {
                  type: source.source.type,
              }
            : undefined,
    };
};

export const getSourceDisplayName = (sourceType: string, ingestionSources: SourceConfig[]) => {
    const sourceConfigs = getSourceConfigs(ingestionSources, sourceType as string);
    return sourceConfigs?.displayName;
};
