import {
    CheckCircleOutlined,
    ClockCircleOutlined,
    CloseCircleOutlined,
    ExclamationCircleOutlined,
    LoadingOutlined,
    StopOutlined,
    WarningOutlined,
} from '@ant-design/icons';
import YAML from 'yamljs';
import { ListIngestionSourcesDocument, ListIngestionSourcesQuery } from '../../../graphql/ingestion.generated';
import { EntityType, FacetMetadata } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../entity/shared/constants';
import { capitalizeFirstLetterOnly, pluralize } from '../../shared/textUtil';
import { SourceConfig } from './builder/types';

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
export const WARNING = 'WARNING';
export const FAILURE = 'FAILURE';
export const CONNECTION_FAILURE = 'CONNECTION_FAILURE';
export const CANCELLED = 'CANCELLED';
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
        (status === WARNING && ExclamationCircleOutlined) ||
        (status === FAILURE && CloseCircleOutlined) ||
        (status === CONNECTION_FAILURE && CloseCircleOutlined) ||
        (status === CANCELLED && StopOutlined) ||
        (status === UP_FOR_RETRY && ClockCircleOutlined) ||
        (status === ROLLED_BACK && WarningOutlined) ||
        (status === ROLLING_BACK && LoadingOutlined) ||
        (status === ROLLBACK_FAILED && CloseCircleOutlined) ||
        ClockCircleOutlined
    );
};

export const getExecutionRequestStatusDisplayText = (status: string) => {
    return (
        (status === RUNNING && 'Running') ||
        (status === SUCCESS && 'Succeeded') ||
        (status === WARNING && 'Completed') ||
        (status === FAILURE && 'Failed') ||
        (status === CONNECTION_FAILURE && 'Connection Failed') ||
        (status === CANCELLED && 'Cancelled') ||
        (status === UP_FOR_RETRY && 'Up for Retry') ||
        (status === ROLLED_BACK && 'Rolled Back') ||
        (status === ROLLING_BACK && 'Rolling Back') ||
        (status === ROLLBACK_FAILED && 'Rollback Failed') ||
        status
    );
};

export const getExecutionRequestSummaryText = (status: string) => {
    switch (status) {
        case RUNNING:
            return 'Ingestion is running...';
        case SUCCESS:
            return 'Ingestion succeeded with no errors or suspected missing data.';
        case WARNING:
            return 'Ingestion completed with minor or intermittent errors.';
        case FAILURE:
            return 'Ingestion failed to complete, or completed with serious errors.';
        case CONNECTION_FAILURE:
            return 'Ingestion failed due to network, authentication, or permission issues.';
        case CANCELLED:
            return 'Ingestion was cancelled.';
        case ROLLED_BACK:
            return 'Ingestion was rolled back.';
        case ROLLING_BACK:
            return 'Ingestion is in the process of rolling back.';
        case ROLLBACK_FAILED:
            return 'Ingestion rollback failed.';
        default:
            return 'Ingestion status not recognized.';
    }
};

export const getExecutionRequestStatusDisplayColor = (status: string) => {
    return (
        (status === RUNNING && REDESIGN_COLORS.BLUE) ||
        (status === SUCCESS && 'green') ||
        (status === WARNING && 'orangered') ||
        (status === FAILURE && 'red') ||
        (status === CONNECTION_FAILURE && 'crimson') ||
        (status === UP_FOR_RETRY && 'orange') ||
        (status === CANCELLED && ANTD_GRAY[9]) ||
        (status === ROLLED_BACK && 'orange') ||
        (status === ROLLING_BACK && 'orange') ||
        (status === ROLLBACK_FAILED && 'red') ||
        ANTD_GRAY[7]
    );
};

export const validateURL = (fieldName: string) => {
    return {
        validator(_, value) {
            const URLPattern = new RegExp(/^(?:http(s)?:\/\/)?[\w.-]+(?:\.[\w.-]+)+[\w\-._~:/?#[\]@!$&'()*+,;=.]+$/);
            const isURLValid = URLPattern.test(value);
            if (!value || isURLValid) {
                return Promise.resolve();
            }
            return Promise.reject(new Error(`A valid ${fieldName} is required.`));
        },
    };
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
