import { Maybe } from 'graphql/jsutils/Maybe';
import {
    ChartStatsSummary,
    DashboardStatsSummary,
    DataProduct,
    DatasetProfile,
    DatasetStatsSummary,
    DateInterval,
    Entity,
    EntityRelationshipsResult,
    EntityType,
    SearchResult,
    DatasetProperties,
    ChartProperties,
    Operation,
    Dataset,
} from '../../../types.generated';

import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import { GenericEntityProperties } from '../../entity/shared/types';
import { OUTPUT_PORTS_FIELD } from '../../search/utils/constants';
import { TimeWindowSize } from '../../shared/time/timeUtils';
import { TITLE_CASE_EXCEPTION_WORDS } from './constants';

export function dictToQueryStringParams(params: Record<string, string | boolean>) {
    return Object.keys(params)
        .map((key) => `${key}=${params[key]}`)
        .join('&');
}

export function urlEncodeUrn(urn: string) {
    return (
        urn &&
        urn
            // Hack - React Router v5 does not like pre-url-encoded paths. Since URNs can contain free form IDs, there's nothing preventing them from having percentages.
            // If we use double encoded paths, React ends up decoding them fully, which breaks our ability to read urns properly.
            .replace(/%/g, '{{encoded_percent}}')
            .replace(/\//g, '%2F')
            .replace(/\?/g, '%3F')
            .replace(/#/g, '%23')
            .replace(/\[/g, '%5B')
            .replace(/\]/g, '%5D')
    );
}

export function decodeUrn(encodedUrn: string) {
    // Hack-This is not ideal because it means that if you had the percent
    // sequence in your urn things may not work as expected.
    return decodeURIComponent(encodedUrn).replace(/{{encoded_percent}}/g, '%');
}

export function getNumberWithOrdinal(n) {
    const suffixes = ['th', 'st', 'nd', 'rd'];
    const v = n % 100;
    return n + (suffixes[(v - 20) % 10] || suffixes[v] || suffixes[0]);
}

export const encodeComma = (str: string) => {
    return str.replace(/,/g, '%2C');
};

export const decodeComma = (str: string) => {
    return str.replace(/%2C/g, ',');
};

export function notEmpty<TValue>(value: TValue | null | undefined): value is TValue {
    return value !== null && value !== undefined;
}

export const truncate = (length: number, input?: string | null) => {
    if (!input) return '';
    if (input.length > length) {
        return `${input.substring(0, length)}...`;
    }
    return input;
};

export const singularizeCollectionName = (collectionName: string): string => {
    if (!collectionName) {
        return collectionName;
    }

    const lastChar = collectionName[collectionName.length - 1];
    if (lastChar === 's') {
        return collectionName.slice(0, -1);
    }

    return collectionName;
};

export function getPlatformName(entityData: GenericEntityProperties | null) {
    const platformNames = entityData?.siblingPlatforms?.map(
        (platform) => platform.properties?.displayName || capitalizeFirstLetterOnly(platform.name),
    );
    return (
        platformNames?.[0] ||
        entityData?.platform?.properties?.displayName ||
        capitalizeFirstLetterOnly(entityData?.platform?.name)
    );
}

export function getExternalUrlDisplayName(entity: GenericEntityProperties | null) {
    // Scoping these constants
    const GITHUB_LINK = 'github.com';
    const GITHUB_NAME = 'GitHub';
    const GITLAB_LINK = 'gitlab.com';
    const GITLAB_NAME = 'GitLab';

    const externalUrl = entity?.properties?.externalUrl;
    if (externalUrl) {
        if (externalUrl.toLocaleLowerCase().includes(GITHUB_LINK)) {
            return GITHUB_NAME;
        }
        if (externalUrl.toLocaleLowerCase().includes(GITLAB_LINK)) {
            return GITLAB_NAME;
        }
    }

    return entity?.platform?.properties?.displayName || capitalizeFirstLetterOnly(entity?.platform?.name);
}

export const EDITED_DESCRIPTIONS_CACHE_NAME = 'editedDescriptions';

export const FORBIDDEN_URN_CHARS_REGEX = /.*[(),\\].*/;

export enum SidebarTitleActionType {
    LineageExplore = 'Lineage Explore',
}

/**
 * Utility function for checking whether a list is a subset of another.
 */
export const isListSubset = (l1, l2): boolean => {
    return l1.every((result) => l2.indexOf(result) >= 0);
};

function getGraphqlErrorCode(e) {
    if (e.graphQLErrors && e.graphQLErrors.length) {
        const firstError = e.graphQLErrors[0];
        const { extensions } = firstError;
        const errorCode = extensions && (extensions.code as number);
        return errorCode;
    }
    return undefined;
}

export const handleBatchError = (urns, e, defaultMessage) => {
    if (urns.length > 1 && getGraphqlErrorCode(e) === 403) {
        return {
            content:
                'Your bulk edit selection included entities that you are unauthorized to update. The bulk edit being performed will not be saved.',
            duration: 3,
        };
    }
    return defaultMessage;
};

// put all of the fineGrainedLineages for a given entity and its siblings into one array so we have all of it in one place
export function getFineGrainedLineageWithSiblings(
    entityData: GenericEntityProperties | null,
    getGenericEntityProperties: (type: EntityType, data: Entity) => GenericEntityProperties | null,
) {
    const fineGrainedLineages = [
        ...(entityData?.fineGrainedLineages || entityData?.inputOutput?.fineGrainedLineages || []),
    ];
    entityData?.siblingsSearch?.searchResults?.forEach((sibling) => {
        if (sibling.entity) {
            const genericSiblingProps = getGenericEntityProperties(sibling.entity.type, sibling.entity);
            if (genericSiblingProps && genericSiblingProps.fineGrainedLineages) {
                fineGrainedLineages.push(...genericSiblingProps.fineGrainedLineages);
            }
        }
    });
    return fineGrainedLineages;
}
export function getDataProduct(dataProductResult: Maybe<EntityRelationshipsResult> | undefined) {
    if (dataProductResult?.relationships && dataProductResult.relationships.length > 0) {
        return dataProductResult.relationships[0].entity as DataProduct;
    }
    return null;
}

export function summaryHasStats(
    statsSummary: DatasetStatsSummary | ChartStatsSummary | DashboardStatsSummary | undefined | null,
): boolean {
    return !!Object.entries(statsSummary || {}).find(
        ([key, value]) => !key.startsWith('__') && (Array.isArray(value) ? value.length : value),
    );
}

export function isOutputPort(result: SearchResult) {
    return result.extraProperties?.find((prop) => prop.name === OUTPUT_PORTS_FIELD)?.value === 'true';
}

export const computeChartTickInterval = (windowSize: TimeWindowSize): DateInterval => {
    switch (windowSize.interval) {
        case DateInterval.Day:
            return DateInterval.Hour;
        case DateInterval.Week:
            return DateInterval.Day;
        case DateInterval.Month:
            return DateInterval.Week;
        case DateInterval.Year:
            return DateInterval.Month;
        default:
            throw new Error(`Unrecognized DateInterval provided ${windowSize.interval}`);
    }
};

export const computeAllFieldPaths = (profiles: Array<DatasetProfile>): Set<string> => {
    const uniqueFieldPaths = new Set<string>();
    profiles.forEach((profile) => {
        const fieldProfiles = profile.fieldProfiles || [];
        fieldProfiles.forEach((fieldProfile) => {
            uniqueFieldPaths.add(fieldProfile.fieldPath);
        });
    });
    return uniqueFieldPaths;
};

const isPresent = (val: any) => {
    return val !== undefined && val !== null;
};

/**
 * Extracts a set of points used to render charts from a list of Dataset Profiles +
 * a particular numeric statistic name to extract. Note that the stat *must* be numeric for this utility to work.
 */
export const extractChartValuesFromTableProfiles = (profiles: Array<any>, statName: string) => {
    return profiles
        .filter((profile) => isPresent(profile[statName]))
        .map((profile) => ({
            timeMs: profile.timestampMillis,
            value: profile[statName] as number,
        }));
};

/**
 * Extracts a set of field-specific points used to render charts from a list of Dataset Profiles +
 * a particular numeric statistic name to extract. Note that the stat *must* be numeric for this utility to work.
 */
export const extractChartValuesFromFieldProfiles = (profiles: Array<any>, fieldPath: string, statName: string) => {
    return profiles
        .filter((profile) => profile.fieldProfiles)
        .map((profile) => {
            const fieldProfiles = profile.fieldProfiles
                ?.filter((field) => field.fieldPath === fieldPath)
                .filter((field) => field[statName] !== null && field[statName] !== undefined);

            if (fieldProfiles?.length === 1) {
                const fieldProfile = fieldProfiles[0];
                return {
                    timeMs: profile.timestampMillis,
                    value: fieldProfile[statName],
                };
            }
            return null;
        })
        .filter((value) => value !== null);
};

// Dataset
export type DatasetLastUpdatedMs = {
    property: 'lastModified' | 'lastUpdated' | undefined;
    lastUpdatedMs: number | undefined;
};
export function getDatasetLastUpdatedMs(
    properties: Pick<DatasetProperties, 'lastModified'> | null | undefined,
    operations: Pick<Operation, 'lastUpdatedTimestamp'>[] | null | undefined,
): DatasetLastUpdatedMs {
    const lastModified = properties?.lastModified?.time || 0;
    const lastUpdated = (operations?.length && operations[0].lastUpdatedTimestamp) || 0;

    const max = Math.max(lastModified, lastUpdated);

    if (max === 0) return { property: undefined, lastUpdatedMs: undefined };
    if (max === lastModified) return { property: 'lastModified', lastUpdatedMs: lastModified };
    return { property: 'lastUpdated', lastUpdatedMs: lastUpdated };
}

// Chart & Dashboard
export type DashboardLastUpdatedMs = {
    property: 'lastModified' | 'lastRefreshed' | undefined;
    lastUpdatedMs: number | undefined;
};

export function getDashboardLastUpdatedMs(
    properties: Pick<ChartProperties, 'lastModified' | 'lastRefreshed'> | null | undefined,
): DashboardLastUpdatedMs {
    const lastModified = properties?.lastModified?.time || 0;
    const lastRefreshed = properties?.lastRefreshed || 0;

    const max = Math.max(lastModified, lastRefreshed);

    if (max === 0) return { property: undefined, lastUpdatedMs: undefined };
    if (max === lastModified) return { property: 'lastModified', lastUpdatedMs: lastModified };
    return { property: 'lastRefreshed', lastUpdatedMs: lastRefreshed };
}

// return title case of the string with handling exceptions
export const toProperTitleCase = (str: string) => {
    return str
        .toLowerCase()
        .split(' ')
        .map((word, index) =>
            index === 0 || !TITLE_CASE_EXCEPTION_WORDS.includes(word)
                ? word.charAt(0).toUpperCase() + word.slice(1)
                : word,
        )
        .join(' ');
};

/**
 * Attempts to extract a description for a sub-resource of an entity, if it exists.
 * @param entity ie dataset
 * @param subResource ie field name
 * @returns the description of the sub-resource if it exists, otherwise undefined
 */
export const tryExtractSubResourceDescription = (entity: Entity, subResource: string): string | undefined => {
    // NOTE: we are casting to Dataset, but GlossaryTerms and more future entities can have editableSchemaMetadata
    // We must do a ? check for editableSchemaMetadata/schemaMetadata to avoid runtime errors
    const maybeEditableMetadataDescription = (entity as Dataset).editableSchemaMetadata?.editableSchemaFieldInfo?.find(
        (field) => field.fieldPath === subResource,
    )?.description;
    const maybeSchemaMetadataDescription = (entity as Dataset).schemaMetadata?.fields?.find(
        (field) => field.fieldPath === subResource,
    )?.description;
    return maybeEditableMetadataDescription?.valueOf() || maybeSchemaMetadataDescription?.valueOf();
};
