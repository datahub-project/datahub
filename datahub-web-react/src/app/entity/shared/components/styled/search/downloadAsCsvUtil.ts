import {
    CorpGroup,
    CorpUser,
    DashboardStatsSummary,
    DatasetStatsSummary,
    EntityType,
} from '../../../../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../../../../shared/textUtil';
import EntityRegistry from '../../../../EntityRegistry';
import { GenericEntityProperties } from '../../../types';
import { SearchResultInterface } from './types';

const VIEW_COUNT = 'view count';
const UNIQUE_USERS = 'unique users';
const ROW_COUNT = 'row count';
const SIZE_IN_BYTES = 'size in bytes';

const searchCsvDownloadHeader = [
    'urn',
    'name',
    'type',
    'description',
    'user owners',
    'user owner emails',
    'group owners',
    'group owner emails',
    'tags',
    'terms',
    'domain',
    'platform',
    'container',
    'entity url',
];

export const getSearchCsvDownloadHeader = (searchResults?: SearchResultInterface[]) => {
    let result = searchCsvDownloadHeader;
    const sampleResult = searchResults?.[0];

    // this is checking if the degree field is filled out- if it is that
    // means the caller is interested in level of dependency.
    if (typeof sampleResult?.degree === 'number') {
        result = [...result, 'level of dependency'];
    }
    if (searchResults?.find((r) => (r.entity as any).statsSummary?.viewCount !== undefined)) {
        result = [...result, VIEW_COUNT];
    }
    if (searchResults?.find((r) => (r.entity as any).statsSummary?.uniqueUserCountLast30Days !== undefined)) {
        result = [...result, UNIQUE_USERS];
    }
    if (searchResults?.find((r) => (r.entity as any).statsSummary?.rowCount !== undefined)) {
        result = [...result, ROW_COUNT];
    }
    if (searchResults?.find((r) => (r.entity as any).statsSummary?.sizeInBytes !== undefined)) {
        result = [...result, SIZE_IN_BYTES];
    }
    return result;
};

export const transformGenericEntityPropertiesToCsvRow = (
    properties: GenericEntityProperties | null,
    entityUrl: string,
    result: SearchResultInterface,
    csvHeader: string[],
) => {
    let row = [
        // urn
        properties?.urn || '',
        // name
        properties?.name || '',
        // type
        result.entity.type || '',
        // description
        properties?.properties?.description || '',
        // user owners
        properties?.ownership?.owners
            ?.filter((owner) => owner.owner.type === EntityType.CorpUser)
            .map(
                (owner) =>
                    (owner.owner as CorpUser).editableProperties?.displayName ||
                    (owner.owner as CorpUser).properties?.fullName ||
                    (owner.owner as CorpUser).properties?.displayName,
            )
            .join(',') || '',
        // user owner emails
        properties?.ownership?.owners
            ?.filter((owner) => owner.owner.type === EntityType.CorpUser)
            .map(
                (owner) =>
                    (owner.owner as CorpUser).editableProperties?.email || (owner.owner as CorpUser).properties?.email,
            )
            .join(',') || '',
        // group owners
        properties?.ownership?.owners
            ?.filter((owner) => owner.owner.type === EntityType.CorpGroup)
            .map((owner) => (owner.owner as CorpGroup).name)
            .join(',') || '',
        // group owner emails
        properties?.ownership?.owners
            ?.filter((owner) => owner.owner.type === EntityType.CorpGroup)
            .map((owner) => (owner.owner as CorpGroup).properties?.email)
            .join(',') || '',
        // tags
        properties?.globalTags?.tags?.map((tag) => tag.tag.name).join(',') || '',
        // terms
        properties?.glossaryTerms?.terms?.map((term) => term.term.name).join(',') || '',
        // domain
        properties?.domain?.domain?.properties?.name || '',
        // properties
        properties?.platform?.properties?.displayName || capitalizeFirstLetterOnly(properties?.platform?.name) || '',
        // container
        properties?.container?.properties?.name || '',
        // entity url
        window.location.origin + entityUrl,
    ];
    if (typeof result.degree === 'number') {
        // optional level of dependency
        row = [...row, String(result?.degree)];
    }
    if (csvHeader.includes(VIEW_COUNT)) {
        row = [...row, String((properties?.statsSummary as DashboardStatsSummary)?.viewCount) || ''];
    }
    if (csvHeader.includes(UNIQUE_USERS)) {
        row = [...row, String(properties?.statsSummary?.uniqueUserCountLast30Days) || ''];
    }
    if (csvHeader.includes(ROW_COUNT)) {
        row = [...row, String((properties?.statsSummary as DatasetStatsSummary)?.rowCount) || ''];
    }
    if (csvHeader.includes(SIZE_IN_BYTES)) {
        row = [...row, String((properties?.statsSummary as DatasetStatsSummary)?.sizeInBytes) || ''];
    }
    return row;
};

export const transformResultsToCsvRow = (
    results: SearchResultInterface[],
    entityRegistry: EntityRegistry,
    csvHeader: string[],
) => {
    return results.map((result) => {
        const genericEntityProperties = entityRegistry.getGenericEntityProperties(result.entity.type, result.entity);
        const entityUrl = entityRegistry.getEntityUrl(result.entity.type, result.entity.urn);
        return transformGenericEntityPropertiesToCsvRow(genericEntityProperties, entityUrl, result, csvHeader);
    });
};
