import { CorpGroup, CorpUser, EntityType } from '../../../../../../types.generated';
import EntityRegistry from '../../../../EntityRegistry';
import { GenericEntityProperties } from '../../../types';
import { SearchResultInterface } from './types';

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

export const getSearchCsvDownloadHeader = (sampleResult?: SearchResultInterface) => {
    let result = searchCsvDownloadHeader;

    // this is checking if the degree field is filled out- if it is that
    // means the caller is interested in level of dependency.
    if (typeof sampleResult?.degree === 'number') {
        result = [...result, 'level of dependency'];
    }
    return result;
};

export const transformGenericEntityPropertiesToCsvRow = (
    properties: GenericEntityProperties | null,
    entityUrl: string,
    result: SearchResultInterface,
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
        properties?.domain?.properties?.name || '',
        // properties
        properties?.platform?.properties?.displayName || '',
        // container
        properties?.container?.properties?.name || '',
        // entity url
        window.location.origin + entityUrl,
    ];
    if (typeof result.degree === 'number') {
        // optional level of dependency
        row = [...row, String(result?.degree)];
    }
    return row;
};

export const transformResultsToCsvRow = (results: SearchResultInterface[], entityRegistry: EntityRegistry) => {
    return results.map((result) => {
        const genericEntityProperties = entityRegistry.getGenericEntityProperties(result.entity.type, result.entity);
        const entityUrl = entityRegistry.getEntityUrl(result.entity.type, result.entity.urn);
        return transformGenericEntityPropertiesToCsvRow(genericEntityProperties, entityUrl, result);
    });
};
