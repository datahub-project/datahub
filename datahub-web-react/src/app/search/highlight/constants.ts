import { EntityType } from '../../../types.generated';

export type MatchedFieldName = 'urn' | 'id' | 'name' | 'title' | 'description' | 'editedDescription';

type MatchFieldMapping = {
    urn: Array<MatchedFieldName>;
    name: Array<MatchedFieldName>;
    description: Array<MatchedFieldName>;
};

export type NormalizedMatchedFieldName = keyof MatchFieldMapping;

type MatchedFieldMappingByEntity = Record<
    EntityType.Dataset | EntityType.CorpGroup | EntityType.Chart,
    MatchFieldMapping
>;

export const MATCHED_FIELD_MAPPING: MatchedFieldMappingByEntity = {
    [EntityType.Dataset]: {
        urn: ['urn'],
        name: ['id'],
        description: ['description', 'editedDescription'],
    },
    [EntityType.CorpGroup]: {
        urn: ['urn'],
        name: ['name'],
        description: ['description', 'editedDescription'],
    },
    [EntityType.Chart]: {
        urn: ['urn'],
        name: ['title'],
        description: ['description', 'editedDescription'],
    },
};
