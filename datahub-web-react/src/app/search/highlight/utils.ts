import { EntityType } from '../../../types.generated';

export type MatchedFieldName = 'urn' | 'id' | 'name' | 'title' | 'description';

type SupportedEntityTypes = EntityType.Dataset | EntityType.CorpGroup | EntityType.Chart;

export type NormalizedMatchedFieldName = 'urn' | 'name' | 'description';

type MatchedFieldMapping = Record<SupportedEntityTypes, Record<NormalizedMatchedFieldName, MatchedFieldName>>;

export const MATCHED_FIELD_MAPPING: MatchedFieldMapping = {
    [EntityType.Dataset]: {
        name: 'id',
        description: 'description',
        urn: 'urn',
    },
    [EntityType.CorpGroup]: {
        name: 'name',
        description: 'description',
        urn: 'urn',
    },
    [EntityType.Chart]: {
        name: 'title',
        description: 'description',
        urn: 'urn',
    },
};
