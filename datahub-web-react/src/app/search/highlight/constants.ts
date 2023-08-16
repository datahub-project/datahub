import { EntityType } from '../../../types.generated';

export type MatchedFieldName =
    | 'id'
    | 'name'
    | 'qualifiedName'
    | 'displayName'
    | 'title'
    | 'description'
    | 'editedDescription'
    | 'editedFieldDescriptions'
    | 'tags'
    | 'fieldTags'
    | 'editedFieldTags'
    | 'glossaryTerms'
    | 'editedFieldGlossaryTerms';

type MatchFieldMapping = {
    name: Array<MatchedFieldName>;
    description: Array<MatchedFieldName>;
    fieldDescription: Array<MatchedFieldName>;
    tags: Array<MatchedFieldName>;
    fieldTags: Array<MatchedFieldName>;
    terms: Array<MatchedFieldName>;
    fieldTerms: Array<MatchedFieldName>;
};

export type NormalizedMatchedFieldName = keyof MatchFieldMapping;

type MatchedFieldMappingByEntity = Record<
    EntityType.Dataset | EntityType.CorpGroup | EntityType.Chart,
    MatchFieldMapping
>;

// todo is there a way to find the @Searchable per entityType field names somehow?

// A mapping of UI use cases to different matched fields
export const MATCHED_FIELD_MAPPING: MatchedFieldMappingByEntity = {
    [EntityType.Dataset]: {
        name: ['name', 'qualifiedName'],
        description: ['description', 'editedDescription'],
        fieldDescription: ['editedFieldDescriptions'],
        tags: ['tags'],
        fieldTags: ['fieldTags', 'editedFieldTags'],
        terms: ['glossaryTerms'],
        fieldTerms: ['editedFieldGlossaryTerms'],
    },
    [EntityType.CorpGroup]: {
        name: ['name', 'displayName'],
        description: ['description', 'editedDescription'],
        fieldDescription: [],
        tags: [],
        fieldTags: [],
        terms: [],
        fieldTerms: [],
    },
    [EntityType.Chart]: {
        name: ['title'],
        description: ['description', 'editedDescription'],
        fieldDescription: [],
        tags: ['tags', 'editedFieldTags'],
        fieldTags: [],
        terms: [],
        fieldTerms: [],
    },

    // todo - create a "Default" one that has a set of common fields, or something like that?
    // todo - or, maybe just merge all of these together into a single mapping field and sort the fieldNames by precedence?
    // the reality is that, do we want to "assume" that displayName is the name we're showing everywhere?
};
