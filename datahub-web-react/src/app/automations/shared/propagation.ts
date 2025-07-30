export const GENERIC_PROPAGATION_ACTION =
    'datahub_integrations.propagation.propagation.generic_propagation_action.GenericPropagationAction';

export function getGenericPropagationTemplateKey(recipe: { action?: { config?: GenericPropagationConfig } }) {
    const [propagatedMetadata, ...rest] = Object.keys(
        recipe.action?.config?.propagation_rule?.metadata_propagated || {},
    );
    if (!propagatedMetadata) {
        console.debug('No metadata propagated found in recipe:', recipe);
        return undefined;
    }
    if (rest.length) {
        console.debug('Multiple metadata propagated found in recipe', recipe);
    }
    return `${GENERIC_PROPAGATION_ACTION}-${propagatedMetadata}`;
}

export enum LookupType {
    ASPECT = 'aspect',
    RELATIONSHIP = 'relationship',
}

export enum PropagationRelationships {
    UPSTREAM = 'upstream',
    DOWNSTREAM = 'downstream',
}

export enum PropagatedMetadata {
    TAGS = 'tags',
    TERMS = 'terms',
    DOCUMENTATION = 'documentation',
    DOMAIN = 'domain',
    STRUCTURED_PROPERTIES = 'structuredProperties',
}

export type AspectLookup = {
    lookup_type: 'aspect';
    aspect_name: string;
    field: string; // Can denote a chain of fields, separated by periods
    // Whether to look at the MCL's previous or new aspect value.
    // Will usually be new aspect value.
    use_previous_aspect?: boolean;
};

export type RelationshipLookup = {
    lookup_type: LookupType.RELATIONSHIP;
    type: PropagationRelationships;
    relationship_names?: string[];
    // Whether to search for urn as search or destination.
    // Will usually be destination.
    is_source?: boolean;
};

export type EntityLookup = AspectLookup | RelationshipLookup;

type FilterOperator =
    | 'CONTAIN'
    | 'EQUAL'
    | 'IEQUAL'
    | 'IN'
    | 'EXISTS'
    | 'GREATER_THAN'
    | 'GREATER_THAN_OR_EQUAL_TO'
    | 'LESS_THAN'
    | 'LESS_THAN_OR_EQUAL_TO'
    | 'START_WITH'
    | 'END_WITH'
    | 'DESCENDANTS_INCL'
    | 'ANCESTORS_INCL'
    | 'RELATED_INCL';

export type SearchFilterRule = {
    field: string;
    condition: FilterOperator;
    values: string[];
    negated?: boolean;
};

export type PropagationRule = {
    metadata_propagated?: Partial<Record<PropagatedMetadata, Record<string, any>>>;
    entity_types?: string[];
    target_urn_resolution: EntityLookup[];
    bootstrap?: SearchFilterRule[];
};

export type GenericPropagationConfig = {
    propagation_rule: PropagationRule;
    enabled?: boolean;
};
