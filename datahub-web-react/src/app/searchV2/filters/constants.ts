import { LAST_MODIFIED_FILTER } from '@app/searchV2/filters/field/fields';
import { FilterField } from '@app/searchV2/filters/types';
import { EntityType } from '../../../types.generated';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    CHART_TYPE_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    LAST_MODIFIED_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
} from '../utils/constants';

export const SORTED_FILTERS = [
    ENTITY_SUB_TYPE_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    OWNERS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    TAGS_FILTER_NAME,
    DOMAINS_FILTER_NAME,
    LAST_MODIFIED_FILTER_NAME,
    CONTAINER_FILTER_NAME,
];

export const FACETS_TO_ENTITY_TYPES = {
    [DOMAINS_FILTER_NAME]: [EntityType.Domain],
    [GLOSSARY_TERMS_FILTER_NAME]: [EntityType.GlossaryTerm],
    [OWNERS_FILTER_NAME]: [EntityType.CorpUser, EntityType.CorpGroup],
    [TAGS_FILTER_NAME]: [EntityType.Tag],
    [CONTAINER_FILTER_NAME]: [EntityType.Container],
};

// remove legacy filter options as well as new _index and browsePathV2 filter from dropdowns
export const FILTERS_TO_REMOVE = [
    ENTITY_FILTER_NAME, // Use _entityType > typeNames as the default.
    TYPE_NAMES_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    ENTITY_INDEX_FILTER_NAME,
    BROWSE_PATH_V2_FILTER_NAME,
    CONTAINER_FILTER_NAME,
    CHART_TYPE_FILTER_NAME,
];

// filters that should not be shown in the active filters section
export const EXCLUDED_ACTIVE_FILTERS = [BROWSE_PATH_V2_FILTER_NAME];

// Filters not based on facets
export const NON_FACET_FILTER_FIELDS: FilterField[] = [LAST_MODIFIED_FILTER];
