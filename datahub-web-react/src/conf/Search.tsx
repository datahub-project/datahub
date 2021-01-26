import { EntityType } from '../components/shared/EntityTypeUtil';

/*
    Searchable Entity Types
 */
export const SEARCHABLE_ENTITY_TYPES = [EntityType.Dataset, EntityType.User];

/*
    Default AutoComplete field by entity. Required if autocomplete is desired on a SEARCHABLE_ENTITY_TYPE.
*/
export const getAutoCompleteFieldName = (type: EntityType) => {
    switch (type) {
        case EntityType.Dataset:
        case EntityType.User:
            return 'name';
        default:
            return null;
    }
};

/*
    Whether to enable the 'All Entities' search type as the default entity type. 

    If false, the default search entity will be the first entry in 
    SEARCH_ENTITY_TYPES.
*/
export const SHOW_ALL_ENTITIES_SEARCH_TYPE = true;

/*
    Placeholder text appearing in the search bar
*/
export const SEARCH_BAR_PLACEHOLDER_TEXT = 'Search Datasets, People, & more...';

/*
    Number of results shown per search results page
*/
export const RESULTS_PER_PAGE = 10;
