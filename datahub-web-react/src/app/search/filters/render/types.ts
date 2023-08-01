import { FacetFilter, FacetFilterInput, FacetMetadata } from '../../../../types.generated';

/**
 * The scenario in which filter rendering is required.
 */
export enum FilterScenarioType {
    /**
     * The V1 search experience, including embedded list search.
     */
    SEARCH_V1,
    /**
     * The V2 search + browse experience, NOT inside the "More" dropdown.
     */
    SEARCH_V2_PRIMARY,
    /**
     * The V2 search + browse experience, inside the "More" dropdown.
     */
    SEARCH_V2_SECONDARY,
}

/**
 * Props passed to every filter renderer
 */
export interface FilterRenderProps {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
}
