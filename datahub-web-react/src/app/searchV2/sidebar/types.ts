import useGetSearchQueryInputs from '../useGetSearchQueryInputs';

export type SidebarFilters = Pick<
    ReturnType<typeof useGetSearchQueryInputs>,
    'entityFilters' | 'query' | 'orFilters' | 'viewUrn'
>;

export enum BrowseMode {
    /**
     * Browse by entity type
     */
    ENTITY_TYPE = 'ENTITY_TYPE',
    /**
     * Browse by platform
     */
    PLATFORM = 'PLATFORM',
}
