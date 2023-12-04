import useGetSearchQueryInputs from '../useGetSearchQueryInputs';

export type SidebarFilters = Pick<
    ReturnType<typeof useGetSearchQueryInputs>,
    'entityFilters' | 'query' | 'orFilters' | 'viewUrn'
>;
