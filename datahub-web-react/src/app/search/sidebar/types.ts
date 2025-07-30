import useGetSearchQueryInputs from '@app/search/useGetSearchQueryInputs';

export type SidebarFilters = Pick<
    ReturnType<typeof useGetSearchQueryInputs>,
    'entityFilters' | 'query' | 'orFilters' | 'viewUrn'
>;
