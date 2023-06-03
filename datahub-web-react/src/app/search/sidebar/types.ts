import useGetSearchQueryInputs from '../useGetSearchQueryInputs';

export type SidebarFilters = Pick<ReturnType<typeof useGetSearchQueryInputs>, 'query' | 'orFilters' | 'viewUrn'>;
