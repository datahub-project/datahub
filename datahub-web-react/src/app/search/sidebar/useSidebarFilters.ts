import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import applyOrFilterOverrides from '../utils/applyOrFilterOverrides';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';

type Props = {
    environment?: string | null;
    platform?: string | null;
};

const useSidebarFilters = ({ environment, platform }: Props) => {
    const filterOverrides = [
        ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
        ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
    ];

    const excludedFilterFields = filterOverrides.map((filter) => filter.field);

    const { query, orFilters: orFiltersWithoutOverrides, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const orFilters = applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides);

    return { query, orFilters, viewUrn } as const;
};

export default useSidebarFilters;
