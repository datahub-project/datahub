import { useMemo } from 'react';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import applyOrFilterOverrides from '../utils/applyOrFilterOverrides';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';

type Props = {
    environment?: string | null;
    platform?: string | null;
};

const useSidebarFilters = ({ environment, platform }: Props) => {
    const filterOverrides = useMemo(
        () => [
            ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
            ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
        ],
        [environment, platform],
    );

    const excludedFilterFields = useMemo(() => filterOverrides.map((filter) => filter.field), [filterOverrides]);

    const { query, orFilters: orFiltersWithoutOverrides, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const orFilters = useMemo(
        () => applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides),
        [filterOverrides, orFiltersWithoutOverrides],
    );

    return useMemo(() => ({ query, orFilters, viewUrn } as const), [orFilters, query, viewUrn]);
};

export default useSidebarFilters;
