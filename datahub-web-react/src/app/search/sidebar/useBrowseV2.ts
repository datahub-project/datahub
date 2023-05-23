import { useMemo } from 'react';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import applyOrFilterOverrides from '../utils/applyOrFilterOverrides';
import { useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform: string;
    path: Array<string>;
    skip: boolean;
};

const useBrowseV2 = ({ entityType, environment, platform, path, skip }: Props) => {
    const filterOverrides = useMemo(
        () => [
            ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
            ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
        ],
        [environment, platform],
    );

    const excludedFilterFields = useMemo(() => filterOverrides.map((filter) => filter.field), [filterOverrides]);

    const { query, orFilters, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const page = 1;
    const count = 10;

    const { data, loading, error } = useGetBrowseResultsV2Query({
        skip,
        fetchPolicy: 'cache-first',
        variables: useMemo(
            () => ({
                input: {
                    type: entityType,
                    path,
                    start: (page - 1) * count,
                    count,
                    orFilters: applyOrFilterOverrides(orFilters, filterOverrides),
                    query,
                    viewUrn,
                },
            }),
            [entityType, filterOverrides, orFilters, path, query, viewUrn],
        ),
    });

    return {
        loading,
        loaded: !!data || !!error,
        error,
        ...data?.browseV2,
    } as const;
};

export default useBrowseV2;
