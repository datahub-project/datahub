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
    const filterOverrides = [
        ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
        ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
    ];

    const excludedFilterFields = filterOverrides.map((filter) => filter.field);

    const { query, orFilters, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const page = 1;
    const count = 10;

    const {
        data: newData,
        previousData,
        loading,
        error,
    } = useGetBrowseResultsV2Query({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                type: entityType,
                path,
                start: (page - 1) * count,
                count,
                orFilters: applyOrFilterOverrides(orFilters, filterOverrides),
                query,
                viewUrn,
            },
        },
    });

    const data = error ? null : newData ?? previousData;

    return {
        loading,
        loaded: !!data || !!error,
        error,
        ...data?.browseV2,
    } as const;
};

export default useBrowseV2;
