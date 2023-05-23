import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import applyOrFilterOverrides from '../utils/applyOrFilterOverrides';
import { useGetBrowseResultsV2LazyQuery } from '../../../graphql/browseV2.generated';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform: string;
    path: Array<string>;
};

const useBrowseV2Query = ({ entityType, environment, platform, path }: Props) => {
    const filterOverrides = [
        ...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : []),
        ...(platform ? [{ field: PLATFORM_FILTER_NAME, value: platform }] : []),
    ];

    const excludedFilterFields = filterOverrides.map((filter) => filter.field);

    const { query, orFilters, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const page = 1;
    const count = 10;

    const [getBrowse, { data: newData, previousData, loading, error }] = useGetBrowseResultsV2LazyQuery({
        fetchPolicy: 'cache-first',
    });

    const getBrowseApi = () => {
        getBrowse({
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
    };

    const data = error ? null : newData ?? previousData;

    return [
        getBrowseApi,
        {
            loading,
            loaded: !!data || !error,
            error,
            ...data?.browseV2,
        } as const,
    ] as const;
};

export default useBrowseV2Query;
