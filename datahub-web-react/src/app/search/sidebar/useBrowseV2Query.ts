import { EntityType } from '../../../types.generated';
import { useGetBrowseResultsV2Query } from '../../../graphql/browseV2.generated';
import useSidebarFilters from './useSidebarFilters';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    path: Array<string>;
    skip: boolean;
};

const useBrowseV2Query = ({ entityType, environment, platform, path, skip }: Props) => {
    const { query, orFilters, viewUrn } = useSidebarFilters({ environment, platform });

    const page = 1;
    const count = 10;
    const start = Math.max(0, page - 1) * count;

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
                start,
                count,
                orFilters,
                query,
                viewUrn,
            },
        },
    });

    const data = error ? null : newData ?? previousData;
    const loaded = !!data || !!error;

    return {
        loading,
        loaded,
        error,
        ...data?.browseV2,
    } as const;
};

export default useBrowseV2Query;
