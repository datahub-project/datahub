import { EntityType } from '../../../types.generated';
import { useGetBrowseResultsV2LazyQuery } from '../../../graphql/browseV2.generated';
import useSidebarFilters from './useSidebarFilters';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    platform?: string | null;
    path: Array<string>;
};

const useBrowseV2Query = ({ entityType, environment, platform, path }: Props) => {
    const { query, orFilters, viewUrn } = useSidebarFilters({ environment, platform });

    const [getBrowse, { data: newData, previousData, loading, error }] = useGetBrowseResultsV2LazyQuery({
        fetchPolicy: 'cache-first',
    });

    const getBrowseApi = () => {
        const page = 1;
        const count = 10;
        const start = Math.max(0, page - 1) * count;

        getBrowse({
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
    };

    const data = error ? null : newData ?? previousData;
    const loaded = !!data || !!error;

    return [
        getBrowseApi,
        {
            loading,
            loaded,
            error,
            ...data?.browseV2,
        } as const,
    ] as const;
};

export default useBrowseV2Query;
