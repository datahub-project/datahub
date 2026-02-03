import { SYSTEM_INTERNAL_SOURCE_TYPE } from '@app/ingestV2/constants';

import { useGetNoOfIngestionSourcesQuery } from '@graphql/ingestion.generated';

export const useHasIngestionSources = () => {
    const { data, loading, error } = useGetNoOfIngestionSourcesQuery({
        variables: {
            input: {
                start: 0,
                count: 0,
                filters: [
                    {
                        field: 'sourceType',
                        values: [SYSTEM_INTERNAL_SOURCE_TYPE],
                        negated: true,
                    },
                ],
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    const totalSources = data?.listIngestionSources?.total ?? 0;
    const hasIngestionSources = totalSources > 0;

    return {
        totalSources,
        hasIngestionSources,
        loading,
        error,
    };
};
