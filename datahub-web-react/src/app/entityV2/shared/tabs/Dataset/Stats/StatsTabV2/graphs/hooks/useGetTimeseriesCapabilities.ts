import { useGetDatasetTimeseriesCapabilityLazyQuery } from '@src/graphql/dataset.generated';
import { useEffect } from 'react';

export default function useGetTimeseriesCapabilities(urn?: string) {
    const [getDatasetTimeseriesCapability, { data, loading }] = useGetDatasetTimeseriesCapabilityLazyQuery({
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (urn) getDatasetTimeseriesCapability({ variables: { urn } });
    }, [urn, getDatasetTimeseriesCapability]);

    return { data: data?.dataset?.timeseriesCapabilities?.assetStats, loading };
}
