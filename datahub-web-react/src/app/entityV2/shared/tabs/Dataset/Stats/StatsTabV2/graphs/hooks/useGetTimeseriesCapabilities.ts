/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect } from 'react';

import { useGetDatasetTimeseriesCapabilityLazyQuery } from '@src/graphql/dataset.generated';

export default function useGetTimeseriesCapabilities(urn?: string | null) {
    const [getDatasetTimeseriesCapability, { data, loading }] = useGetDatasetTimeseriesCapabilityLazyQuery({
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        if (urn) getDatasetTimeseriesCapability({ variables: { urn } });
    }, [urn, getDatasetTimeseriesCapability]);

    return { data: data?.dataset?.timeseriesCapabilities?.assetStats, loading };
}
