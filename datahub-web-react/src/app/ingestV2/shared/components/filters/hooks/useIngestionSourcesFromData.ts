/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

import { ListIngestionSourcesQuery } from '@graphql/ingestion.generated';
import { IngestionSource } from '@types';

export default function useIngestionSourcesFromData(data: ListIngestionSourcesQuery | undefined, loading: boolean) {
    const [ingestionSources, setIngestionSources] = useState<IngestionSource[]>([]);

    useEffect(() => {
        if (!loading) {
            setIngestionSources(
                (data?.listIngestionSources?.ingestionSources ?? []).map((ingestionSource) => ({
                    ...ingestionSource,
                    executions: undefined,
                    ownership: undefined,
                })),
            );
        }
    }, [data, loading]);

    return ingestionSources;
}
