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
