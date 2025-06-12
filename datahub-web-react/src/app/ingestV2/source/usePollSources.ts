import { useEffect, useState } from 'react';

import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import { updateListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';

import { useGetIngestionSourceLazyQuery } from '@graphql/ingestion.generated';
import { IngestionSource, ListIngestionSourcesInput } from '@types';

const REFRESH_INTERVAL_MS = 3000;

interface Props {
    urn: string;
    setFinalSources: React.Dispatch<React.SetStateAction<IngestionSource[]>>;
    setSourcesToRefetch: React.Dispatch<React.SetStateAction<Set<string>>>;
    queryInputs: ListIngestionSourcesInput;
}

export default function usePollSources({ urn, setFinalSources, setSourcesToRefetch, queryInputs }: Props) {
    const [startPolling, setStartPolling] = useState(false);

    const [getIngestionSourceQuery, { client }] = useGetIngestionSourceLazyQuery({
        fetchPolicy: 'network-only',
        onCompleted: (data) => {
            const updatedSource = data?.ingestionSource;
            if (!updatedSource?.executions?.executionRequests) return;

            setFinalSources(
                (prev) =>
                    prev.map((source) =>
                        source.urn === updatedSource.urn ? updatedSource : source,
                    ) as IngestionSource[],
            );

            updateListIngestionSourcesCache(client, updatedSource, queryInputs);

            const stillRunning = updatedSource.executions?.executionRequests?.some((req) =>
                isExecutionRequestActive(req),
            );

            if (!stillRunning) {
                setSourcesToRefetch((prev) => {
                    const newSet = new Set(prev);
                    newSet.delete(urn);
                    return newSet;
                });
            }
        },
    });

    useEffect(() => {
        if (!urn) return undefined;

        const timeout = setTimeout(() => {
            getIngestionSourceQuery({
                variables: { urn },
            });
            setStartPolling(true);
        }, REFRESH_INTERVAL_MS);

        return () => {
            clearTimeout(timeout);
        };
    }, [getIngestionSourceQuery, urn]);

    useEffect(() => {
        if (!startPolling) return undefined;

        const interval = setInterval(() => {
            getIngestionSourceQuery({
                variables: { urn },
            });
        }, REFRESH_INTERVAL_MS);

        return () => clearInterval(interval);
    }, [startPolling, getIngestionSourceQuery, urn]);
}
