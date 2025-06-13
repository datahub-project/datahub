import { useCallback, useEffect, useState } from 'react';

import { isExecutionRequestActive } from '@app/ingestV2/executions/utils';
import { updateListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';

import { useGetIngestionSourceLazyQuery } from '@graphql/ingestion.generated';
import { IngestionSource, ListIngestionSourcesInput } from '@types';

const REFRESH_INTERVAL_MS = 3000;
const MAX_EMPTY_RETRIES = 10;

interface Props {
    urn: string;
    setFinalSources: React.Dispatch<React.SetStateAction<IngestionSource[]>>;
    setSourcesToRefetch: React.Dispatch<React.SetStateAction<Set<string>>>;
    setExecutedUrns: React.Dispatch<React.SetStateAction<Set<string>>>;
    queryInputs: ListIngestionSourcesInput;
    isExecutedNow: boolean;
}

export default function usePollSource({
    urn,
    setFinalSources,
    setSourcesToRefetch,
    setExecutedUrns,
    queryInputs,
    isExecutedNow,
}: Props) {
    const [startPolling, setStartPolling] = useState(false);
    const [emptyRetries, setEmptyRetries] = useState(0);

    const removeUrnFromPolling = useCallback(() => {
        setSourcesToRefetch((prev) => {
            const newSet = new Set(prev);
            newSet.delete(urn);
            return newSet;
        });

        setExecutedUrns((prev) => {
            const newSet = new Set(prev);
            newSet.delete(urn);
            return newSet;
        });
    }, [setExecutedUrns, setSourcesToRefetch, urn]);

    const [getIngestionSourceQuery, { client }] = useGetIngestionSourceLazyQuery({
        fetchPolicy: 'network-only',
        onCompleted: (data) => {
            const updatedSource = data?.ingestionSource;
            if (!updatedSource) return;

            const executionRequests = updatedSource.executions?.executionRequests || [];

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

            // Source executed recenty but returned no executions, retry polling
            if (isExecutedNow && executionRequests.length === 0) {
                setEmptyRetries((prev) => prev + 1);
                return;
            }

            // Source is not recently executed and has no executions, remove from polling
            if (!isExecutedNow && executionRequests.length === 0) {
                setSourcesToRefetch((prev) => {
                    const newSet = new Set(prev);
                    newSet.delete(urn);
                    return newSet;
                });
                return;
            }

            // Execution completed, remove from polling
            if (!stillRunning) {
                removeUrnFromPolling();
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
            // No executions returned after MAX_EMPTY_RETRIES, stop polling
            if (isExecutedNow && emptyRetries >= MAX_EMPTY_RETRIES) {
                removeUrnFromPolling();
                return;
            }

            getIngestionSourceQuery({
                variables: { urn },
            });
        }, REFRESH_INTERVAL_MS);

        return () => clearInterval(interval);
    }, [
        startPolling,
        getIngestionSourceQuery,
        urn,
        isExecutedNow,
        emptyRetries,
        setSourcesToRefetch,
        removeUrnFromPolling,
    ]);
}
