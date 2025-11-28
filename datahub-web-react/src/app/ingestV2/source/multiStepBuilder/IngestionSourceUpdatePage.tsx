import { useApolloClient } from '@apollo/client';
import { Loader } from '@components';
import { message } from 'antd';
import React, { useCallback, useMemo } from 'react';
import { useHistory, useLocation, useParams } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { updateListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';
import { useUpdateIngestionSource } from '@app/ingestV2/source/hooks/useUpdateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ScheduleStep } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStep';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import {
    buildOwnerEntities,
    mapSourceTypeAliases,
    removeExecutionsFromIngestionSource,
} from '@app/ingestV2/source/utils';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { PageRoutes } from '@conf/Global';

import { useGetIngestionSourceQuery } from '@graphql/ingestion.generated';
import { IngestionSource, StringMapEntryInput } from '@types';

const STEPS: IngestionSourceFormStep[] = [
    {
        label: 'Connection Details',
        key: 'connectionDetails',
        content: <ConnectionDetailsStep />,
    },
    {
        label: 'Sync Schedule ',
        key: 'syncSchedule',
        content: <ScheduleStep />,
    },
];

export function IngestionSourceUpdatePage() {
    const history = useHistory();
    const location = useLocation();
    const client = useApolloClient();

    const ingestionSourcesListQueryInputs = useMemo(() => location.state?.queryInputs, [location.state]);
    const ingestionSourcesListBackUrl = useMemo(() => location.state?.backUrl, [location.state]);

    const { defaultOwnershipType } = useOwnershipTypes();

    const { urn } = useParams<{ urn: string }>();

    const { data: ingestionSourceData, loading } = useGetIngestionSourceQuery({
        variables: {
            urn,
        },
    });

    const updateIngestionSource = useUpdateIngestionSource();

    const formatExtraArgs = (extraArgs): StringMapEntryInput[] => {
        if (extraArgs === null || extraArgs === undefined) return [];
        return extraArgs
            .filter((entry) => entry.value !== null && entry.value !== undefined && entry.value !== '')
            .map((entry) => ({ key: entry.key, value: entry.value }));
    };

    const onSubmit = useCallback(
        async (data: SourceBuilderState | undefined) => {
            if (!data) return undefined;

            const shouldRun = true; // TODO:: set a real value
            try {
                const source = ingestionSourceData?.ingestionSource as IngestionSource | undefined;
                const input = {
                    type: data.type as string,
                    name: data.name as string,
                    config: {
                        recipe: data.config?.recipe as string,
                        version: (data.config?.version?.length && (data.config?.version as string)) || undefined,
                        executorId:
                            (data.config?.executorId?.length && (data.config?.executorId as string)) ||
                            DEFAULT_EXECUTOR_ID,
                        debugMode: data.config?.debugMode || false,
                        extraArgs: formatExtraArgs(data.config?.extraArgs || []),
                    },
                    schedule: data.schedule && {
                        interval: data.schedule?.interval as string,
                        timezone: data.schedule?.timezone as string,
                    },
                };

                await updateIngestionSource(urn, input, data.owners, source?.ownership?.owners || []);

                if (ingestionSourcesListQueryInputs) {
                    const updatedSource = {
                        config: {
                            ...data.config,
                            version: null,
                        },
                        name: data.name,
                        type: data.type,
                        schedule: data.schedule || null,
                        urn,
                        ownership: {
                            owners: buildOwnerEntities(urn, data.owners, defaultOwnershipType) || [],
                        },
                    };
                    updateListIngestionSourcesCache(client, updatedSource, ingestionSourcesListQueryInputs, false);
                }

                analytics.event({
                    type: EventType.UpdateIngestionSourceEvent,
                    sourceType: input.type,
                    sourceUrn: urn,
                    interval: input.schedule?.interval,
                    numOwners: data.owners?.length,
                    outcome: shouldRun ? 'save_and_run' : 'save',
                });

                message.success({
                    content: `Successfully updated ingestion source!`,
                    duration: 3,
                });

                history.push(ingestionSourcesListBackUrl ?? PageRoutes.INGESTION, {
                    createdOrUpdatedSourceUrn: urn,
                    shouldRun,
                });
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: e.message,
                        duration: 3,
                    });
                }
            }
            return undefined;
        },
        [
            updateIngestionSource,
            urn,
            history,
            ingestionSourceData,
            client,
            ingestionSourcesListQueryInputs,
            ingestionSourcesListBackUrl,
            defaultOwnershipType,
        ],
    );

    const onCancel = useCallback(() => {
        history.push(ingestionSourcesListBackUrl ?? PageRoutes.INGESTION);
    }, [history, ingestionSourcesListBackUrl]);

    if (!ingestionSourceData?.ingestionSource || loading) {
        return <Loader />;
    }

    return (
        <IngestionSourceBuilder
            steps={STEPS}
            onSubmit={onSubmit}
            onCancel={onCancel}
            initialState={mapSourceTypeAliases(
                removeExecutionsFromIngestionSource(ingestionSourceData.ingestionSource),
            )}
        />
    );
}
