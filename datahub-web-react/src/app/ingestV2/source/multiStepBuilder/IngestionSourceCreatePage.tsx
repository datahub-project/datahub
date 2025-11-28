import { useApolloClient } from '@apollo/client';
import { message } from 'antd';
import React, { useCallback } from 'react';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { DEFAULT_PAGE_SIZE } from '@app/ingestV2/constants';
import { DEFAULT_EXECUTOR_ID, SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { addToListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';
import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { SelectSourceStep } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceStep';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ScheduleStep } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStep';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { buildOwnerEntities, getIngestionSourceSystemFilter } from '@app/ingestV2/source/utils';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { PageRoutes } from '@conf/Global';

import { StringMapEntryInput } from '@types';

const PLACEHOLDER_URN = 'placeholder-urn';

const STEPS: IngestionSourceFormStep[] = [
    {
        label: 'Select Source',
        key: 'selectSource',
        content: <SelectSourceStep />,
        hideRightPanel: true,
    },
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

export function IngestionSourceCreatePage() {
    const history = useHistory();
    const client = useApolloClient();

    const createIngestionSource = useCreateSource();

    const formatExtraArgs = (extraArgs): StringMapEntryInput[] => {
        if (extraArgs === null || extraArgs === undefined) return [];
        return extraArgs
            .filter((entry) => entry.value !== null && entry.value !== undefined && entry.value !== '')
            .map((entry) => ({ key: entry.key, value: entry.value }));
    };

    const { defaultOwnershipType } = useOwnershipTypes();

    const onSubmit = useCallback(
        async (data: SourceBuilderState | undefined) => {
            if (!data) return undefined;
            const shouldRun = true; // TODO:: set a real value

            const input = {
                type: data.type as string,
                name: data.name as string,
                config: {
                    recipe: data.config?.recipe as string,
                    version: (data.config?.version?.length && (data.config?.version as string)) || undefined,
                    executorId:
                        (data.config?.executorId?.length && (data.config?.executorId as string)) || DEFAULT_EXECUTOR_ID,
                    debugMode: data.config?.debugMode || false,
                    extraArgs: formatExtraArgs(data.config?.extraArgs || []),
                },
                schedule: data.schedule && {
                    interval: data.schedule?.interval as string,
                    timezone: data.schedule?.timezone as string,
                },
            };

            try {
                const newSourceUrn = await createIngestionSource(input, data.owners);
                if (!newSourceUrn) return undefined;

                const newSource = {
                    urn: newSourceUrn ?? PLACEHOLDER_URN,
                    name: data.name as string,
                    type: data.type as string,
                    config: { executorId: '', recipe: '', version: null, debugMode: null, extraArgs: null },
                    schedule: {
                        interval: data.schedule?.interval || '',
                        timezone: data.schedule?.timezone || null,
                    },
                    platform: null,
                    executions: null,
                    source: null,
                    ownership: {
                        owners: buildOwnerEntities(newSourceUrn, data.owners, defaultOwnershipType),
                        lastModified: {
                            time: 0,
                        },
                        __typename: 'Ownership' as const,
                    },
                    __typename: 'IngestionSource' as const,
                };

                addToListIngestionSourcesCache(client, newSource, {
                    start: 0,
                    count: DEFAULT_PAGE_SIZE,
                    query: undefined,
                    filters: [getIngestionSourceSystemFilter(true)],
                    sort: undefined,
                });

                analytics.event({
                    type: EventType.CreateIngestionSourceEvent,
                    sourceType: input.type,
                    sourceUrn: newSourceUrn,
                    interval: input.schedule?.interval,
                    numOwners: data.owners?.length,
                    outcome: shouldRun ? 'save_and_run' : 'save',
                });

                message.success({
                    content: `Successfully created ingestion source!`,
                    duration: 3,
                });

                history.push(`${PageRoutes.INGESTION}/sources`, {
                    createdOrUpdatedSourceUrn: newSourceUrn,
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
        [createIngestionSource, history, client, defaultOwnershipType],
    );

    const onCancel = useCallback(() => {
        history.push(PageRoutes.INGESTION);
    }, [history]);

    return <IngestionSourceBuilder steps={STEPS} onSubmit={onSubmit} onCancel={onCancel} />;
}
