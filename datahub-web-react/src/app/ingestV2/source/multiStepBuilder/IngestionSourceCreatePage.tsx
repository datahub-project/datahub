import React, { useCallback } from 'react';
import { useHistory } from 'react-router';

import { DEFAULT_EXECUTOR_ID, SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { SelectSourceStep } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceStep';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ScheduleStep } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStep';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { PageRoutes } from '@conf/Global';

import { StringMapEntryInput } from '@types';

const STEPS: IngestionSourceFormStep[] = [
    {
        label: 'Select Source',
        key: 'selectSource',
        content: <SelectSourceStep />,
        hideRightPanel: true,
        // hidePannel: true,
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

    const createIngestionSource = useCreateSource();

    const formatExtraArgs = (extraArgs): StringMapEntryInput[] => {
        if (extraArgs === null || extraArgs === undefined) return [];
        return extraArgs
            .filter((entry) => entry.value !== null && entry.value !== undefined && entry.value !== '')
            .map((entry) => ({ key: entry.key, value: entry.value }));
    };

    const onSubmit = useCallback(
        async (data: SourceBuilderState | undefined) => {
            if (data) {
                await createIngestionSource({
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
                });
            }

            history.push(PageRoutes.INGESTION, {
                create: true,
            });
        },
        [createIngestionSource, history],
    );

    const onCancel = useCallback(() => {
        history.push(PageRoutes.INGESTION);
    }, [history]);

    return <IngestionSourceBuilder steps={STEPS} onSubmit={onSubmit} onCancel={onCancel} />;
}
