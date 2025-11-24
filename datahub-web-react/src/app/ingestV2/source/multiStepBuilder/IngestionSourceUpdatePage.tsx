import { Loader } from '@components';
import React, { useCallback } from 'react';
import { useHistory, useParams } from 'react-router';

import { DEFAULT_EXECUTOR_ID, SourceBuilderState } from '@app/ingestV2/source/builder/types';
import { useUpdateIngestionSource } from '@app/ingestV2/source/hooks/useUpdateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { SelectSourceStep } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceStep';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ScheduleStep } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStep';
import { IngestionSourceFormStep } from '@app/ingestV2/source/multiStepBuilder/types';
import { mapSourceTypeAliases, removeExecutionsFromIngestionSource } from '@app/ingestV2/source/utils';
import { PageRoutes } from '@conf/Global';

import { useGetIngestionSourceQuery } from '@graphql/ingestion.generated';
import { StringMapEntryInput } from '@types';

const STEPS: IngestionSourceFormStep[] = [
    {
        label: 'Select Source',
        key: 'selectSource',
        content: <SelectSourceStep />,
        hideRightPanel: true,
        hideBottomPanel: true,
        disabled: true,
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

export function IngestionSourceUpdatePage() {
    const history = useHistory();

    const { urn } = useParams<{ urn: string }>();

    const { data: ingestionSourceData, loading } = useGetIngestionSourceQuery({
        variables: {
            urn,
        },
    });

    const createIngestionSource = useUpdateIngestionSource();

    const formatExtraArgs = (extraArgs): StringMapEntryInput[] => {
        if (extraArgs === null || extraArgs === undefined) return [];
        return extraArgs
            .filter((entry) => entry.value !== null && entry.value !== undefined && entry.value !== '')
            .map((entry) => ({ key: entry.key, value: entry.value }));
    };

    const onSubmit = useCallback(
        async (data: SourceBuilderState | undefined) => {
            if (data) {
                await createIngestionSource(urn, {
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
        [createIngestionSource, urn, history],
    );

    const onCancel = useCallback(() => {
        history.push(PageRoutes.INGESTION);
    }, [history]);

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
