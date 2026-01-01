import { useApolloClient } from '@apollo/client';
import { Text } from '@components';
import { message } from 'antd';
import React, { useCallback, useState } from 'react';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { DEFAULT_PAGE_SIZE } from '@app/ingestV2/constants';
import { addToListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';
import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { SelectSourceStep } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceStep';
import SelectSourceSubtitle from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceSubtitle';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ConnectionDetailsSubTitle } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsSubTitle';
import { ScheduleStep } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStep';
import { ScheduleStepSubtitle } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStepSubtitle';
import { DAILY_MIDNIGHT_CRON_INTERVAL } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/constants';
import {
    IngestionSourceFormStep,
    MultiStepSourceBuilderState,
    SubmitOptions,
} from '@app/ingestV2/source/multiStepBuilder/types';
import {
    getIngestionSourceMutationInput,
    getIngestionSourceSystemFilter,
    getNewIngestionSourcePlaceholder,
} from '@app/ingestV2/source/utils';
import { DiscardUnsavedChangesConfirmationProvider } from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { PageRoutes } from '@conf/Global';

const PLACEHOLDER_URN = 'placeholder-urn';

const STEPS: IngestionSourceFormStep[] = [
    {
        label: 'Select Source',
        subTitle: <SelectSourceSubtitle />,
        key: 'selectSource',
        content: <SelectSourceStep />,
        hideRightPanel: true,
        hideBottomPanel: true,
    },
    {
        label: 'Connection Details',
        subTitle: <ConnectionDetailsSubTitle />,
        key: 'connectionDetails',
        content: <ConnectionDetailsStep />,
    },
    {
        label: 'Sync Schedule ',
        subTitle: <ScheduleStepSubtitle />,
        key: 'syncSchedule',
        content: <ScheduleStep />,
    },
];

export function IngestionSourceCreatePage() {
    const history = useHistory();
    const client = useApolloClient();
    const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

    const createIngestionSource = useCreateSource();

    const { defaultOwnershipType } = useOwnershipTypes();

    const initialState = {
        schedule: {
            interval: DAILY_MIDNIGHT_CRON_INTERVAL,
        },
    };

    const onSubmit = useCallback(
        async (data: MultiStepSourceBuilderState | undefined, options: SubmitOptions | undefined) => {
            if (!data) return undefined;
            setIsSubmitting(true);
            const shouldRun = options?.shouldRun;
            const input = getIngestionSourceMutationInput(data);

            try {
                const newSourceUrn = await createIngestionSource(input, data.owners);
                if (!newSourceUrn) return undefined;

                const newSourcePlaceholder = getNewIngestionSourcePlaceholder(
                    newSourceUrn ?? PLACEHOLDER_URN,
                    data,
                    defaultOwnershipType,
                );

                addToListIngestionSourcesCache(client, newSourcePlaceholder, {
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
                    ingestionOnboardingRedesignV1: true,
                });

                analytics.event({
                    type: EventType.IngestionExitConfigurationEvent,
                    sourceType: input.type,
                    exitType: shouldRun ? 'save_and_run' : 'save_draft',
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

            setIsSubmitting(false);
            return undefined;
        },
        [createIngestionSource, history, client, defaultOwnershipType],
    );

    const onCancel = useCallback(() => {
        analytics.event({
            type: EventType.IngestionExitConfigurationEvent,
            exitType: 'cancel',
        });
        history.push(PageRoutes.INGESTION);
    }, [history]);

    return (
        <DiscardUnsavedChangesConfirmationProvider
            enableRedirectHandling={!isSubmitting}
            confirmationModalTitle="You have unsaved changes"
            confirmationModalContent={
                <Text color="gray" colorLevel={1700}>
                    Exiting now will discard your configuration. You can continue setup or exit and start over later
                </Text>
            }
            confirmButtonText="Continue Setup"
            closeButtonText="Exit Without Saving"
        >
            <IngestionSourceBuilder steps={STEPS} onSubmit={onSubmit} onCancel={onCancel} initialState={initialState} />
        </DiscardUnsavedChangesConfirmationProvider>
    );
}
