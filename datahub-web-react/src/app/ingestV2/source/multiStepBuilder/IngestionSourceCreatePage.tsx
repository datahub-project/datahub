import { useApolloClient } from '@apollo/client';
import { Text } from '@components';
import { message } from 'antd';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { useIngestionContext } from '@app/ingestV2/IngestionContext';
import { DEFAULT_PAGE_SIZE } from '@app/ingestV2/constants';
import { addToListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';
import { useCreateSource } from '@app/ingestV2/source/hooks/useCreateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { SelectSourceStep } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceStep';
import SelectSourceSubtitle from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/SelectSourceSubtitle';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ConnectionDetailsSubTitle } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsSubTitle';
import {
    IngestionSourceFormStep,
    MultiStepSourceBuilderState,
    SubmitOptions,
} from '@app/ingestV2/source/multiStepBuilder/types';
import {
    DEFAULT_SOURCE_SORT_CRITERION,
    getIngestionSourceMutationInput,
    getIngestionSourceSystemFilter,
    getNewIngestionSourcePlaceholder,
} from '@app/ingestV2/source/utils';
import { DiscardUnsavedChangesConfirmationProvider } from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { PageRoutes } from '@conf/Global';

const PLACEHOLDER_URN = 'placeholder-urn';

export function IngestionSourceCreatePage() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const history = useHistory();
    const client = useApolloClient();
    const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
    const { setCreatedOrUpdatedSource, setShouldRunCreatedOrUpdatedSource } = useIngestionContext();

    const createIngestionSource = useCreateSource();

    const { defaultOwnershipType } = useOwnershipTypes();

    const initialState = {};

    const STEPS: IngestionSourceFormStep[] = useMemo(
        () => [
            {
                label: t('multiStep.createPage.selectSourceStepLabel'),
                subTitle: <SelectSourceSubtitle />,
                key: 'selectSource',
                content: <SelectSourceStep />,
                hideRightPanel: true,
                hideBottomPanel: true,
            },
            {
                label: t('multiStep.builder.connectionDetailsStepLabel'),
                subTitle: <ConnectionDetailsSubTitle />,
                key: 'connectionDetails',
                content: <ConnectionDetailsStep />,
            },
        ],
        [t],
    );

    const onSubmit = useCallback(
        async (data: MultiStepSourceBuilderState | undefined, options: SubmitOptions | undefined) => {
            if (!data) return undefined;
            setIsSubmitting(true);
            const shouldRun = options?.shouldRun;
            const input = getIngestionSourceMutationInput(data);

            try {
                const newSourceUrn = await createIngestionSource(input, data.owners);
                if (!newSourceUrn) return undefined;

                setCreatedOrUpdatedSource(newSourceUrn);
                setShouldRunCreatedOrUpdatedSource(!!shouldRun);

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
                    sort: DEFAULT_SOURCE_SORT_CRITERION,
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
                    content: t('multiStep.createPage.successMessage'),
                    duration: 3,
                });

                history.push(`${PageRoutes.INGESTION}/sources`);
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
        [
            createIngestionSource,
            setCreatedOrUpdatedSource,
            setShouldRunCreatedOrUpdatedSource,
            history,
            client,
            defaultOwnershipType,
            t,
        ],
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
            confirmationModalTitle={t('multiStep.builder.discard.title')}
            confirmationModalContent={
                <Text color="gray" colorLevel={1700}>
                    {t('multiStep.builder.discard.description')}
                </Text>
            }
            confirmButtonText={t('multiStep.builder.discard.confirm')}
            closeButtonText={t('multiStep.builder.discard.close')}
        >
            <IngestionSourceBuilder steps={STEPS} onSubmit={onSubmit} onCancel={onCancel} initialState={initialState} />
        </DiscardUnsavedChangesConfirmationProvider>
    );
}
