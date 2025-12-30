import { useApolloClient } from '@apollo/client';
import { Loader, Text } from '@components';
import { message } from 'antd';
import deepEqual from 'fast-deep-equal';
import React, { useCallback, useMemo, useState } from 'react';
import { useHistory, useLocation, useParams } from 'react-router';

import analytics, { EventType } from '@app/analytics';
import { updateListIngestionSourcesCache } from '@app/ingestV2/source/cacheUtils';
import { useUpdateIngestionSource } from '@app/ingestV2/source/hooks/useUpdateSource';
import { IngestionSourceBuilder } from '@app/ingestV2/source/multiStepBuilder/IngestionSourceBuilder';
import { ConnectionDetailsStep } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsStep';
import { ConnectionDetailsSubTitle } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/ConnectionDetailsSubTitle';
import { ScheduleStep } from '@app/ingestV2/source/multiStepBuilder/steps/step3SyncSchedule/ScheduleStep';
import {
    IngestionSourceFormStep,
    MultiStepSourceBuilderState,
    SubmitOptions,
} from '@app/ingestV2/source/multiStepBuilder/types';
import {
    buildOwnerEntities,
    getIngestionSourceMutationInput,
    mapSourceTypeAliases,
    removeExecutionsFromIngestionSource,
} from '@app/ingestV2/source/utils';
import { DiscardUnsavedChangesConfirmationProvider } from '@app/sharedV2/confirmation/DiscardUnsavedChangesConfirmationContext';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';
import { PageRoutes } from '@conf/Global';

import { useGetIngestionSourceQuery } from '@graphql/ingestion.generated';
import { IngestionSource } from '@types';

const STEPS: IngestionSourceFormStep[] = [
    {
        label: 'Connection Details',
        subTitle: <ConnectionDetailsSubTitle />,
        key: 'connectionDetails',
        content: <ConnectionDetailsStep />,
    },
    {
        label: 'Sync Schedule ',
        key: 'syncSchedule',
        content: <ScheduleStep />,
        subTitle: 'Configure an ingestion schedule',
    },
];

export function IngestionSourceUpdatePage() {
    const history = useHistory();
    const location = useLocation();
    const client = useApolloClient();
    const [isSubmitting, setIsSubmitting] = useState<boolean>(false);

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

    const onSubmit = useCallback(
        async (data: MultiStepSourceBuilderState | undefined, options: SubmitOptions | undefined) => {
            if (!data) return undefined;
            setIsSubmitting(true);

            const shouldRun = options?.shouldRun;
            try {
                const source = ingestionSourceData?.ingestionSource as IngestionSource | undefined;
                const input = getIngestionSourceMutationInput(data);
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
                    ingestionOnboardingRedesignV1: true,
                });

                analytics.event({
                    type: EventType.IngestionExitConfigurationEvent,
                    sourceType: input.type,
                    exitType: shouldRun ? 'save_and_run' : 'save_draft',
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

            setIsSubmitting(false);
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
        analytics.event({
            type: EventType.IngestionExitConfigurationEvent,
            sourceType: ingestionSourceData?.ingestionSource?.type,
            exitType: 'cancel',
        });
        history.push(ingestionSourcesListBackUrl ?? PageRoutes.INGESTION);
    }, [history, ingestionSourceData?.ingestionSource?.type, ingestionSourcesListBackUrl]);

    const isDirtyChecker = useCallback(
        (
            initialStateToCheck: MultiStepSourceBuilderState | undefined,
            currentStateToCheck: MultiStepSourceBuilderState | undefined,
        ) => {
            // These fields could have differences without real changes so we exclude them from deepEqual comparison
            const excludedFieldsFromComparison = {
                isConnectionDetailsValid: null,
                owners: null,
                ingestionSource: null,
            };
            const initialStateToCompare = {
                ...(initialStateToCheck ?? {}),
                ...excludedFieldsFromComparison,
            };
            const currentStateToCompare = { ...(currentStateToCheck ?? {}), ...excludedFieldsFromComparison };

            if (!deepEqual(initialStateToCompare, currentStateToCompare)) {
                return true;
            }

            const initialOwnersUrns = new Set(
                initialStateToCheck?.ingestionSource?.ownership?.owners?.map((owner) => owner.owner.urn) ?? [],
            );
            const currentOwnersUrns = new Set(currentStateToCheck?.owners?.map((owner) => owner.urn) ?? []);

            // Check if the owner sets are different
            return !(
                initialOwnersUrns.size === currentOwnersUrns.size &&
                [...initialOwnersUrns].every((value) => currentOwnersUrns.has(value))
            );
        },
        [],
    );

    if (!ingestionSourceData?.ingestionSource || loading) {
        return <Loader />;
    }

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
            <IngestionSourceBuilder
                steps={STEPS}
                onSubmit={onSubmit}
                onCancel={onCancel}
                initialState={{
                    ...mapSourceTypeAliases(removeExecutionsFromIngestionSource(ingestionSourceData.ingestionSource)),
                    ...{ isEditing: true, ingestionSource: ingestionSourceData.ingestionSource as IngestionSource },
                }}
                isDirtyChecker={isDirtyChecker}
            />
        </DiscardUnsavedChangesConfirmationProvider>
    );
}
