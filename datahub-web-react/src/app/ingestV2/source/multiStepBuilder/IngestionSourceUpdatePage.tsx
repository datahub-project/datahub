import { useApolloClient } from '@apollo/client';
import { Loader } from '@components';
import { message } from 'antd';
import React, { useCallback, useMemo } from 'react';
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
            initialState={{
                ...mapSourceTypeAliases(removeExecutionsFromIngestionSource(ingestionSourceData.ingestionSource)),
                ...{ isEditing: true, ingestionSource: ingestionSourceData.ingestionSource as IngestionSource },
            }}
        />
    );
}
