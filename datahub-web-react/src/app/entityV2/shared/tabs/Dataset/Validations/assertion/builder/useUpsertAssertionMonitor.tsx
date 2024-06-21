import { message } from 'antd';
import {
    useUpsertDatasetFieldAssertionMonitorMutation,
    useUpsertDatasetFreshnessAssertionMonitorMutation,
    useUpsertDatasetSchemaAssertionMonitorMutation,
    useUpsertDatasetSqlAssertionMonitorMutation,
    useUpsertDatasetVolumeAssertionMonitorMutation,
} from '../../../../../../../../graphql/assertion.generated';
import {
    builderStateToUpsertFieldAssertionMonitorVariables,
    builderStateToUpsertFreshnessAssertionMonitorVariables,
    builderStateToUpsertSchemaAssertionMonitorVariables,
    builderStateToUpsertSqlAssertionMonitorVariables,
    builderStateToUpsertVolumeAssertionMonitorVariables,
} from './utils';
import { Assertion, AssertionType } from '../../../../../../../../types.generated';
import analytics, { EventType } from '../../../../../../../analytics';
import { AssertionMonitorBuilderState } from './types';

export const useUpsertAssertionMonitor = (
    builderState: AssertionMonitorBuilderState,
    onUpdate?: (assertion: Assertion) => void,
    isUpdate?: boolean,
): (() => Promise<any>) => {
    /**
     * Mutations for upserting Assertions, and the Monitor that evaluates them.
     */
    const [upsertFreshnessAssertionMonitorMutation] = useUpsertDatasetFreshnessAssertionMonitorMutation();
    const [upsertVolumeAssertionMonitorMutation] = useUpsertDatasetVolumeAssertionMonitorMutation();
    const [upsertSqlAssertionMonitorMutation] = useUpsertDatasetSqlAssertionMonitorMutation();
    const [upsertFieldAssertionMonitorMutation] = useUpsertDatasetFieldAssertionMonitorMutation();
    const [upsertSchemaAssertionMonitorMutation] = useUpsertDatasetSchemaAssertionMonitorMutation();

    /**
     * Get the mutation to use for the Assertion type being upserted.
     */
    const getUpsertAssertionMutation = () => {
        switch (builderState.assertion?.type) {
            case AssertionType.Freshness:
                return upsertFreshnessAssertionMonitorMutation;
            case AssertionType.Volume:
                return upsertVolumeAssertionMonitorMutation;
            case AssertionType.Sql:
                return upsertSqlAssertionMonitorMutation;
            case AssertionType.Field:
                return upsertFieldAssertionMonitorMutation;
            case AssertionType.DataSchema:
                return upsertSchemaAssertionMonitorMutation;
            default:
                return null;
        }
    };

    /**
     * Get the mutation variables to use for the Assertion type being upserted.
     */
    const getUpsertAssertionVariables = () => {
        switch (builderState.assertion?.type) {
            case AssertionType.Freshness:
                return builderStateToUpsertFreshnessAssertionMonitorVariables(builderState);
            case AssertionType.Volume:
                return builderStateToUpsertVolumeAssertionMonitorVariables(builderState);
            case AssertionType.Sql:
                return builderStateToUpsertSqlAssertionMonitorVariables(builderState);
            case AssertionType.Field:
                return builderStateToUpsertFieldAssertionMonitorVariables(builderState);
            case AssertionType.DataSchema:
                return builderStateToUpsertSchemaAssertionMonitorVariables(builderState);
            default:
                return null;
        }
    };

    /**
     * Create a new AssertionMonitor :)
     *
     * Notice that this creates 2 things:
     *
     * 1. A native assertion associated with the entity.
     * 2. A monitor which is responsible for evaluating this assertion.
     */
    const upsertAssertionMonitor = () => {
        const upsertAssertionMutation = getUpsertAssertionMutation();
        const upsertAssertionVariables = getUpsertAssertionVariables();

        if (upsertAssertionMutation && upsertAssertionVariables) {
            return upsertAssertionMutation({
                variables: upsertAssertionVariables as any,
            })
                .then(({ data, errors }: any) => {
                    if (!errors) {
                        const assertion =
                            data?.upsertDatasetFreshnessAssertionMonitor ||
                            data?.upsertDatasetVolumeAssertionMonitor ||
                            data?.upsertDatasetSqlAssertionMonitor ||
                            data?.upsertDatasetFieldAssertionMonitor;
                        analytics.event({
                            type: isUpdate
                                ? EventType.UpdateAssertionMonitorEvent
                                : EventType.CreateAssertionMonitorEvent,
                            assertionType: builderState.assertion?.type as string,
                            entityUrn: builderState.entityUrn!,
                        });
                        message.success({
                            content: `${isUpdate ? 'Updated' : 'Created'}!`,
                            duration: 3,
                        });
                        onUpdate?.(assertion);
                        return;
                    }
                    throw new Error('Encountered errors while upserting assertion');
                })
                .catch(() => {
                    message.destroy();
                    message.error({
                        content: `Failed to ${
                            isUpdate ? 'update' : 'create'
                        } Assertion Monitor! An unexpected error occurred`,
                    });
                });
        }

        message.destroy();
        message.error({
            content: `Failed to ${isUpdate ? 'update' : 'create'} Assertion Monitor! An unexpected error occurred`,
        });
        return Promise.reject(new Error('Could not find upsertAssertionMutation or upsertAssertionVariables!'));
    };

    return upsertAssertionMonitor;
};
