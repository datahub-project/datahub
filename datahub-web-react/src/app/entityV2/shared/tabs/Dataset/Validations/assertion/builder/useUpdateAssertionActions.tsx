import { message } from 'antd';
import { useUpdateAssertionActionsMutation } from '../../../../../../../../graphql/assertion.generated';
import { Assertion } from '../../../../../../../../types.generated';
import { builderStateToUpdateAssertionActionsVariables } from './utils';
import { AssertionMonitorBuilderState } from './types';
import analytics, { EventType } from '../../../../../../../analytics';

export const useUpdateAssertionActionsWithBuilderState = (builderState: AssertionMonitorBuilderState, onUpdate?: (a: Assertion) => void): (() => Promise<void>) => {
    const [updateAssertionActionsMutation] = useUpdateAssertionActionsMutation();

    const updateAssertionActions = () => {
        const assertionActionVariables = builderStateToUpdateAssertionActionsVariables(builderState);

        if (assertionActionVariables) {
            return updateAssertionActionsMutation({
                variables: assertionActionVariables,
            })
                .then(({ data, errors }: any) => {
                    if (!errors) {
                        const assertion =
                            data?.upsertDatasetFreshnessAssertionMonitor ||
                            data?.upsertDatasetVolumeAssertionMonitor ||
                            data?.upsertDatasetSqlAssertionMonitor ||
                            data?.upsertDatasetFieldAssertionMonitor;
                        analytics.event({
                            type: EventType.UpdateAssertionActionsEvent,
                            assertionType: builderState.assertion?.type as string,
                            assertionUrn: builderState.assertion?.urn as string,
                            entityUrn: builderState.entityUrn as string,
                        });
                        message.success({
                            content: 'Updated!',
                            duration: 3,
                        });
                        onUpdate?.(assertion);
                        return;
                    }
                    throw new Error('Encountered errors while updating assertion actions');
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to update assertion actions! An unexpected error occurred' });
                });
        }

        message.destroy();
        message.error({ content: 'Failed to update assertion actions! An unexpected error occurred' });
        return Promise.reject(new Error('Could not find assertionActionVariables!'));
    };

    return updateAssertionActions;
};