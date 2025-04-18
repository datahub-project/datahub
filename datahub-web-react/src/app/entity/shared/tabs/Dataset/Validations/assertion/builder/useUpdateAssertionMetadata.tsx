import { message } from 'antd';
import { useUpdateAssertionMetadataMutation } from '../../../../../../../../graphql/assertion.generated';
import { Assertion } from '../../../../../../../../types.generated';
import { builderStateToUpdateAssertionMetadataVariables } from './utils';
import { AssertionMonitorBuilderState } from './types';
import analytics, { EventType } from '../../../../../../../analytics';

export const useUpdateAssertionMetadataWithBuilderState = (
    builderState: AssertionMonitorBuilderState,
    onUpdate?: (a: Assertion) => void,
): (() => Promise<void>) => {
    const [updateAssertionMetadataMutation] = useUpdateAssertionMetadataMutation();

    const updateAssertionMetadata = () => {
        const assertionMetadataVariables = builderStateToUpdateAssertionMetadataVariables(builderState);

        if (assertionMetadataVariables) {
            return updateAssertionMetadataMutation({
                variables: assertionMetadataVariables,
            })
                .then(({ data, errors }: any) => {
                    if (!errors) {
                        const assertion =
                            data?.upsertDatasetFreshnessAssertionMonitor ||
                            data?.upsertDatasetVolumeAssertionMonitor ||
                            data?.upsertDatasetSqlAssertionMonitor ||
                            data?.upsertDatasetFieldAssertionMonitor ||
                            data?.upsertDatasetSchemaAssertionMonitor;
                        analytics.event({
                            type: EventType.UpdateAssertionMetadataEvent,
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
                    throw new Error('Encountered errors while updating assertion');
                })
                .catch(() => {
                    message.destroy();
                    message.error({ content: 'Failed to update assertion! An unexpected error occurred' });
                });
        }

        message.destroy();
        message.error({ content: 'Failed to update assertion! An unexpected error occurred' });
        return Promise.reject(new Error('Could not find assertionMetadataVariables!'));
    };

    return updateAssertionMetadata;
};
