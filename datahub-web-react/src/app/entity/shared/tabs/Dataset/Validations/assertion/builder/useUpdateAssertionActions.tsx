import { message } from 'antd';
import { useUpdateAssertionActionsMutation } from '../../../../../../../../graphql/assertion.generated';
import { Assertion } from '../../../../../../../../types.generated';
import { builderStateToUpdateAssertionActionsVariables } from './utils';

export const useUpdateAssertionActions = (urn, builderState, onUpdate): (() => void) => {
    const [updateAssertionActionsMutation] = useUpdateAssertionActionsMutation();

    /**
     * Updates the actions for any assertion.
     */
    const updateAssertionActions = () => {
        const updateAssertionActionsVariables = builderStateToUpdateAssertionActionsVariables(urn, builderState);
        updateAssertionActionsMutation({
            variables: updateAssertionActionsVariables,
        })
            .then(({ data, errors }) => {
                if (!errors) {
                    message.success({
                        content: `Updated Assertion!`,
                        duration: 3,
                    });
                    onUpdate?.(data?.updateAssertionActions as Assertion);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to update Assertion! An unexpected error occurred' });
            });
    };

    return updateAssertionActions;
};
