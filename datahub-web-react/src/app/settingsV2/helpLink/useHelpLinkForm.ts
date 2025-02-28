import { message } from 'antd';
import handleGraphQLError from '@src/app/shared/handleGraphQLError';
import { ErrorResponse } from '@apollo/client/link/error';
import { useUpdateHelpLinkMutation } from '../../../graphql/settings.generated';
import { useGlobalSettingsContext } from '../../context/GlobalSettings/GlobalSettingsContext';

export default function useHelpLinkForm() {
    const { helpLinkState, refetch, globalSettings } = useGlobalSettingsContext();
    const { isEnabled, setIsEnabled, label, setLabel, link, setLink, resetHelpLinkState } = helpLinkState;

    const [updateHelpLink] = useUpdateHelpLinkMutation();

    function onSuccess() {
        refetch();
        message.success('Successfully updated custom help link!');
    }

    function onError(error: ErrorResponse) {
        resetHelpLinkState();
        handleGraphQLError({
            error,
            defaultMessage: 'Failed to update the custom help link. An unexpected error occurred',
            permissionMessage:
                'Unauthorized to update the custom help link. Please contact your DataHub administrator.',
        });
    }

    function toggleHelpLink() {
        if (isEnabled && globalSettings?.visualSettings?.helpLink?.isEnabled) {
            updateHelpLink({ variables: { input: { isEnabled: false } } })
                .then(onSuccess)
                .catch(onError);
        } else if (!isEnabled && link && label) {
            updateHelpLink({ variables: { input: { isEnabled: true, label, link } } })
                .then(onSuccess)
                .catch(onError);
        }
        setIsEnabled(!isEnabled);
    }

    function saveHelpLink() {
        updateHelpLink({ variables: { input: { isEnabled, label, link } } })
            .then(onSuccess)
            .catch(onError);
    }

    return {
        isEnabled,
        toggleHelpLink,
        label,
        setLabel,
        link,
        setLink,
        saveHelpLink,
    };
}
