import { message } from 'antd';
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

    function toggleHelpLink() {
        if (isEnabled && globalSettings?.visualSettings?.helpLink?.isEnabled) {
            updateHelpLink({ variables: { input: { isEnabled: false } } })
                .then(onSuccess)
                .catch(resetHelpLinkState);
        } else if (!isEnabled && link && label) {
            updateHelpLink({ variables: { input: { isEnabled: true, label, link } } })
                .then(onSuccess)
                .catch(resetHelpLinkState);
        }
        setIsEnabled(!isEnabled);
    }

    function saveHelpLink() {
        updateHelpLink({ variables: { input: { isEnabled, label, link } } })
            .then(onSuccess)
            .catch(resetHelpLinkState);
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
