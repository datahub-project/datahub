import { useCallback, useEffect, useState } from 'react';

import { GlobalSettings } from '@types';

const DEFAULT_LABEL = 'Contact Admin';

export default function useHelpLinkState(globalSettings?: GlobalSettings | null) {
    const [isEnabled, setIsEnabled] = useState(false);
    const [label, setLabel] = useState(DEFAULT_LABEL);
    const [link, setLink] = useState('');

    const resetHelpLinkState = useCallback(() => {
        if (globalSettings?.visualSettings?.helpLink) {
            const { helpLink } = globalSettings.visualSettings;
            setIsEnabled(helpLink.isEnabled);
            setLabel(helpLink.label);
            setLink(helpLink.link);
        } else {
            setIsEnabled(false);
            setLabel(DEFAULT_LABEL);
            setLink('');
        }
    }, [globalSettings?.visualSettings]);

    useEffect(() => {
        resetHelpLinkState();
    }, [globalSettings, resetHelpLinkState]);

    return {
        isEnabled,
        setIsEnabled,
        label,
        setLabel,
        link,
        setLink,
        resetHelpLinkState,
    };
}
