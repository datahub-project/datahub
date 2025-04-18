import React, { useContext } from 'react';
import { GlobalSettings } from '../../../types.generated';

interface HelpLinkState {
    isEnabled: boolean;
    setIsEnabled: (isEnabled: boolean) => void;
    label: string;
    setLabel: (label: string) => void;
    link: string;
    setLink: (link: string) => void;
    resetHelpLinkState: () => void;
}

export type GlobalSettingsContextType = {
    globalSettings?: GlobalSettings;
    helpLinkState: HelpLinkState;
    refetch: () => void;
};

const DEFAULT_HELP_LINK_STATE: HelpLinkState = {
    isEnabled: false,
    setIsEnabled: () => {},
    label: 'Contact Admin',
    setLabel: () => {},
    link: '',
    setLink: () => {},
    resetHelpLinkState: () => {},
};

export const DEFAULT_CONTEXT = {
    helpLinkState: DEFAULT_HELP_LINK_STATE,
    refetch: () => null,
};

export const GlobalSettingsContext = React.createContext<GlobalSettingsContextType>(DEFAULT_CONTEXT);

export function useGlobalSettingsContext() {
    return useContext(GlobalSettingsContext);
}
