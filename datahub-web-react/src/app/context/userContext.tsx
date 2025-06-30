import React from 'react';

import { CustomUserContextState, DEFAULT_CUSTOM_STATE } from '@app/context/CustomUserContext';

import { CorpUser, PlatformPrivileges } from '@types';

/**
 * Local State is persisted to local storage.
 */
export type LocalState = {
    selectedViewUrn?: string | null;
    selectedPath?: string | null;
    selectedSearch?: string | null;
    showBrowseV2Sidebar?: boolean;
};

/**
 * State is transient, it is refreshed on browser refesh.
 */
export type State = {
    views: {
        globalDefaultViewUrn?: string | null;
        personalDefaultViewUrn?: string | null;
        loadedGlobalDefaultViewUrn: boolean;
        loadedPersonalDefaultViewUrn: boolean;
        hasSetDefaultView: boolean;
    };
    customState?: CustomUserContextState;
};

/**
 * Context about the currently-authenticated user.
 */
export type UserContextType = {
    loaded: boolean;
    urn?: string | null;
    user?: CorpUser | null;
    platformPrivileges?: PlatformPrivileges | null;
    localState: LocalState;
    state: State;
    updateLocalState: (newState: LocalState) => void;
    updateState: (newState: State) => void;
    refetchUser: () => any;
};

export const DEFAULT_LOCAL_STATE: LocalState = {
    selectedViewUrn: undefined,
};

export const DEFAULT_STATE: State = {
    views: {
        globalDefaultViewUrn: undefined,
        personalDefaultViewUrn: undefined,
        loadedGlobalDefaultViewUrn: false,
        loadedPersonalDefaultViewUrn: false,
        hasSetDefaultView: false,
    },
    customState: DEFAULT_CUSTOM_STATE,
};

export const DEFAULT_CONTEXT = {
    loaded: false,
    urn: undefined,
    user: undefined,
    state: DEFAULT_STATE,
    localState: DEFAULT_LOCAL_STATE,
    updateLocalState: (_: LocalState) => null,
    updateState: (_: State) => null,
    refetchUser: () => null,
};

export const UserContext = React.createContext<UserContextType>(DEFAULT_CONTEXT);
