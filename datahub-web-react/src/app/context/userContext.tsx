import React from 'react';
import { CorpUser, PlatformPrivileges } from '../../types.generated';

/**
 * Local State is persisted to local storage.
 */
export type LocalState = {
    selectedViewUrn?: string | null;
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
};

/**
 * Context about the currently-authenticated user.
 */
export type UserContextType = {
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
};

export const DEFAULT_CONTEXT = {
    urn: undefined,
    user: undefined,
    state: DEFAULT_STATE,
    localState: DEFAULT_LOCAL_STATE,
    updateLocalState: (_: LocalState) => null,
    updateState: (_: State) => null,
    refetchUser: () => null,
};

export const UserContext = React.createContext<UserContextType>(DEFAULT_CONTEXT);
