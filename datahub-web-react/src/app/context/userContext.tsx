import React from 'react';
import { CorpUser, EntityRelationshipsResult, PlatformPrivileges } from '../../types.generated';

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
    unfinishedTaskCount: number;
    notificationsCount: number;
    proposalCount: number;
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
    userGroups?: EntityRelationshipsResult;
    platformPrivileges?: PlatformPrivileges | null;
    localState: LocalState;
    state: State;
    updateLocalState: (newState: LocalState) => void;
    updateState: (newState: State) => void;
    refetchUser: () => any;
    refetchUnfinishedTaskCount: () => any;
};

export const DEFAULT_LOCAL_STATE: LocalState = {
    selectedViewUrn: undefined,
};

export const DEFAULT_STATE: State = {
    unfinishedTaskCount: 0,
    notificationsCount: 0,
    proposalCount: 0,
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
    userGroups: undefined,
    state: DEFAULT_STATE,
    localState: DEFAULT_LOCAL_STATE,
    updateLocalState: (_: LocalState) => null,
    updateState: (_: State) => null,
    refetchUser: () => null,
    refetchUnfinishedTaskCount: () => null,
};

export const UserContext = React.createContext<UserContextType>(DEFAULT_CONTEXT);
