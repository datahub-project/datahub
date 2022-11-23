import React, { useEffect, useState } from 'react';
import { useGetMeLazyQuery } from '../../graphql/me.generated';
import { CorpUser, PlatformPrivileges } from '../../types.generated';
import { UserContext, LocalState, DEFAULT_STATE, State } from './userContext';

// TODO: Migrate all usage of useAuthenticatedUser to using this provider.

/**
 * Key used when writing user state to local browser state.
 */
const LOCAL_STATE_KEY = 'userState';

/**
 * Loads a persisted object from the local browser storage.
 */
const loadLocalState = () => {
    return JSON.parse(localStorage.getItem(LOCAL_STATE_KEY) || '{}');
};

/**
 * Saves an object to local browser storage.
 */
const saveLocalState = (newState: LocalState) => {
    return localStorage.setItem(LOCAL_STATE_KEY, JSON.stringify(newState));
};

/**
 * A provider of context related to the currently authenticated user.
 */
const UserContextProvider = ({ children }: { children: React.ReactNode }) => {
    /**
     * Stores transient session state, and browser-persistent local state.
     */
    const [state, setState] = useState<State>(DEFAULT_STATE);
    const [localState, setLocalState] = useState<LocalState>(loadLocalState());

    /**
     * Retrieve the current user details once on component mount.
     */
    const [getMe, { data: meData, refetch }] = useGetMeLazyQuery();
    useEffect(() => getMe(), [getMe]);

    const updateLocalState = (newState: LocalState) => {
        saveLocalState(newState);
        setLocalState(newState);
    };

    return (
        <UserContext.Provider
            value={{
                urn: meData?.me?.corpUser?.urn,
                user: meData?.me?.corpUser as CorpUser,
                platformPrivileges: meData?.me?.platformPrivileges as PlatformPrivileges,
                state,
                localState,
                updateState: setState,
                updateLocalState,
                refetchUser: refetch as any,
            }}
        >
            {children}
        </UserContext.Provider>
    );
};

export default UserContextProvider;
