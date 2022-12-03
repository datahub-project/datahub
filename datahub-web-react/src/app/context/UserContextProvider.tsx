import React, { useCallback, useEffect, useState } from 'react';
import { useGetMeLazyQuery } from '../../graphql/me.generated';
import { useGetGlobalViewsSettingsLazyQuery } from '../../graphql/app.generated';
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
    const [getMe, { data: meData, refetch }] = useGetMeLazyQuery({ fetchPolicy: 'cache-first' });
    useEffect(() => getMe(), [getMe]);

    /**
     * Retrieve the Global View settings once on component mount.
     */
    const [getGlobalViewSettings, { data: settingsData }] = useGetGlobalViewsSettingsLazyQuery({
        fetchPolicy: 'cache-first',
    });
    useEffect(() => getGlobalViewSettings(), [getGlobalViewSettings]);

    const updateLocalState = (newState: LocalState) => {
        saveLocalState(newState);
        setLocalState(newState);
    };

    const setDefaultSelectedView = useCallback(
        (newViewUrn) => {
            updateLocalState({
                ...localState,
                selectedViewUrn: newViewUrn,
            });
        },
        [localState],
    );

    // Update the global default views in local state
    useEffect(() => {
        if (!state.views.loadedGlobalDefaultViewUrn && settingsData?.globalViewsSettings) {
            setState({
                ...state,
                views: {
                    ...state.views,
                    globalDefaultViewUrn: settingsData?.globalViewsSettings?.defaultView,
                    loadedGlobalDefaultViewUrn: true,
                },
            });
        }
    }, [settingsData, state]);

    // Update the personal default views in local state
    useEffect(() => {
        if (!state.views.loadedPersonalDefaultViewUrn && meData?.me?.corpUser?.settings) {
            setState({
                ...state,
                views: {
                    ...state.views,
                    personalDefaultViewUrn: meData?.me?.corpUser?.settings?.views?.defaultView?.urn,
                    loadedPersonalDefaultViewUrn: true,
                },
            });
        }
    }, [meData, state]);

    /**
     * Initialize the default selected view for the logged in user.
     *
     * This is computed as either the user's personal default view (if one is set)
     * else the global default view (if one is set) else undefined as normal.
     *
     * This logic should only run once at initial page load because if a user
     * unselects the current active view, it should NOT be reset to the default they've selected.
     */
    useEffect(() => {
        const shouldSetDefaultView =
            !state.views.hasSetDefaultView &&
            state.views.loadedPersonalDefaultViewUrn &&
            state.views.loadedGlobalDefaultViewUrn;
        if (shouldSetDefaultView) {
            if (localState.selectedViewUrn === undefined) {
                if (state.views.personalDefaultViewUrn) {
                    setDefaultSelectedView(state.views.personalDefaultViewUrn);
                } else if (state.views.globalDefaultViewUrn) {
                    setDefaultSelectedView(state.views.globalDefaultViewUrn);
                }
            }
            setState({
                ...state,
                views: {
                    ...state.views,
                    hasSetDefaultView: true,
                },
            });
        }
    }, [state, localState.selectedViewUrn, setDefaultSelectedView]);

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
