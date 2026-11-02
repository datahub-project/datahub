import React, { useCallback, useEffect, useState } from 'react';

import { DEFAULT_STATE, LocalState, State, UserContext } from '@app/context/userContext';

import { useGetGlobalViewsSettingsLazyQuery } from '@graphql/app.generated';
import { useGetMeLazyQuery } from '@graphql/me.generated';
import { CorpUser, PlatformPrivileges } from '@types';

// useGetAuthenticatedUser is a thin wrapper over this provider; prefer useUserContext for new call sites.

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
    const [getMe, { data: meData, loading: meLoading, error: meError, called: meCalled, refetch }] = useGetMeLazyQuery({
        fetchPolicy: 'cache-first',
    });
    useEffect(() => {
        getMe();
    }, [getMe]);

    /**
     * Retrieve the Global View settings once on component mount.
     */
    const [
        getGlobalViewSettings,
        { data: settingsData, loading: globalViewsLoading, error: globalViewsError, called: globalViewsCalled },
    ] = useGetGlobalViewsSettingsLazyQuery({
        fetchPolicy: 'cache-first',
    });
    useEffect(() => {
        getGlobalViewSettings();
    }, [getGlobalViewSettings]);

    // A query has settled on success (including an empty settings payload) or on error.
    // Waiting only for a settings object never finishes for a user or org with no default,
    // or when the request fails, so search would stay blocked.
    const personalDefaultsSettled = meCalled && !meLoading && (meData !== undefined || meError !== undefined);
    const globalDefaultsSettled =
        globalViewsCalled && !globalViewsLoading && (settingsData !== undefined || globalViewsError !== undefined);

    const updateLocalState = useCallback((newState: LocalState) => {
        saveLocalState(newState);
        setLocalState(newState);
    }, []);

    const setDefaultSelectedView = useCallback(
        (newViewUrn) => {
            updateLocalState({
                ...localState,
                selectedViewUrn: newViewUrn,
            });
        },
        [localState, updateLocalState],
    );

    // Update the global default views in local state once the query settles.
    useEffect(() => {
        if (!globalDefaultsSettled) return;
        setState((previous) => {
            if (previous.views.loadedGlobalDefaultViewUrn) return previous;
            return {
                ...previous,
                views: {
                    ...previous.views,
                    globalDefaultViewUrn: settingsData?.globalViewsSettings?.defaultView,
                    loadedGlobalDefaultViewUrn: true,
                },
            };
        });
    }, [globalDefaultsSettled, settingsData]);

    // Update the personal default views in local state once the query settles.
    useEffect(() => {
        if (!personalDefaultsSettled) return;
        setState((previous) => {
            if (previous.views.loadedPersonalDefaultViewUrn) return previous;
            return {
                ...previous,
                views: {
                    ...previous.views,
                    personalDefaultViewUrn: meData?.me?.corpUser?.settings?.views?.defaultView?.urn,
                    loadedPersonalDefaultViewUrn: true,
                },
            };
        });
    }, [personalDefaultsSettled, meData]);

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
                loaded: !!meData,
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
