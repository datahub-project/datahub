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

/** Stop blocking search if default-view lookups never settle. */
export const DEFAULT_VIEW_RESOLUTION_TIMEOUT_MS = 10_000;

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
    const [getMe, { data: meData, error: meError, loading: meLoading, called: meCalled, refetch }] = useGetMeLazyQuery({
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
        { data: settingsData, error: settingsError, loading: settingsLoading, called: settingsCalled },
    ] = useGetGlobalViewsSettingsLazyQuery({
        fetchPolicy: 'cache-first',
    });
    useEffect(() => {
        getGlobalViewSettings();
    }, [getGlobalViewSettings]);

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

    // A settled response includes errors and empty payloads so one failed lookup cannot block search.
    const globalQuerySettled = settingsCalled && !settingsLoading;
    const personalQuerySettled = meCalled && !meLoading;

    // Update the global default views in local state
    useEffect(() => {
        if (state.views.loadedGlobalDefaultViewUrn || !globalQuerySettled) return;
        setState((previous) => {
            if (previous.views.loadedGlobalDefaultViewUrn) return previous;
            return {
                ...previous,
                views: {
                    ...previous.views,
                    globalDefaultViewUrn: settingsError ? undefined : settingsData?.globalViewsSettings?.defaultView,
                    loadedGlobalDefaultViewUrn: true,
                },
            };
        });
    }, [globalQuerySettled, settingsData, settingsError, state.views.loadedGlobalDefaultViewUrn]);

    // Update the personal default views in local state
    useEffect(() => {
        if (state.views.loadedPersonalDefaultViewUrn || !personalQuerySettled) return;
        setState((previous) => {
            if (previous.views.loadedPersonalDefaultViewUrn) return previous;
            return {
                ...previous,
                views: {
                    ...previous.views,
                    personalDefaultViewUrn: meError
                        ? undefined
                        : meData?.me?.corpUser?.settings?.views?.defaultView?.urn,
                    loadedPersonalDefaultViewUrn: true,
                },
            };
        });
    }, [meData, meError, personalQuerySettled, state.views.loadedPersonalDefaultViewUrn]);

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
        if (!shouldSetDefaultView) return;

        // Write the default before marking resolution complete, so search starts once with that view.
        if (localState.selectedViewUrn === undefined) {
            const defaultViewUrn = state.views.personalDefaultViewUrn || state.views.globalDefaultViewUrn;
            if (defaultViewUrn) {
                setDefaultSelectedView(defaultViewUrn);
                return;
            }
        }

        setState((previous) => {
            if (previous.views.hasSetDefaultView) return previous;
            return {
                ...previous,
                views: {
                    ...previous.views,
                    hasSetDefaultView: true,
                },
            };
        });
    }, [state, localState.selectedViewUrn, setDefaultSelectedView]);

    useEffect(() => {
        if (state.views.hasSetDefaultView) return undefined;
        const timeoutId = window.setTimeout(() => {
            setState((previous) => {
                if (previous.views.hasSetDefaultView) return previous;
                return {
                    ...previous,
                    views: {
                        ...previous.views,
                        hasSetDefaultView: true,
                    },
                };
            });
        }, DEFAULT_VIEW_RESOLUTION_TIMEOUT_MS);
        return () => window.clearTimeout(timeoutId);
    }, [state.views.hasSetDefaultView]);

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
