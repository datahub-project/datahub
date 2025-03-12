import React, { useContext, useState, ReactNode, useMemo, useCallback, useEffect } from 'react';

export enum NavBarStateType {
    Collapsed = 'COLLAPSED',
    Opened = 'OPENED',
    Floating = 'FLOATING',
}

/**
 * Key used when writing user state to local browser state.
 */
const LOCAL_STATE_KEY = 'navBarState';

/**
 * Local State is persisted to local storage.
 */
export type LocalState = {
    state?: NavBarStateType | null;
};

/**
 * Loads a persisted object from the local browser storage.
 */
const loadLocalState = () => {
    return JSON.parse(localStorage.getItem(LOCAL_STATE_KEY) || '{}') as LocalState;
};

/**
 * Saves an object to local browser storage.
 */
const saveLocalState = (newState: LocalState) => {
    return localStorage.setItem(LOCAL_STATE_KEY, JSON.stringify(newState));
};

export interface NavBarContextType {
    state: NavBarStateType;
    isCollapsed: boolean;
    toggle: () => void;
    selectedKey: string;
    setSelectedKey: (key: string) => void;
    setDefaultNavBarState: (state: NavBarStateType) => void;
}

export const NavBarContext = React.createContext<NavBarContextType>({
    state: NavBarStateType.Collapsed,
    isCollapsed: true,
    toggle: () => {},
    selectedKey: '',
    setSelectedKey: () => {},
    setDefaultNavBarState: () => {},
});

export const useNavBarContext = () => useContext(NavBarContext);

interface Props {
    children?: ReactNode | undefined;
}

export const NavBarProvider = ({ children }: Props) => {
    // Default state of the nav bar (if there are no state in local storage)
    const [defaultState, setDefaultState] = useState<NavBarStateType>(NavBarStateType.Collapsed);
    const [localState, setLocalState] = useState<LocalState>(loadLocalState());
    const [selectedKey, setSelectedKey] = useState<string>('');

    useEffect(() => {
        saveLocalState(localState);
    }, [localState]);

    const state = useMemo(() => {
        return localState.state || defaultState || NavBarStateType.Collapsed;
    }, [defaultState, localState.state]);

    const updateState = useCallback((newState: NavBarStateType) => {
        setLocalState((currentState) => ({ ...currentState, state: newState }));
    }, []);

    const getNewState = useCallback((currentState: NavBarStateType): NavBarStateType => {
        switch (currentState) {
            case NavBarStateType.Collapsed:
                return NavBarStateType.Opened;
            case NavBarStateType.Opened:
                return NavBarStateType.Collapsed;
            case NavBarStateType.Floating:
                return NavBarStateType.Collapsed;
            default:
                return NavBarStateType.Collapsed;
        }
    }, []);

    const isCollapsed = useMemo(() => state === NavBarStateType.Collapsed, [state]);

    const toggle = useCallback(() => {
        const newState = getNewState(state);
        updateState(newState);
    }, [state, getNewState, updateState]);

    return (
        <NavBarContext.Provider
            value={{
                state,
                isCollapsed,
                toggle,
                selectedKey,
                setSelectedKey,
                setDefaultNavBarState: setDefaultState,
            }}
        >
            {children}
        </NavBarContext.Provider>
    );
};
