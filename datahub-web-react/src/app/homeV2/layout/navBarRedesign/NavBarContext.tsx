import React, { ReactNode, useCallback, useContext, useEffect, useMemo, useState } from 'react';

export enum NavBarStateType {
    Collapsed = 'COLLAPSED',
    Opened = 'OPENED',
}

/**
 * Key used when writing user state to local browser state.
 */
const LOCAL_STATE_KEY = 'navBarState';

/**
 * Local State is persisted to local storage.
 */
type LocalState = {
    state?: NavBarStateType | null;
};

const loadLocalState = (): LocalState => {
    const raw = JSON.parse(localStorage.getItem(LOCAL_STATE_KEY) || '{}') as LocalState;
    if (raw.state && raw.state !== NavBarStateType.Collapsed && raw.state !== NavBarStateType.Opened) {
        return { ...raw, state: null };
    }
    return raw;
};

const saveLocalState = (newState: LocalState): void => {
    localStorage.setItem(LOCAL_STATE_KEY, JSON.stringify(newState));
};

interface NavBarContextType {
    state: NavBarStateType;
    isCollapsed: boolean;
    toggle: () => void;
    selectedKey: string;
    setSelectedKey: (key: string) => void;
}

// First-time users (no persisted preference) see the nav opened so it's
// discoverable. Once a user toggles, their choice is persisted to
// localStorage and always wins on subsequent visits.
const DEFAULT_NAV_BAR_STATE = NavBarStateType.Opened;

const NavBarContext = React.createContext<NavBarContextType>({
    state: DEFAULT_NAV_BAR_STATE,
    isCollapsed: false,
    toggle: () => {},
    selectedKey: '',
    setSelectedKey: () => {},
});

export const useNavBarContext = () => useContext(NavBarContext);

interface Props {
    children?: ReactNode | undefined;
}

export const NavBarProvider = ({ children }: Props) => {
    const [localState, setLocalState] = useState<LocalState>(loadLocalState());
    const [selectedKey, setSelectedKey] = useState<string>('');

    useEffect(() => {
        saveLocalState(localState);
    }, [localState]);

    const state = useMemo(() => localState.state || DEFAULT_NAV_BAR_STATE, [localState.state]);

    const isCollapsed = useMemo(() => state === NavBarStateType.Collapsed, [state]);

    const toggle = useCallback(() => {
        setLocalState((currentState) => {
            const effective = currentState.state ?? DEFAULT_NAV_BAR_STATE;
            return {
                ...currentState,
                state: effective === NavBarStateType.Opened ? NavBarStateType.Collapsed : NavBarStateType.Opened,
            };
        });
    }, []);

    return (
        <NavBarContext.Provider
            value={{
                state,
                isCollapsed,
                toggle,
                selectedKey,
                setSelectedKey,
            }}
        >
            {children}
        </NavBarContext.Provider>
    );
};
