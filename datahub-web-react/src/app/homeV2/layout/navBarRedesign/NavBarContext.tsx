import React, { useContext, useState, ReactNode, useMemo } from 'react';

export enum NavBarStateType {
    Collapsed = 'COLLAPSED',
    Opened = 'OPENED',
    Floating = 'FLOATING',
}

export interface NavBarContextType {
    state: NavBarStateType;
    setState: (newState: NavBarStateType) => void;
    isCollapsed: boolean;
    toggle: () => void;
    selectedKey: string;
    setSelectedKey: (key: string) => void;
}

export const NavBarContext = React.createContext<NavBarContextType>({
    state: NavBarStateType.Collapsed,
    setState: () => {},
    isCollapsed: true,
    toggle: () => {},
    selectedKey: '',
    setSelectedKey: () => {},
});

export const useNavBarContext = () => useContext(NavBarContext);

interface Props {
    children?: ReactNode | undefined;
}

export const NavBarProvider = ({ children }: Props) => {
    const [state, setState] = useState<NavBarStateType>(NavBarStateType.Collapsed);
    const [selectedKey, setSelectedKey] = useState<string>('');

    const isCollapsed = useMemo(() => state === NavBarStateType.Collapsed, [state]);
    const toggle = () => {
        setState((currentState) => {
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
        });
    };

    return (
        <NavBarContext.Provider
            value={{
                state,
                setState,
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
