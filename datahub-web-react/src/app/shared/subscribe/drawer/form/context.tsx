import React, { Dispatch, ReactNode, createContext, useContext, useReducer } from 'react';
import { Action, State } from './types';
import { initialState, reducer } from './reducer';

const DrawerStateContext = createContext<State | null>(null);
const DrawerActionContext = createContext<Dispatch<Action> | null>(null);

const DrawerStateProvider = ({ children, value }: { children: ReactNode; value: State }) => {
    return <DrawerStateContext.Provider value={value}>{children}</DrawerStateContext.Provider>;
};

const DrawerActionProvider = ({ children, value }: { children: ReactNode; value: Dispatch<Action> }) => {
    return <DrawerActionContext.Provider value={value}>{children}</DrawerActionContext.Provider>;
};

const SubscriptionDrawerProvider = ({ children }: { children: ReactNode }) => {
    const [state, action] = useReducer(reducer, initialState);

    return (
        <DrawerStateProvider value={state}>
            <DrawerActionProvider value={action}>{children}</DrawerActionProvider>
        </DrawerStateProvider>
    );
};

export const useDrawerStateContext = () => {
    const context = useContext(DrawerStateContext);
    if (context === null)
        throw new Error(`${useDrawerStateContext.name} must be used under a ${DrawerStateProvider.name}`);
    return context;
};

export const useDrawerActionContext = () => {
    const context = useContext(DrawerActionContext);
    if (context === null)
        throw new Error(`${useDrawerActionContext.name} must be used under a ${DrawerActionProvider.name}`);
    return context;
};

export default SubscriptionDrawerProvider;
