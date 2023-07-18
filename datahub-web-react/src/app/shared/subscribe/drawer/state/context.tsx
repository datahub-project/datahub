import React, { Dispatch, ReactNode, createContext, useContext, useReducer } from 'react';
import { Action, State } from './types';
import { createInitialState, reducer } from './reducer';

const DrawerStateContext = createContext<State | null>(null);
const DrawerDispatchContext = createContext<Dispatch<Action> | null>(null);

// todo - we should have a dirty form state?
// and then another state that's just like global state like isPersonal
// and actions that modify that dirty state?
// DrawerStateProvider
// FormStateProvider
// FormActionProvider
const DrawerStateProvider = ({ children, value }: { children: ReactNode; value: State }) => {
    return <DrawerStateContext.Provider value={value}>{children}</DrawerStateContext.Provider>;
};

const DrawerDispatchProvider = ({ children, value }: { children: ReactNode; value: Dispatch<Action> }) => {
    return <DrawerDispatchContext.Provider value={value}>{children}</DrawerDispatchContext.Provider>;
};

const SubscriptionDrawerProvider = ({ children, isPersonal }: { children: ReactNode; isPersonal: boolean }) => {
    const [state, dispatch] = useReducer(reducer, createInitialState(isPersonal));

    return (
        <DrawerStateProvider value={state}>
            <DrawerDispatchProvider value={dispatch}>{children}</DrawerDispatchProvider>
        </DrawerStateProvider>
    );
};

export const useDrawerState = () => {
    const context = useContext(DrawerStateContext);
    if (context === null) throw new Error(`${useDrawerState.name} must be used under a ${DrawerStateProvider.name}`);
    return context;
};

export const useDrawerDispatch = () => {
    const context = useContext(DrawerDispatchContext);
    if (context === null)
        throw new Error(`${useDrawerDispatch.name} must be used under a ${DrawerDispatchProvider.name}`);
    return context;
};

export default SubscriptionDrawerProvider;
