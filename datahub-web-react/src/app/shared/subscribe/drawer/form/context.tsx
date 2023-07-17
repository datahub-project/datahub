import React, { Dispatch, ReactNode, createContext, useContext, useReducer } from 'react';
import { Action, State } from './types';
import { createInitialState, reducer } from './reducer';

const FormStateContext = createContext<State | null>(null);
const FormDispatchContext = createContext<Dispatch<Action> | null>(null);

// todo - we should have a dirty form state?
// and then another state that's just like global state like isPersonal
// and actions that modify that dirty state?
// DrawerStateProvider
// FormStateProvider
// FormActionProvider
const FormStateProvider = ({ children, value }: { children: ReactNode; value: State }) => {
    return <FormStateContext.Provider value={value}>{children}</FormStateContext.Provider>;
};

const FormDispatchProvider = ({ children, value }: { children: ReactNode; value: Dispatch<Action> }) => {
    return <FormDispatchContext.Provider value={value}>{children}</FormDispatchContext.Provider>;
};

const SubscriptionFormProvider = ({ children, isPersonal }: { children: ReactNode; isPersonal: boolean }) => {
    const [state, dispatch] = useReducer(reducer, createInitialState(isPersonal));

    return (
        <FormStateProvider value={state}>
            <FormDispatchProvider value={dispatch}>{children}</FormDispatchProvider>
        </FormStateProvider>
    );
};

export const useFormStateContext = () => {
    const context = useContext(FormStateContext);
    if (context === null) throw new Error(`${useFormStateContext.name} must be used under a ${FormStateProvider.name}`);
    return context;
};

export const useFormDispatchContext = () => {
    const context = useContext(FormDispatchContext);
    if (context === null)
        throw new Error(`${useFormDispatchContext.name} must be used under a ${FormDispatchProvider.name}`);
    return context;
};

export default SubscriptionFormProvider;
