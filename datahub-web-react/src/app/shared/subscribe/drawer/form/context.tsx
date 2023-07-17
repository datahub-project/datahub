import React, { Dispatch, ReactNode, createContext, useContext, useReducer } from 'react';
import { Action, State } from './types';
import { initialState, reducer } from './reducer';

const FormStateContext = createContext<State | null>(null);
const FormActionContext = createContext<Dispatch<Action> | null>(null);

const FormStateProvider = ({ children, value }: { children: ReactNode; value: State }) => {
    return <FormStateContext.Provider value={value}>{children}</FormStateContext.Provider>;
};

const FormActionProvider = ({ children, value }: { children: ReactNode; value: Dispatch<Action> }) => {
    return <FormActionContext.Provider value={value}>{children}</FormActionContext.Provider>;
};

const SubscriptionFormProvider = ({ children }: { children: ReactNode }) => {
    const [state, action] = useReducer(reducer, initialState);

    return (
        <FormStateProvider value={state}>
            <FormActionProvider value={action}>{children}</FormActionProvider>
        </FormStateProvider>
    );
};

export const useFormStateContext = () => {
    const context = useContext(FormStateContext);
    if (context === null) throw new Error(`${useFormStateContext.name} must be used under a ${FormStateProvider.name}`);
    return context;
};

export const useFormActionContext = () => {
    const context = useContext(FormActionContext);
    if (context === null)
        throw new Error(`${useFormActionContext.name} must be used under a ${FormActionProvider.name}`);
    return context;
};

export default SubscriptionFormProvider;
