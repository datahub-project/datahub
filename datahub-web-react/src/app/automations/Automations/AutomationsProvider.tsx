import React, { ReactNode, useCallback, useContext, useMemo, useState } from 'react';

import { ActionPipeline } from '@src/types.generated';

import { useListActionPipelinesQuery } from '@graphql/actionPipeline.generated';

export interface AutomationsContextType {
    automations: ActionPipeline[];
    isLoading: boolean;
    refetchAutomations?: (withTimeout: boolean) => void;
}

export const AutomationsContext = React.createContext<AutomationsContextType>({
    automations: [],
    isLoading: false,
    refetchAutomations: () => null,
});

export const useAutomationsContext = () => useContext(AutomationsContext);

interface Props {
    context?: AutomationsContextType;
    children: ReactNode;
}

/**
 * Context provider that loads all automations to power the automations page.
 */
export const AutomationsContextProvider = ({ context, children }: Props) => {
    // Fetch action pipelines to display on the page.
    const { data: actionPipelinesData, refetch } = useListActionPipelinesQuery({
        variables: {
            input: {
                start: 0,
                count: 100,
            },
        },
        fetchPolicy: 'cache-first',
    });

    // Raw Data
    const actionPipelines = useMemo(
        () => (actionPipelinesData?.listActionPipelines?.actionPipelines as ActionPipeline[]) || [],
        [actionPipelinesData],
    );

    // Loading State
    const [isLoading, setIsLoading] = useState(false);

    // Memoized refetch function to avoid re-renders
    const refetchAutomations = useCallback(
        (withTimeout: boolean) => {
            setIsLoading(true);
            setTimeout(
                () => {
                    refetch?.();
                    setIsLoading(false);
                },
                withTimeout ? 3000 : 0,
            );
        },
        [refetch], // Dependencies for useCallback
    );

    const memoizedContext = useMemo(() => context, [context]);

    // Memoize the context value to prevent unnecessary re-renders
    const contextValue = useMemo(
        () => ({
            ...memoizedContext,
            automations: actionPipelines,
            isLoading,
            refetchAutomations,
        }),
        [memoizedContext, actionPipelines, isLoading, refetchAutomations],
    );

    return <AutomationsContext.Provider value={contextValue}>{children}</AutomationsContext.Provider>;
};
