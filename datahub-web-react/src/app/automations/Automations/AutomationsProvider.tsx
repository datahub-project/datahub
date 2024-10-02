import React, { useContext, useState, ReactNode } from 'react';

import { useListActionPipelinesQuery } from '@graphql/actionPipeline.generated';
import { ActionPipeline } from '@src/types.generated';

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
    children?: ReactNode | undefined;
}

export const AutomationsContextProvider = ({ context, children }: Props) => {
    // Fetch action pipelines
    const { data: actionPipelinesData, refetch } = useListActionPipelinesQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
    });

    // Raw Data
    const actionPipelines =
        (actionPipelinesData?.listActionPipelines?.actionPipelines as ActionPipeline[]) || ([] as ActionPipeline[]);

    // Loading State
    const [isLoading, setIsLoading] = useState(false);

    // Refetch Automations
    const refetchAutomations = (withTimeout) => {
        setIsLoading(true);
        setTimeout(
            () => {
                refetch?.();
                setIsLoading(false);
            },
            withTimeout ? 1000 : 0,
        );
    };

    return (
        <AutomationsContext.Provider
            value={{
                ...context,
                automations: actionPipelines,
                isLoading,
                refetchAutomations: (withTimeout) => refetchAutomations(withTimeout),
            }}
        >
            {children}
        </AutomationsContext.Provider>
    );
};
