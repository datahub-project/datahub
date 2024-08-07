import React, { useContext, useEffect, useState, ReactNode } from 'react';

import { useListTestsQuery } from '@graphql/test.generated';
import { useListActionPipelinesQuery } from '@graphql/actionPipeline.generated';

import { ListAutomationItem } from '@app/automations/types';
import { env } from '@app/automations/constants';
import { simplifyDataForListView } from '@app/automations/utils';

export interface AutomationsContextType {
    automations: ListAutomationItem[];
    isLoading: boolean;
    refetchAutomations?: () => void;
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
    const { hideMetadataTests } = env;

    // All Automations
    const [allAutomations, setAllAutomations] = useState<ListAutomationItem[]>([]);

    // Fetch metadata tests
    const {
        data: metadataTestsData,
        loading: testLoading,
        refetch: refetchTests,
    } = useListTestsQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
        fetchPolicy: 'no-cache',
        skip: hideMetadataTests,
    });

    // Fetch action pipelines
    const {
        data: actionPipelinesData,
        loading: actionLoading,
        refetch: refetchActions,
    } = useListActionPipelinesQuery({
        variables: {
            input: {
                start: 0,
                count: 10,
            },
        },
        fetchPolicy: 'no-cache',
    });

    // Raw Data
    const metadataTests = metadataTestsData?.listTests?.tests || [];
    const actionPipelines = actionPipelinesData?.listActionPipelines?.actionPipelines || [];

    // Simplify Data for List View
    const simplifiedMetadataTests = simplifyDataForListView(metadataTests);
    const simplifiedActionPipelines = simplifyDataForListView(actionPipelines);

    // All Automations
    useEffect(() => {
        const dataList: ListAutomationItem[] = [...simplifiedActionPipelines, ...simplifiedMetadataTests];
        if (dataList.length !== allAutomations.length) setAllAutomations(dataList);
    }, [simplifiedActionPipelines, simplifiedMetadataTests, allAutomations, setAllAutomations]);

    // Refetch Automations
    const refetchAutomations = () => {
        refetchTests();
        refetchActions();
    };

    // Loading
    const isLoading = testLoading || actionLoading;

    return (
        <AutomationsContext.Provider
            value={{
                ...context,
                automations: allAutomations,
                isLoading,
                refetchAutomations,
            }}
        >
            {children}
        </AutomationsContext.Provider>
    );
};
