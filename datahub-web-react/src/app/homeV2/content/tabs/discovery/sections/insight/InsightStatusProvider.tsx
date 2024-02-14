import React, { createContext, useContext, useEffect, useState } from 'react';

type InsightStatusContextType = {
    insightStatuses: Map<string, boolean>;
    registerInsight: (id: string, isPresent: boolean) => void;
};

const InsightStatusContext = createContext<InsightStatusContextType>({
    insightStatuses: new Map(),
    registerInsight: () => null,
});

export const useInsightStatusContext = () => {
    return useContext(InsightStatusContext);
};

export const useRegisterInsight = (id, isPresent) => {
    // TODO: Determine why this input is changing on each render.
    const { registerInsight } = useInsightStatusContext();

    useEffect(() => {
        const newIsPresent = !!isPresent;
        registerInsight(id, newIsPresent);
        return () => registerInsight(id, false); // Cleanup
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [id, isPresent]);
};

/**
 * Used for insight modules to communicate their status to the parent
 * Insights coordinator module.
 */
export const InsightStatusProvider = ({ children }: { children: React.ReactNode }) => {
    const [insightStatuses, setInsightStatuses] = useState(new Map());

    const registerInsight = (id: string, isPresent: boolean) => {
        const newIsPresent = isPresent;
        const currentIsPresent = insightStatuses.has(id) ? insightStatuses.get(id) : false;
        if (newIsPresent !== currentIsPresent) {
            const newStatuses = new Map(insightStatuses);
            newStatuses.set(id, newIsPresent);
            setInsightStatuses(newStatuses);
        }
    };

    return (
        <InsightStatusContext.Provider value={{ insightStatuses, registerInsight }}>
            {children}
        </InsightStatusContext.Provider>
    );
};
