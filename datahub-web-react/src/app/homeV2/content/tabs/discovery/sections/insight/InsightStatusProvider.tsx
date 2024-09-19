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
        if (isPresent !== undefined) {
            // Only register the insight once it's a true or false value, not during loading.
            registerInsight(id, isPresent);
        }
        return () => registerInsight(id, false); // Cleanup
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [id, isPresent]);
};

/**
 * Used for insight modules to communicate their status to the parent
 * Insights coordinator module.
 */
export const InsightStatusProvider = ({
    children,
    displayedInsightIds,
}: {
    children: React.ReactNode;
    displayedInsightIds: string[];
}) => {
    const [insightStatuses, setInsightStatuses] = useState(new Map());
    const [displayInsights, setDisplayInsights] = useState(true);

    const registerInsight = (id: string, isPresent: boolean) => {
        setInsightStatuses((prevStatuses) => {
            const currentIsPresent = prevStatuses.get(id);
            if (!prevStatuses.has(id) || isPresent !== currentIsPresent) {
                const newStatuses = new Map(prevStatuses);
                newStatuses.set(id, isPresent);
                return newStatuses;
            }
            return prevStatuses;
        });
    };

    useEffect(() => {
        // If all insights have been registerd, and all are hidden, we can remove un-render the entire for you section.
        const allInsightsHidden = displayedInsightIds.every(
            (id) => insightStatuses.has(id) && !insightStatuses.get(id),
        );
        if (allInsightsHidden) {
            setDisplayInsights(false);
        }
    }, [insightStatuses, displayedInsightIds, setDisplayInsights]);

    return (
        <InsightStatusContext.Provider value={{ insightStatuses, registerInsight }}>
            {displayInsights ? children : undefined}
        </InsightStatusContext.Provider>
    );
};
