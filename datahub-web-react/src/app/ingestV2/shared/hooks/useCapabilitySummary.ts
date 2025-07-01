import { useEffect, useState } from 'react';

import { type CapabilitySummary, capabilitySummaryManager } from '@app/ingestV2/shared/capabilitySummary';

export const useCapabilitySummary = () => {
    const [capabilitySummary, setCapabilitySummary] = useState<CapabilitySummary | null>(null);
    const [isLoading, setIsLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchCapabilitySummary = async () => {
            await capabilitySummaryManager.fetchCapabilitySummary();
            const state = capabilitySummaryManager.getState();
            setCapabilitySummary(state.capabilitySummary);
            setIsLoading(state.isLoading);
            setError(state.error);
        };

        fetchCapabilitySummary();
    }, []);

    return {
        capabilitySummary,
        isLoading,
        error,
        isCapabilitySupported: capabilitySummaryManager.isCapabilitySupported.bind(capabilitySummaryManager),
        isProfilingSupported: capabilitySummaryManager.isProfilingSupported.bind(capabilitySummaryManager),
        isTestConnectionSupported: capabilitySummaryManager.isTestConnectionSupported.bind(capabilitySummaryManager),
        getConnectorsWithTestConnection:
            capabilitySummaryManager.getConnectorsWithTestConnection.bind(capabilitySummaryManager),
    };
};
