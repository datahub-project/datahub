import { useCallback, useEffect, useState } from 'react';

import { type CapabilitySummary, type PluginDetails } from '@app/ingestV2/shared/capabilitySummary';

export const useCapabilitySummary = () => {
    const [capabilitySummary, setCapabilitySummary] = useState<CapabilitySummary | null>(null);
    const [isLoading, setIsLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchCapabilitySummary = async () => {
            setIsLoading(true);
            setError(null);

            try {
                const response = await fetch('/assets/ingestion/capability_summary.json');
                if (!response.ok) {
                    throw new Error(`Failed to fetch capability summary: ${response.status} ${response.statusText}`);
                }
                const data = await response.json();
                setCapabilitySummary(data);
            } catch (fetchError) {
                console.error('Error fetching capability summary:', fetchError);
                setError(fetchError instanceof Error ? fetchError.message : 'Failed to fetch capability summary');
            } finally {
                setIsLoading(false);
            }
        };

        fetchCapabilitySummary();
    }, []);

    const getPluginCapabilities = useCallback(
        (platformId: string): PluginDetails | null => {
            if (!capabilitySummary?.plugin_details?.[platformId]) {
                return null;
            }
            return capabilitySummary.plugin_details[platformId];
        },
        [capabilitySummary],
    );

    const isCapabilitySupported = useCallback(
        (platformId: string, capabilityName: string): boolean => {
            const capabilities = getPluginCapabilities(platformId)?.capabilities;
            if (!capabilities) {
                return false;
            }
            return capabilities?.some((capability) => capability.capability === capabilityName && capability.supported);
        },
        [getPluginCapabilities],
    );

    const isProfilingSupported = useCallback(
        (platformId: string): boolean => {
            return isCapabilitySupported(platformId, 'DATA_PROFILING');
        },
        [isCapabilitySupported],
    );

    const isTestConnectionSupported = useCallback(
        (platformId: string): boolean => {
            return isCapabilitySupported(platformId, 'TEST_CONNECTION');
        },
        [isCapabilitySupported],
    );

    const getConnectorsWithTestConnection = useCallback((): Set<string> => {
        if (!capabilitySummary?.plugin_details) {
            return new Set();
        }

        const connectorsWithTestConnection = new Set<string>();

        Object.keys(capabilitySummary.plugin_details).forEach((platformId) => {
            if (isTestConnectionSupported(platformId)) {
                connectorsWithTestConnection.add(platformId);
            }
        });

        return connectorsWithTestConnection;
    }, [capabilitySummary, isTestConnectionSupported]);

    return {
        capabilitySummary,
        isLoading,
        error,
        isCapabilitySupported,
        isProfilingSupported,
        isTestConnectionSupported,
        getConnectorsWithTestConnection,
    };
};
