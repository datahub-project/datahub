import { useCallback, useEffect, useState } from 'react';

import { type ConnectorRegistry, type PluginDetails } from '@app/ingestV2/shared/connectorRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

export const useCapabilitySummary = () => {
    const [capabilitySummary, setCapabilitySummary] = useState<ConnectorRegistry | null>(null);
    const [isLoading, setIsLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchCapabilitySummary = async () => {
            setIsLoading(true);
            setError(null);

            try {
                // Fetch manifest first to get list of package files
                const manifestResponse = await fetch(
                    resolveRuntimePath('/assets/ingestion/connector_registry/manifest.json'),
                );
                if (!manifestResponse.ok) {
                    throw new Error('Connector registry manifest not found');
                }

                const manifest = await manifestResponse.json();
                const packageNames: string[] = manifest.packages || [];

                const mergedData: ConnectorRegistry = {
                    generated_by: 'metadata-ingestion/scripts/connector_registry.py',
                    plugin_details: {},
                };

                // Fetch each package file listed in manifest
                const packagePromises = packageNames.map(async (packageName) => {
                    try {
                        const fileResponse = await fetch(
                            resolveRuntimePath(`/assets/ingestion/connector_registry/${packageName}.json`),
                        );
                        if (fileResponse.ok) {
                            const packageData = await fileResponse.json();
                            const pluginCount = Object.keys(packageData.plugin_details || {}).length;
                            console.info(`Loaded package '${packageName}': ${pluginCount} connectors`);
                            return packageData.plugin_details;
                        }
                        return {};
                    } catch (packageError) {
                        console.warn(`Failed to load package '${packageName}':`, packageError);
                        return {};
                    }
                });

                const packageResults = await Promise.all(packagePromises);
                packageResults.forEach((pluginDetails) => {
                    mergedData.plugin_details = {
                        ...mergedData.plugin_details,
                        ...pluginDetails,
                    };
                });

                console.info(`Total connectors loaded: ${Object.keys(mergedData.plugin_details).length}`);
                setCapabilitySummary(mergedData);
            } catch (fetchError) {
                console.error('Error fetching connector registry:', fetchError);
                setError(fetchError instanceof Error ? fetchError.message : 'Failed to fetch connector registry');
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
