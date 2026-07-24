import { useEffect, useState } from 'react';

import type { Capability } from '@app/ingestV2/shared/connectorRegistry';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

// Mirrors the backend PluginIndexEntry (metadata-ingestion registry_client.py):
// one unified schema shared by the CLI and the UI.
interface RegistryPluginEntry {
    id: string;
    repo: string;
    version: string;
    type?: string;
    description?: string;
    author?: string;
    display_name?: string;
    icon_url?: string;
    recipe_template?: string;
    support_status?: string;
    capabilities?: Capability[];
    source_url?: string;
    package_name?: string;
    sha256?: string; // integrity hash (verified CLI-side at install; not used in the browser)
    trust_tier?: string; // community | verified | official
}

interface CommunityPluginMeta {
    installSpec: string;
    version: string;
    iconUrl?: string;
    recipeTemplate?: string;
    capabilities?: Capability[];
    supportStatus?: string;
    sourceUrl?: string;
    trustTier?: string;
    sha256?: string; // forwarded to the executor so the wheel is checksum-verified before install
}

/**
 * Fetches the community plugin registry index and transforms entries
 * into SourceConfig objects for display in the connector grid.
 *
 * Community plugins are identified by `isCommunity: true` on the SourceConfig.
 */
export function useCommunityPlugins() {
    const [communityPlugins, setCommunityPlugins] = useState<SourceConfig[]>([]);
    const [communityPluginMeta, setCommunityPluginMeta] = useState<Record<string, CommunityPluginMeta>>({});
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let cancelled = false;

        const fetchPlugins = async () => {
            setIsLoading(true);
            setError(null);

            try {
                const resp = await fetch(resolveRuntimePath('/assets/ingestion/community_plugins.json'));
                if (!resp.ok) {
                    throw new Error(`Registry fetch failed: ${resp.status}`);
                }
                const data = await resp.json();

                const entries: RegistryPluginEntry[] = Array.isArray(data) ? data : data.plugins || [];

                // Only include source-type plugins
                const sourceEntries = entries.filter((e) => !e.type || e.type === 'source');

                const sources: SourceConfig[] = [];
                const meta: Record<string, CommunityPluginMeta> = {};

                sourceEntries.forEach((entry) => {
                    const displayName =
                        entry.display_name || entry.id.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());

                    // Support both GitHub repos and PyPI packages
                    const installSpec = entry.package_name
                        ? `pypi:${entry.package_name}==${entry.version}`
                        : `github:${entry.repo}@${entry.version}`;

                    const sourceUrl = entry.source_url || `https://github.com/${entry.repo}`;

                    // Build a minimal recipe from template or skeleton
                    const recipe = entry.recipe_template || `source:\n  type: ${entry.id}\n  config: {}\n`;

                    sources.push({
                        urn: `urn:li:dataPlatform:${entry.id}`,
                        name: entry.id,
                        displayName,
                        docsUrl: sourceUrl,
                        description: entry.description || `Community connector: ${displayName}`,
                        recipe,
                        supportStatus: entry.support_status || 'COMMUNITY',
                        logoUrl: entry.icon_url,
                    });

                    meta[entry.id] = {
                        installSpec,
                        version: entry.version,
                        iconUrl: entry.icon_url,
                        recipeTemplate: entry.recipe_template,
                        capabilities: entry.capabilities,
                        supportStatus: entry.support_status,
                        sourceUrl,
                        trustTier: entry.trust_tier,
                        sha256: entry.sha256,
                    };
                });

                if (!cancelled) {
                    setCommunityPlugins(sources);
                    setCommunityPluginMeta(meta);
                }
            } catch (e) {
                if (!cancelled) {
                    const message = e instanceof Error ? e.message : 'Failed to fetch community plugins';
                    setError(message);
                    console.warn('Community plugin registry fetch failed:', message);
                }
            } finally {
                if (!cancelled) {
                    setIsLoading(false);
                }
            }
        };

        fetchPlugins();
        return () => {
            cancelled = true;
        };
    }, []);

    return { communityPlugins, communityPluginMeta, isLoading, error };
}

export type { CommunityPluginMeta };
