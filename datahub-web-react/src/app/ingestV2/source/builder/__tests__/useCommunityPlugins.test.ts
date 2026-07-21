import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useCommunityPlugins } from '@app/ingestV2/source/builder/useCommunityPlugins';

function mockRegistry(payload: unknown) {
    vi.stubGlobal(
        'fetch',
        vi.fn().mockResolvedValue({
            ok: true,
            json: async () => payload,
        }),
    );
}

describe('useCommunityPlugins', () => {
    beforeEach(() => {
        // The hook logs a warning on fetch failure; keep test output clean.
        vi.spyOn(console, 'warn').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    it('maps a GitHub-hosted source plugin to a SourceConfig and a github install spec', async () => {
        mockRegistry([
            {
                id: 'my-test-source',
                repo: 'owner/my-test-source',
                version: 'v0.1.0',
                type: 'source',
                description: 'A test source',
                icon_url: 'https://example.com/icon.png',
            },
        ]);

        const { result, waitFor } = renderHook(() => useCommunityPlugins());
        await waitFor(() => expect(result.current.communityPlugins).toHaveLength(1));

        const [plugin] = result.current.communityPlugins;
        expect(plugin.name).toBe('my-test-source');
        // display_name absent → derived from the id (kebab-case → Title Case).
        expect(plugin.displayName).toBe('My Test Source');
        expect(plugin.supportStatus).toBe('COMMUNITY');
        expect(plugin.logoUrl).toBe('https://example.com/icon.png');

        const meta = result.current.communityPluginMeta['my-test-source'];
        expect(meta.installSpec).toBe('github:owner/my-test-source@v0.1.0');
        expect(meta.sourceUrl).toBe('https://github.com/owner/my-test-source');
    });

    it('builds a pypi install spec when package_name is present', async () => {
        mockRegistry({
            plugins: [{ id: 'pypi-src', repo: 'owner/pypi-src', version: '1.2.3', package_name: 'acme-src' }],
        });

        const { result, waitFor } = renderHook(() => useCommunityPlugins());
        await waitFor(() => expect(result.current.communityPlugins).toHaveLength(1));

        expect(result.current.communityPluginMeta['pypi-src'].installSpec).toBe('pypi:acme-src==1.2.3');
    });

    it('excludes non-source plugin types', async () => {
        mockRegistry([
            { id: 'a-source', repo: 'o/a', version: '1' },
            { id: 'a-sink', repo: 'o/b', version: '1', type: 'sink' },
        ]);

        const { result, waitFor } = renderHook(() => useCommunityPlugins());
        await waitFor(() => expect(result.current.isLoading).toBe(false));

        expect(result.current.communityPlugins.map((p) => p.name)).toEqual(['a-source']);
    });

    it('surfaces an error and returns no plugins when the registry fetch fails', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 500 }));

        const { result, waitFor } = renderHook(() => useCommunityPlugins());
        await waitFor(() => expect(result.current.error).toBeTruthy());

        expect(result.current.communityPlugins).toHaveLength(0);
    });

    it('carries capabilities, support status, and trust tier from the unified schema', async () => {
        mockRegistry({
            plugins: [
                {
                    id: 'cap-source',
                    repo: 'owner/cap-source',
                    version: '1.0.0',
                    type: 'source',
                    support_status: 'CERTIFIED',
                    trust_tier: 'verified',
                    capabilities: [{ capability: 'LINEAGE_COARSE', description: 'Table lineage', supported: true }],
                },
            ],
        });

        const { result, waitFor } = renderHook(() => useCommunityPlugins());
        await waitFor(() => expect(result.current.communityPlugins).toHaveLength(1));

        expect(result.current.communityPlugins[0].supportStatus).toBe('CERTIFIED');
        const meta = result.current.communityPluginMeta['cap-source'];
        expect(meta.trustTier).toBe('verified');
        expect(meta.supportStatus).toBe('CERTIFIED');
        expect(meta.capabilities).toHaveLength(1);
        expect(meta.capabilities?.[0].capability).toBe('LINEAGE_COARSE');
    });
});
