import { waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import { renderHook } from '@utils/test-utils/renderHook';

function mockRegistry(pluginDetails: Record<string, unknown>) {
    vi.stubGlobal(
        'fetch',
        vi.fn(async (url: string) => ({
            ok: true,
            json: async () =>
                url.endsWith('manifest.json') ? { packages: ['datahub'] } : { plugin_details: pluginDetails },
        })),
    );
}

describe('useCapabilitySummary', () => {
    afterEach(() => {
        vi.unstubAllGlobals();
    });

    // The Databricks tile's source type is an alias of unity-catalog, which is how the registry lists it.
    it('reports Test Connection for a source type that is an alias of a supporting connector', async () => {
        mockRegistry({
            'unity-catalog': { capabilities: [{ capability: 'TEST_CONNECTION', supported: true }] },
        });

        const { result } = renderHook(() => useCapabilitySummary());
        await waitFor(() => expect(result.current.capabilitySummary).not.toBeNull());

        expect(result.current.getConnectorsWithTestConnection().has('databricks')).toBe(true);
    });
});
