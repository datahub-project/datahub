import { act, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { resetMcpToolCatalogCache, useMcpToolCatalog } from '@app/context/agents/useMcpToolCatalog';
import { resetCachedBasePath } from '@utils/runtimeBasePath';
import { renderHook } from '@utils/test-utils/renderHook';

const CATALOG = { tools: [{ toolId: 'search', description: 'Search', groups: ['read_metadata'] }] };

function jsonResponse(body: unknown, ok = true) {
    return Promise.resolve({ ok, status: ok ? 200 : 503, json: () => Promise.resolve(body) } as Response);
}

describe('useMcpToolCatalog', () => {
    const fetchMock = vi.fn();

    beforeEach(() => {
        resetMcpToolCatalogCache();
        fetchMock.mockReset();
        vi.stubGlobal('fetch', fetchMock);
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        document.querySelector('base')?.remove();
        resetCachedBasePath();
    });

    it('fetches the catalog under the base path with the session cookie and fills in defaults', async () => {
        const base = document.createElement('base');
        base.setAttribute('href', '/datahub/');
        document.head.appendChild(base);
        resetCachedBasePath();
        fetchMock.mockReturnValue(jsonResponse(CATALOG));

        const { result } = renderHook(() => useMcpToolCatalog());

        expect(result.current.loading).toBe(true);
        await waitFor(() => expect(result.current.catalog).toBeDefined());
        expect(fetchMock).toHaveBeenCalledWith(
            '/datahub/ai/catalog/tools',
            expect.objectContaining({ credentials: 'same-origin' }),
        );
        expect(result.current.catalog).toEqual({ ...CATALOG, agents: [] });
        expect(result.current.loading).toBe(false);
    });

    it('reports a failure and loads again on retry', async () => {
        fetchMock.mockReturnValueOnce(jsonResponse({}, false)).mockReturnValueOnce(jsonResponse(CATALOG));
        vi.spyOn(console, 'error').mockImplementation(() => {});

        const { result } = renderHook(() => useMcpToolCatalog());
        await waitFor(() => expect(result.current.failed).toBe(true));
        expect(result.current.catalog).toBeUndefined();

        act(() => result.current.retry());

        await waitFor(() => expect(result.current.catalog?.tools).toHaveLength(1));
        expect(result.current.failed).toBe(false);
    });
});
