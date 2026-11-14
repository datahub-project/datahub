import { waitFor } from '@testing-library/react';

import type * as HookModule from '@app/sharedV2/githubStar/useGithubStarCount';
import { renderHook } from '@utils/test-utils/renderHook';

function mockFetchResponse(body: unknown, ok = true) {
    const fetchMock = vi.fn(() => Promise.resolve({ ok, json: () => Promise.resolve(body) } as Response));
    vi.stubGlobal('fetch', fetchMock);
    return fetchMock;
}

// The hook shares one request per page load, so each test gets a fresh copy of the module.
async function loadHook(): Promise<typeof HookModule> {
    vi.resetModules();
    return import('@app/sharedV2/githubStar/useGithubStarCount');
}

describe('useGithubStarCount', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('fetches the star count and caches it', async () => {
        const { default: useGithubStarCount, GITHUB_STAR_COUNT_CACHE_KEY } = await loadHook();
        const fetchMock = mockFetchResponse({ stargazers_count: 11234 });

        const { result } = renderHook(() => useGithubStarCount(true));

        await waitFor(() => expect(result.current).toBe(11234));
        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(JSON.parse(localStorage.getItem(GITHUB_STAR_COUNT_CACHE_KEY) ?? '{}').count).toBe(11234);
    });

    it('shares one request between callers on the same page', async () => {
        const { default: useGithubStarCount } = await loadHook();
        const fetchMock = mockFetchResponse({ stargazers_count: 11234 });

        const first = renderHook(() => useGithubStarCount(true));
        const second = renderHook(() => useGithubStarCount(true));

        await waitFor(() => expect(first.result.current).toBe(11234));
        await waitFor(() => expect(second.result.current).toBe(11234));
        expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('uses a fresh cached count without calling GitHub', async () => {
        const { default: useGithubStarCount, GITHUB_STAR_COUNT_CACHE_KEY } = await loadHook();
        localStorage.setItem(GITHUB_STAR_COUNT_CACHE_KEY, JSON.stringify({ count: 42, fetchedAt: Date.now() }));
        const fetchMock = mockFetchResponse({ stargazers_count: 99 });

        const { result } = renderHook(() => useGithubStarCount(true));

        expect(result.current).toBe(42);
        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('refetches once the cached count is stale', async () => {
        const {
            default: useGithubStarCount,
            GITHUB_STAR_COUNT_CACHE_KEY,
            GITHUB_STAR_COUNT_CACHE_TTL_MS,
        } = await loadHook();
        localStorage.setItem(
            GITHUB_STAR_COUNT_CACHE_KEY,
            JSON.stringify({ count: 42, fetchedAt: Date.now() - GITHUB_STAR_COUNT_CACHE_TTL_MS - 1 }),
        );
        mockFetchResponse({ stargazers_count: 99 });

        const { result } = renderHook(() => useGithubStarCount(true));

        await waitFor(() => expect(result.current).toBe(99));
    });

    it('stays null and does not retry on the same page when GitHub is unreachable', async () => {
        const { default: useGithubStarCount } = await loadHook();
        const fetchMock = vi.fn(() => Promise.reject(new Error('blocked')));
        vi.stubGlobal('fetch', fetchMock);

        const first = renderHook(() => useGithubStarCount(true));
        await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
        first.unmount();
        const second = renderHook(() => useGithubStarCount(true));

        await waitFor(() => expect(second.result.current).toBeNull());
        expect(fetchMock).toHaveBeenCalledTimes(1);
    });

    it('does not call GitHub when disabled', async () => {
        const { default: useGithubStarCount } = await loadHook();
        const fetchMock = mockFetchResponse({ stargazers_count: 99 });

        const { result } = renderHook(() => useGithubStarCount(false));

        expect(result.current).toBeNull();
        expect(fetchMock).not.toHaveBeenCalled();
    });
});
