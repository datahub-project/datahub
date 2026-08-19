import { renderHook } from '@testing-library/react-hooks';
import { afterEach, describe, expect, it, vi } from 'vitest';

// Imported after the mocks are declared (vitest hoists vi.mock regardless).
// eslint-disable-next-line import/first
import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';

// vi.mock is hoisted above imports, so a shared mock fn must come from vi.hoisted.
const { mockUseCommunityPlugins } = vi.hoisted(() => ({ mockUseCommunityPlugins: vi.fn() }));

// Deterministic built-in list so the merge behaviour is easy to assert.
vi.mock('@app/ingestV2/source/builder/sources.json', () => ({
    default: [
        { urn: 'urn:snowflake', name: 'snowflake', displayName: 'Snowflake', docsUrl: '', recipe: '' },
        { urn: 'urn:mysql', name: 'mysql', displayName: 'MySQL', docsUrl: '', recipe: '' },
    ],
}));

vi.mock('@app/ingestV2/source/builder/useCommunityPlugins', () => ({
    useCommunityPlugins: () => mockUseCommunityPlugins(),
}));

describe('useIngestionSources — community plugin merge', () => {
    afterEach(() => vi.clearAllMocks());

    it('appends unique community plugins and lets built-in sources win on a name conflict', () => {
        mockUseCommunityPlugins.mockReturnValue({
            communityPlugins: [
                // Same name as a built-in → dropped in favour of the built-in.
                {
                    urn: 'urn:c-snowflake',
                    name: 'snowflake',
                    displayName: 'Snowflake (community)',
                    docsUrl: '',
                    recipe: '',
                    supportStatus: 'COMMUNITY',
                },
                // Unique → appended after the built-ins.
                {
                    urn: 'urn:my-source',
                    name: 'my-source',
                    displayName: 'My Source',
                    docsUrl: '',
                    recipe: '',
                    supportStatus: 'COMMUNITY',
                },
            ],
            communityPluginMeta: { 'my-source': { installSpec: 'github:o/my-source@v1', version: 'v1' } },
            isLoading: false,
            error: null,
        });

        const { result } = renderHook(() => useIngestionSources());
        const sources = result.current.ingestionSources;

        expect(sources.map((s) => s.name)).toEqual(['snowflake', 'mysql', 'my-source']);
        // The built-in Snowflake is retained, not the community override.
        expect(sources.find((s) => s.name === 'snowflake')?.displayName).toBe('Snowflake');
        expect(result.current.communityPluginMeta['my-source'].installSpec).toBe('github:o/my-source@v1');
    });
});
