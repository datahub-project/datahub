import { renderHook } from '@testing-library/react-hooks';

import useGetEntityByUrl from '@app/embed/lookup/useGetEntityByUrl';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType, FilterOperator } from '@types';

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({ getPathName: () => 'dashboard' }),
}));

const REPORT_ID = '22222222-2222-2222-2222-222222222222';
const APP_URL = `https://app.powerbi.com/groups/me/apps/33333333-3333-3333-3333-333333333333/reports/${REPORT_ID}/44444444444444444444444444444444?experience=power-bi`;
const WORKSPACE_URL = `https://app.powerbi.com/groups/11111111-1111-1111-1111-111111111111/reports/${REPORT_ID}`;

const mockQuery = useGetSearchResultsForMultipleQuery as unknown as ReturnType<typeof vi.fn>;

type QueryOptions = { skip?: boolean; variables: { input: { orFilters: { and: { condition: FilterOperator }[] }[] } } };

function searchResponse(urns: string[]) {
    return {
        data: {
            searchAcrossEntities: {
                searchResults: urns.map((urn) => ({ entity: { urn, type: EntityType.Dashboard } })),
            },
        },
        error: undefined,
    };
}

/** Responds based on the filter condition so call ordering is not baked into assertions. */
function mockSearch({ equal, contain }: { equal: string[]; contain: string[] }) {
    const calls: QueryOptions[] = [];

    mockQuery.mockImplementation((options: QueryOptions) => {
        calls.push(options);
        if (options.skip) return { data: undefined, error: undefined };

        const isContain = options.variables.input.orFilters.some((orFilter) =>
            orFilter.and.some((filter) => filter.condition === FilterOperator.Contain),
        );
        return searchResponse(isContain ? contain : equal);
    });

    return {
        containCall: () =>
            calls.find((call) =>
                call.variables.input.orFilters.some((orFilter) =>
                    orFilter.and.some((filter) => filter.condition === FilterOperator.Contain),
                ),
            ),
    };
}

describe('useGetEntityByUrl', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('resolves an exact URL match without running a substring query', () => {
        const { containCall } = mockSearch({ equal: ['urn:li:dashboard:(powerbi,reports.a)'], contain: [] });

        const { result } = renderHook(() => useGetEntityByUrl(WORKSPACE_URL));

        expect(result.current.embedUrl).toContain('reports.a');
        expect(result.current.notFound).toBe(false);
        expect(containCall()?.skip).toBe(true);
    });

    it('keeps an exact match ambiguous rather than widening it with a substring query', () => {
        const { containCall } = mockSearch({
            equal: ['urn:li:dashboard:(powerbi,reports.a)', 'urn:li:chart:(powerbi,tiles.b)'],
            contain: [],
        });

        const { result } = renderHook(() => useGetEntityByUrl(WORKSPACE_URL));

        expect(result.current.foundMultiple).toBe(true);
        expect(containCall()?.skip).toBe(true);
    });

    it('falls back to a substring match for Workspace App URLs that cannot match exactly', () => {
        const { containCall } = mockSearch({ equal: [], contain: ['urn:li:dashboard:(powerbi,reports.a)'] });

        const { result } = renderHook(() => useGetEntityByUrl(APP_URL));

        expect(containCall()?.skip).toBe(false);
        expect(result.current.embedUrl).toContain('reports.a');
        expect(result.current.notFound).toBe(false);
    });

    it('reports not found when neither match finds an entity', () => {
        mockSearch({ equal: [], contain: [] });

        const { result } = renderHook(() => useGetEntityByUrl(APP_URL));

        expect(result.current.notFound).toBe(true);
    });

    it('does not attempt a substring match for non-Power BI URLs', () => {
        const { containCall } = mockSearch({ equal: [], contain: [] });

        const { result } = renderHook(() => useGetEntityByUrl('https://example.com/dashboards/123'));

        expect(containCall()?.skip).toBe(true);
        expect(result.current.notFound).toBe(true);
    });
});
