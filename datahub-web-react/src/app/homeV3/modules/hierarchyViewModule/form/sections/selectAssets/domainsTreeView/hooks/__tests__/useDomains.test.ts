import { renderHook } from '@testing-library/react-hooks';

import useDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomains';
import { DomainItem } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomainsNodes';

import { useListDomainsQuery } from '@graphql/domain.generated';
import { EntityType } from '@types';

vi.mock('@graphql/domain.generated', () => ({
    useListDomainsQuery: vi.fn(),
}));

describe('useDomains', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('calls query without parentDomain when not provided', () => {
        vi.mocked(useListDomainsQuery).mockReturnValue({ data: null, loading: false } as any);
        renderHook(() => useDomains());
        expect(useListDomainsQuery).toHaveBeenCalledWith({
            variables: {
                input: {
                    start: 0,
                    count: 1000,
                    parentDomain: undefined,
                },
            },
        });
    });

    it('passes parentDomain to the query', () => {
        vi.mocked(useListDomainsQuery).mockReturnValue({ data: null, loading: false } as any);
        renderHook(() => useDomains('parent1'));
        expect(useListDomainsQuery).toHaveBeenCalledWith({
            variables: {
                input: {
                    start: 0,
                    count: 1000,
                    parentDomain: 'parent1',
                },
            },
        });
    });

    it('returns domains from data', () => {
        const domainsData: DomainItem[] = [
            { urn: 'domain1', id: 'domain1', type: EntityType.Domain },
            { urn: 'domain2', id: 'domain2', type: EntityType.Domain },
        ];
        vi.mocked(useListDomainsQuery).mockReturnValue({
            data: { listDomains: { start: 0, total: 2, count: 2, domains: domainsData } },
            loading: false,
        } as any);

        const { result } = renderHook(() => useDomains());
        expect(result.current.domains).toEqual(domainsData);
    });

    it('returns domains as undefined when data is undefined', () => {
        vi.mocked(useListDomainsQuery).mockReturnValue({ data: undefined, loading: false } as any);
        const { result } = renderHook(() => useDomains());
        expect(result.current.domains).toBeUndefined();
    });

    it('returns empty array when listDomains is missing', () => {
        vi.mocked(useListDomainsQuery).mockReturnValue({ data: {}, loading: false } as any);
        const { result } = renderHook(() => useDomains());
        expect(result.current.domains).toEqual([]);
    });

    it('returns loading state', () => {
        vi.mocked(useListDomainsQuery).mockReturnValue({ data: {}, loading: true } as any);
        const { result } = renderHook(() => useDomains());
        expect(result.current.loading).toBe(true);
    });

    it('memoizes domains when data does not change', () => {
        const domainsData = [{ urn: 'domain1' }];
        const mockData = { listDomains: { domains: domainsData } };
        vi.mocked(useListDomainsQuery).mockReturnValue({ data: mockData, loading: false } as any);

        const { result, rerender } = renderHook(() => useDomains());
        const firstDomains = result.current.domains;
        rerender();

        expect(result.current.domains).toBe(firstDomains);
    });
});
