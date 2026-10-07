import { renderHook } from '@testing-library/react-hooks';

import useDomainSearchResults from '@app/domainV2/useDomainSearchResults';

import { EntityType } from '@types';

const mockUseGetAutoCompleteResultsQuery = vi.fn();
const mockUseGetAutoCompleteMultipleResultsQuery = vi.fn();
const mockUseGetDomainsOwnedByQuery = vi.fn();

vi.mock('@graphql/search.generated', () => ({
    useGetAutoCompleteResultsQuery: (...args: unknown[]) => mockUseGetAutoCompleteResultsQuery(...args),
    useGetAutoCompleteMultipleResultsQuery: (...args: unknown[]) => mockUseGetAutoCompleteMultipleResultsQuery(...args),
}));

vi.mock('@graphql/domain.generated', () => ({
    useGetDomainsOwnedByQuery: (...args: unknown[]) => mockUseGetDomainsOwnedByQuery(...args),
}));

const adsDomain = { urn: 'urn:li:domain:ads', type: EntityType.Domain, properties: { name: 'Ads' } };
const adDeliveryDomain = {
    urn: 'urn:li:domain:ad-delivery',
    type: EntityType.Domain,
    properties: { name: 'Ad Delivery' },
    ownership: { owners: [{ owner: { urn: 'urn:li:corpuser:sourabh' } }] },
};
const sourabh = { urn: 'urn:li:corpuser:sourabh', type: EntityType.CorpUser, username: 'sourabh' };

const idle = { data: undefined, loading: false };

describe('useDomainSearchResults', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockUseGetAutoCompleteResultsQuery.mockReturnValue(idle);
        mockUseGetAutoCompleteMultipleResultsQuery.mockReturnValue(idle);
        mockUseGetDomainsOwnedByQuery.mockReturnValue(idle);
    });

    it('skips every query when there is no search text', () => {
        const { result } = renderHook(() => useDomainSearchResults(''));

        expect(result.current).toEqual({ results: [], loading: false });
        expect(mockUseGetAutoCompleteResultsQuery.mock.calls[0][0].skip).toBe(true);
        expect(mockUseGetAutoCompleteMultipleResultsQuery.mock.calls[0][0].skip).toBe(true);
        expect(mockUseGetDomainsOwnedByQuery.mock.calls[0][0].skip).toBe(true);
    });

    it('does not search for owned domains until an owner matches the query', () => {
        mockUseGetAutoCompleteResultsQuery.mockReturnValue({
            data: { autoComplete: { entities: [adsDomain] } },
            loading: false,
        });
        mockUseGetAutoCompleteMultipleResultsQuery.mockReturnValue({
            data: { autoCompleteForMultiple: { suggestions: [] } },
            loading: false,
        });

        const { result } = renderHook(() => useDomainSearchResults('Ads'));

        expect(mockUseGetDomainsOwnedByQuery.mock.calls[0][0].skip).toBe(true);
        expect(result.current.results).toEqual([{ entity: adsDomain }]);
    });

    it('appends domains owned by matching users after name matches and records which owner matched', () => {
        mockUseGetAutoCompleteResultsQuery.mockReturnValue({
            data: { autoComplete: { entities: [adsDomain] } },
            loading: false,
        });
        mockUseGetAutoCompleteMultipleResultsQuery.mockReturnValue({
            data: { autoCompleteForMultiple: { suggestions: [{ type: EntityType.CorpUser, entities: [sourabh] }] } },
            loading: false,
        });
        mockUseGetDomainsOwnedByQuery.mockReturnValue({
            data: { searchAcrossEntities: { searchResults: [{ entity: adDeliveryDomain }, { entity: adsDomain }] } },
            loading: false,
        });

        const { result } = renderHook(() => useDomainSearchResults('Sourabh'));

        const ownedQueryInput = mockUseGetDomainsOwnedByQuery.mock.calls[0][0].variables.input;
        expect(ownedQueryInput.orFilters).toEqual([
            { and: [{ field: 'owners', values: ['urn:li:corpuser:sourabh'] }] },
        ]);
        expect(result.current.results).toEqual([
            { entity: adsDomain },
            { entity: adDeliveryDomain, matchedOwner: sourabh },
        ]);
    });

    it('stays loading until the owned-domain lookup finishes', () => {
        mockUseGetAutoCompleteResultsQuery.mockReturnValue({ data: undefined, loading: false });
        mockUseGetAutoCompleteMultipleResultsQuery.mockReturnValue({
            data: { autoCompleteForMultiple: { suggestions: [{ type: EntityType.CorpUser, entities: [sourabh] }] } },
            loading: false,
        });
        mockUseGetDomainsOwnedByQuery.mockReturnValue({ data: undefined, loading: true });

        const { result } = renderHook(() => useDomainSearchResults('Sourabh'));

        expect(result.current.loading).toBe(true);
    });
});
