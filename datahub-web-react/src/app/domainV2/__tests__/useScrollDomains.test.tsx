import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { DomainsContext } from '@app/domainV2/DomainsContext';
import useScrollDomains, { DOMAIN_COUNT } from '@app/domainV2/useScrollDomains';

import { ListDomainFragment } from '@graphql/domain.generated';
import { EntityType } from '@types';

type InViewOptions = {
    triggerOnce: boolean;
    rootMargin: string;
};

type ScrollQueryOptions = {
    variables: {
        input: {
            scrollId: string | null;
        };
    };
};

// Simple mock implementations
const mockUseInView = vi.fn<(options: InViewOptions) => [ReturnType<typeof vi.fn>, boolean]>();
const mockUseScrollAcrossEntitiesQuery = vi.fn<(options: ScrollQueryOptions) => unknown>();
const mockUseManageDomains = vi.fn();

// Mock modules with simple return values
vi.mock('react-intersection-observer', () => ({
    useInView: (options: InViewOptions) => mockUseInView(options),
}));

vi.mock('@graphql/search.generated', () => ({
    useScrollAcrossEntitiesQuery: (options: ScrollQueryOptions) => mockUseScrollAcrossEntitiesQuery(options),
}));

vi.mock('@app/domainV2/useManageDomains', () => ({
    default: () => mockUseManageDomains(),
}));

const createWrapper = ({ children }: { children: React.ReactNode }) => (
    <MockedProvider mocks={[]} addTypename={false}>
        <DomainsContext.Provider
            value={{
                entityData: null,
                setEntityData: vi.fn(),
                newDomain: null,
                setNewDomain: vi.fn(),
                deletedDomain: null,
                setDeletedDomain: vi.fn(),
                updatedDomain: null,
                setUpdatedDomain: vi.fn(),
            }}
        >
            {children}
        </DomainsContext.Provider>
    </MockedProvider>
);

const createMockDomain = (index: number): ListDomainFragment =>
    ({
        urn: `urn:li:domain:${index}`,
        id: `${index}`,
        type: EntityType.Domain,
        properties: {
            name: `Domain ${index}`,
            description: null,
            customProperties: [],
        },
        parentDomains: null,
        ownership: null,
        privileges: null,
        displayProperties: null,
        deprecation: null,
        entities: null,
        dataProducts: null,
        applicationsInDomain: null,
        children: null,
        institutionalMemory: null,
    });

const createScrollData = (domains: ListDomainFragment[], nextScrollId: string | null = null) => ({
    scrollAcrossEntities: {
        searchResults: domains.map((entity) => ({ entity })),
        nextScrollId,
    },
});

describe('useScrollDomains', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Set up default mock returns
        mockUseInView.mockReturnValue([vi.fn(), false]);
        mockUseScrollAcrossEntitiesQuery.mockReturnValue({
            data: null,
            loading: false,
            error: null,
            refetch: vi.fn(),
        });
        mockUseManageDomains.mockReturnValue(undefined);
    });

    it('should initialize with empty data', () => {
        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        expect(result.current.domains).toEqual([]);
        expect(result.current.hasInitialized).toBe(false);
        expect(result.current.loading).toBe(false);
        expect(result.current.error).toBe(null);
    });

    it('should return scrollRef function', () => {
        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        expect(result.current.scrollRef).toBeDefined();
        expect(typeof result.current.scrollRef).toBe('function');
        expect(mockUseInView).toHaveBeenCalledWith({ triggerOnce: false, rootMargin: '200px' });
    });

    it('should handle loading state', () => {
        mockUseScrollAcrossEntitiesQuery.mockReturnValue({
            data: null,
            loading: true,
            error: null,
            refetch: vi.fn(),
        });

        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        expect(result.current.loading).toBe(true);
    });

    it('should handle error state', () => {
        const mockError = new Error('Test error');
        mockUseScrollAcrossEntitiesQuery.mockReturnValue({
            data: null,
            loading: false,
            error: mockError,
            refetch: vi.fn(),
        });

        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        expect(result.current.error).toBe(mockError);
    });

    it('should append the next page when the sentinel is in view', async () => {
        const firstPage = Array.from({ length: DOMAIN_COUNT }, (_, index) => createMockDomain(index));
        const nextDomain = createMockDomain(DOMAIN_COUNT);
        const secondPage = [nextDomain];
        mockUseInView.mockReturnValue([vi.fn(), true]);
        mockUseScrollAcrossEntitiesQuery.mockImplementation((options: ScrollQueryOptions) => {
            const isSecondPage = options.variables.input.scrollId === 'second-page';
            return {
                data: isSecondPage ? createScrollData(secondPage) : createScrollData(firstPage, 'second-page'),
                loading: false,
                error: null,
                refetch: vi.fn(),
            };
        });

        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        await waitFor(() => expect(result.current.domains).toHaveLength(DOMAIN_COUNT + 1));
        expect(result.current.domains.map((domain) => domain.urn)).toContain(nextDomain.urn);
        expect(
            mockUseScrollAcrossEntitiesQuery.mock.calls.some(
                ([options]) => options.variables.input.scrollId === 'second-page',
            ),
        ).toBe(true);
    });

    it('should initialize a short result set without requesting another page', async () => {
        mockUseInView.mockReturnValue([vi.fn(), true]);
        mockUseScrollAcrossEntitiesQuery.mockReturnValue({
            data: createScrollData([createMockDomain(1), createMockDomain(2)]),
            loading: false,
            error: null,
            refetch: vi.fn(),
        });

        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        await waitFor(() => expect(result.current.hasInitialized).toBe(true));
        expect(result.current.domains).toHaveLength(2);
        expect(
            mockUseScrollAcrossEntitiesQuery.mock.calls.every(
                ([options]) => options.variables.input.scrollId === null,
            ),
        ).toBe(true);
    });

    it('should dedupe domains repeated across scroll pages', async () => {
        const firstPage = Array.from({ length: DOMAIN_COUNT }, (_, index) => createMockDomain(index));
        const repeatedDomain = createMockDomain(0);
        const secondPage = [repeatedDomain, createMockDomain(DOMAIN_COUNT)];
        mockUseInView.mockReturnValue([vi.fn(), true]);
        mockUseScrollAcrossEntitiesQuery.mockImplementation((options: ScrollQueryOptions) => {
            const isSecondPage = options.variables.input.scrollId === 'second-page';
            return {
                data: isSecondPage ? createScrollData(secondPage) : createScrollData(firstPage, 'second-page'),
                loading: false,
                error: null,
                refetch: vi.fn(),
            };
        });

        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        await waitFor(() => expect(result.current.domains).toHaveLength(DOMAIN_COUNT + 1));
        expect(result.current.domains.filter((domain) => domain.urn === repeatedDomain.urn)).toHaveLength(1);
    });

    it('should surface query errors without advancing pagination', async () => {
        const mockError = new Error('Test error');
        mockUseInView.mockReturnValue([vi.fn(), true]);
        mockUseScrollAcrossEntitiesQuery.mockReturnValue({
            data: null,
            loading: false,
            error: mockError,
            refetch: vi.fn(),
        });

        const { result } = renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        await waitFor(() => expect(result.current.error).toBe(mockError));
        expect(
            mockUseScrollAcrossEntitiesQuery.mock.calls.every(
                ([options]) => options.variables.input.scrollId === null,
            ),
        ).toBe(true);
    });

    it('should call useManageDomains hook', () => {
        renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        expect(mockUseManageDomains).toHaveBeenCalled();
    });
});
