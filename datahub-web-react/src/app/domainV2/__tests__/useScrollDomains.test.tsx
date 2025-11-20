import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { DomainsContext } from '@app/domainV2/DomainsContext';
import useScrollDomains from '@app/domainV2/useScrollDomains';

// Simple mock implementations
const mockUseInView = vi.fn();
const mockUseScrollAcrossEntitiesQuery = vi.fn();
const mockUseManageDomains = vi.fn();

// Mock modules with simple return values
vi.mock('react-intersection-observer', () => ({
    useInView: () => mockUseInView(),
}));

vi.mock('@graphql/search.generated', () => ({
    useScrollAcrossEntitiesQuery: () => mockUseScrollAcrossEntitiesQuery(),
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

describe('useScrollDomains - Simplified Tests', () => {
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

    it('should call useManageDomains hook', () => {
        renderHook(() => useScrollDomains({}), {
            wrapper: createWrapper,
        });

        expect(mockUseManageDomains).toHaveBeenCalled();
    });
});
