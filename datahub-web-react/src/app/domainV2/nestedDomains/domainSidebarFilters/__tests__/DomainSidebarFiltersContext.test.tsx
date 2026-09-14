import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it } from 'vitest';

import {
    DomainSidebarFiltersProvider,
    useDomainSidebarFilters,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/DomainSidebarFiltersContext';
import { DomainOwnerInfo } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';
import {
    DEFAULT_DOMAIN_SIDEBAR_SORT,
    DOMAIN_SIDEBAR_SORT,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarSort';

import { EntityType } from '@types';

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <DomainSidebarFiltersProvider>{children}</DomainSidebarFiltersProvider>
);

function makeOwnerInfo(overrides: Partial<DomainOwnerInfo> = {}): DomainOwnerInfo {
    return {
        urn: 'urn:li:corpuser:jane',
        displayName: 'Jane Doe',
        type: EntityType.CorpUser,
        ...overrides,
    };
}

describe('DomainSidebarFiltersContext', () => {
    describe('defaults', () => {
        it('initializes selection, available owners, and sort defaults', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            expect(result.current.selectedOwnerUrns).toEqual([]);
            expect(result.current.availableOwners).toEqual([]);
            expect(result.current.sortSelection).toBe(DEFAULT_DOMAIN_SIDEBAR_SORT);
        });

        it('returns noop defaults when used outside a provider (used by picker variants)', () => {
            const { result } = renderHook(() => useDomainSidebarFilters());

            expect(result.current.selectedOwnerUrns).toEqual([]);
            expect(result.current.availableOwners).toEqual([]);
            expect(result.current.sortSelection).toBe(DEFAULT_DOMAIN_SIDEBAR_SORT);
            expect(() => result.current.setSelectedOwnerUrns(['x'])).not.toThrow();
            expect(() => result.current.setAvailableOwners([makeOwnerInfo()])).not.toThrow();
            expect(() => result.current.setSortSelection(DOMAIN_SIDEBAR_SORT.NAME_DESC)).not.toThrow();
        });
    });

    describe('setSelectedOwnerUrns', () => {
        it('updates the selection without touching the available-owners list', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => result.current.setSelectedOwnerUrns(['urn:li:corpuser:jane']));

            expect(result.current.selectedOwnerUrns).toEqual(['urn:li:corpuser:jane']);
            expect(result.current.availableOwners).toEqual([]);
        });
    });

    describe('setAvailableOwners', () => {
        it('replaces the available-owners list wholesale', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            const first = [makeOwnerInfo({ urn: 'urn:li:corpuser:jane' })];
            const second = [
                makeOwnerInfo({ urn: 'urn:li:corpuser:john', displayName: 'John' }),
                makeOwnerInfo({ urn: 'urn:li:corpGroup:eng', displayName: 'Eng', type: EntityType.CorpGroup }),
            ];

            act(() => result.current.setAvailableOwners(first));
            expect(result.current.availableOwners).toEqual(first);

            act(() => result.current.setAvailableOwners(second));
            expect(result.current.availableOwners).toEqual(second);
        });

        it('does not touch the user selection', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => result.current.setSelectedOwnerUrns(['urn:li:corpuser:jane']));
            act(() => result.current.setAvailableOwners([makeOwnerInfo({ urn: 'urn:li:corpuser:john' })]));

            expect(result.current.selectedOwnerUrns).toEqual(['urn:li:corpuser:jane']);
        });
    });

    describe('sort', () => {
        it('updates sort independently of owner selection', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => result.current.setSortSelection(DOMAIN_SIDEBAR_SORT.NAME_DESC));

            expect(result.current.sortSelection).toBe(DOMAIN_SIDEBAR_SORT.NAME_DESC);
            expect(result.current.selectedOwnerUrns).toEqual([]);
        });
    });
});
