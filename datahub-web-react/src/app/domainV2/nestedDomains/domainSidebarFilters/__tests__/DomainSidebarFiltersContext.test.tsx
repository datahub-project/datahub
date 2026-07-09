import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it } from 'vitest';

import {
    DomainSidebarFiltersProvider,
    useDomainSidebarFilters,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/DomainSidebarFiltersContext';
import { DomainOwnerInfo } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';

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
        it('initializes both selection and available owners to empty', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            expect(result.current.selectedOwnerUrns).toEqual([]);
            expect(result.current.availableOwners).toEqual([]);
        });

        it('returns noop defaults when used outside a provider (used by picker variants)', () => {
            // Important: DomainNode is rendered inside pickers without a
            // provider; calling useDomainSidebarFilters() there must NOT throw.
            const { result } = renderHook(() => useDomainSidebarFilters());

            expect(result.current.selectedOwnerUrns).toEqual([]);
            expect(result.current.availableOwners).toEqual([]);
            expect(() => result.current.setSelectedOwnerUrns(['x'])).not.toThrow();
            expect(() => result.current.setAvailableOwners([makeOwnerInfo()])).not.toThrow();
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
        it('replaces the available-owners list wholesale (mirrors how facets re-arrive on every server response)', () => {
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

            // Selection survives even when the available list no longer
            // contains the selected URN — the dropdown is responsible for
            // showing/handling stale chips, not the context.
            expect(result.current.selectedOwnerUrns).toEqual(['urn:li:corpuser:jane']);
        });
    });
});
