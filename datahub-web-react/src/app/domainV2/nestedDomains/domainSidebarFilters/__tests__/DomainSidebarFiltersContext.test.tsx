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
        pictureLink: null,
        ...overrides,
    };
}

describe('DomainSidebarFiltersContext', () => {
    describe('defaults', () => {
        it('initializes both filter state and the owner registry to empty', () => {
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
            expect(() => result.current.registerOwners([makeOwnerInfo()])).not.toThrow();
        });
    });

    describe('setSelectedOwnerUrns', () => {
        it('updates the selection without touching the owner registry', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => result.current.setSelectedOwnerUrns(['urn:li:corpuser:jane']));

            expect(result.current.selectedOwnerUrns).toEqual(['urn:li:corpuser:jane']);
            expect(result.current.availableOwners).toEqual([]);
        });
    });

    describe('registerOwners', () => {
        it('publishes new owners in insertion order', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() =>
                result.current.registerOwners([
                    makeOwnerInfo({ urn: 'urn:li:corpuser:jane' }),
                    makeOwnerInfo({ urn: 'urn:li:corpuser:john', displayName: 'John' }),
                ]),
            );

            expect(result.current.availableOwners.map((o) => o.urn)).toEqual([
                'urn:li:corpuser:jane',
                'urn:li:corpuser:john',
            ]);
        });

        it('dedupes by URN across calls — first occurrence wins', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => {
                result.current.registerOwners([makeOwnerInfo({ urn: 'urn:li:corpuser:jane', displayName: 'First' })]);
            });
            act(() => {
                result.current.registerOwners([
                    makeOwnerInfo({ urn: 'urn:li:corpuser:jane', displayName: 'Second' }),
                    makeOwnerInfo({ urn: 'urn:li:corpuser:john', displayName: 'John' }),
                ]);
            });

            expect(result.current.availableOwners).toEqual([
                makeOwnerInfo({ urn: 'urn:li:corpuser:jane', displayName: 'First' }),
                makeOwnerInfo({ urn: 'urn:li:corpuser:john', displayName: 'John' }),
            ]);
        });

        it('preserves the existing list reference when the batch contains only known URNs', () => {
            // This matters because DomainNode calls registerOwners on every
            // render. If a no-op batch returned a fresh array, the provider
            // would re-render needlessly and every memoized child would too.
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => result.current.registerOwners([makeOwnerInfo({ urn: 'urn:li:corpuser:jane' })]));
            const firstSnapshot = result.current.availableOwners;

            act(() => result.current.registerOwners([makeOwnerInfo({ urn: 'urn:li:corpuser:jane' })]));
            const secondSnapshot = result.current.availableOwners;

            expect(secondSnapshot).toBe(firstSnapshot);
        });

        it('is a no-op for an empty batch', () => {
            const { result } = renderHook(() => useDomainSidebarFilters(), { wrapper });

            act(() => result.current.registerOwners([]));

            expect(result.current.availableOwners).toEqual([]);
        });
    });
});
