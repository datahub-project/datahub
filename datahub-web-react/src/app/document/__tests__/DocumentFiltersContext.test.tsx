import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { DocumentFiltersProvider, useDocumentFilters } from '@app/document/DocumentFiltersContext';

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <DocumentFiltersProvider>{children}</DocumentFiltersProvider>
);

describe('DocumentFiltersContext', () => {
    describe('defaults', () => {
        it('should initialize all three dimensions to their unfiltered defaults', () => {
            const { result } = renderHook(() => useDocumentFilters(), { wrapper });

            expect(result.current.status).toBe('all');
            expect(result.current.selectedAuthorUrns).toEqual([]);
            expect(result.current.selectedPlatformUrns).toEqual([]);
        });
    });

    describe('direct setters', () => {
        it('should update only the dimension being set', () => {
            const { result } = renderHook(() => useDocumentFilters(), { wrapper });

            act(() => result.current.setStatus('unpublished'));
            expect(result.current.status).toBe('unpublished');
            expect(result.current.selectedAuthorUrns).toEqual([]);
            expect(result.current.selectedPlatformUrns).toEqual([]);

            act(() => result.current.setSelectedAuthorUrns(['urn:li:corpuser:jane']));
            expect(result.current.status).toBe('unpublished');
            expect(result.current.selectedAuthorUrns).toEqual(['urn:li:corpuser:jane']);

            act(() => result.current.setSelectedPlatformUrns(['urn:li:dataPlatform:notion']));
            expect(result.current.selectedPlatformUrns).toEqual(['urn:li:dataPlatform:notion']);
        });
    });

    describe('hook misuse', () => {
        it('should throw when used outside of a DocumentFiltersProvider', () => {
            const { result } = renderHook(() => useDocumentFilters());
            expect(result.error?.message).toMatch(/DocumentFiltersProvider/);
        });
    });
});
