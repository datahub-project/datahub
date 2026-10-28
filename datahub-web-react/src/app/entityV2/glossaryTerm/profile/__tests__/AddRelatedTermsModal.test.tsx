import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import AddRelatedTermsModal from '@app/entityV2/glossaryTerm/profile/AddRelatedTermsModal';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { TermRelationshipType } from '@types';

const { termSearch, searchState } = vi.hoisted(() => ({
    termSearch: vi.fn(),
    searchState: { data: undefined as { search?: { searchResults: unknown[] } } | undefined, loading: false },
}));

vi.mock('@graphql/search.generated', () => ({
    useGetSearchResultsLazyQuery: () => [termSearch, searchState],
}));

vi.mock('@graphql/glossaryTerm.generated', () => ({
    useAddRelatedTermsMutation: () => [vi.fn()],
}));

vi.mock('@app/glossary/GlossaryBrowser/GlossaryBrowser', () => ({
    default: () => React.createElement('div', { 'data-testid': 'glossary-browser' }),
}));

beforeAll(() => {
    vi.stubGlobal(
        'IntersectionObserver',
        class {
            private readonly callback: IntersectionObserverCallback;

            constructor(callback: IntersectionObserverCallback) {
                this.callback = callback;
            }

            observe(target: Element) {
                this.callback(
                    [{ isIntersecting: true, target } as IntersectionObserverEntry],
                    this as unknown as IntersectionObserver,
                );
            }

            unobserve() {}

            disconnect() {}

            takeRecords() {
                return [];
            }
        },
    );
});

const renderModal = () =>
    render(
        <MockedProvider>
            <CustomThemeProvider>
                <AddRelatedTermsModal onClose={vi.fn()} relationshipType={TermRelationshipType.HasA} />
            </CustomThemeProvider>
        </MockedProvider>,
    );

describe('AddRelatedTermsModal', () => {
    beforeEach(() => {
        termSearch.mockReset();
        searchState.data = undefined;
        searchState.loading = false;
        termSearch.mockImplementation(() => {
            searchState.loading = true;
        });
    });

    it('shows the glossary browser when the modal opens', () => {
        renderModal();

        expect(screen.getByTestId('glossary-browser')).toBeInTheDocument();
    });

    it('shows a loader instead of no-results while term search is in flight', () => {
        renderModal();

        fireEvent.change(screen.getByPlaceholderText('Search...'), { target: { value: 'revenue' } });

        expect(screen.getByLabelText('Loading...')).toBeInTheDocument();
        expect(screen.queryByTestId('no-results-found')).not.toBeInTheDocument();
        expect(termSearch).toHaveBeenCalled();
    });
});
