import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import NoResultsFoundPlaceholder from '@app/searchV2/searchBarV2/components/NoResultsFoundPlaceholder';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

function renderPlaceholder(props: React.ComponentProps<typeof NoResultsFoundPlaceholder>) {
    render(
        <MockedProvider>
            <TestPageContainer>
                <NoResultsFoundPlaceholder {...props} />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('NoResultsFoundPlaceholder', () => {
    it('shows the given message without a clear suggestion when nothing is applied', () => {
        renderPlaceholder({ message: 'No matching assets' });

        expect(screen.getByText('No results found')).toBeInTheDocument();
        expect(screen.getByText('No matching assets')).toBeInTheDocument();
        expect(screen.queryByTestId('no-results-found-button-clear')).not.toBeInTheDocument();
    });

    it('offers to clear applied filters', () => {
        const onClearFilters = vi.fn();
        renderPlaceholder({ hasAppliedFilters: true, onClearFilters });

        expect(screen.getByTestId('no-results-found')).toHaveTextContent(
            "Try adjusting your search to find what you're looking for, or clear filters.",
        );
        fireEvent.click(screen.getByTestId('no-results-found-button-clear'));
        expect(onClearFilters).toHaveBeenCalledTimes(1);
    });
});
