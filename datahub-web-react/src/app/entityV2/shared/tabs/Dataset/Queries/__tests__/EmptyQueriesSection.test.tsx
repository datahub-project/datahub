import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import EmptyQueriesSection from '@app/entityV2/shared/tabs/Dataset/Queries/EmptyQueriesSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@images/no-docs.svg', () => ({
    default: 'no-docs.svg',
}));

vi.mock('../AddButton', () => ({
    default: ({ buttonLabel }: { buttonLabel?: string }) => (
        <button type="button" data-testid="add-query-button">
            {buttonLabel || 'Add'}
        </button>
    ),
}));

const renderWithProviders = (ui: React.ReactElement) =>
    render(
        <MockedProvider mocks={[]} addTypename={false}>
            <TestPageContainer>{ui}</TestPageContainer>
        </MockedProvider>,
    );

describe('EmptyQueriesSection', () => {
    it('should render default empty text when emptyText is not provided', () => {
        renderWithProviders(<EmptyQueriesSection showButton={false} />);

        expect(screen.getByText('No highlighted queries yet')).toBeInTheDocument();
    });

    it('should render custom emptyText when provided', () => {
        renderWithProviders(
            <EmptyQueriesSection
                showButton={false}
                emptyText="You don't have permission to view queries for this dataset."
            />,
        );

        expect(screen.getByText("You don't have permission to view queries for this dataset.")).toBeInTheDocument();
        expect(screen.queryByText('No highlighted queries yet')).not.toBeInTheDocument();
    });

    it('should render section name when provided', () => {
        renderWithProviders(<EmptyQueriesSection sectionName="Queries" showButton={false} />);

        expect(screen.getByText('Queries')).toBeInTheDocument();
    });

    it('should show add button when showButton is true', () => {
        renderWithProviders(<EmptyQueriesSection showButton buttonLabel="Add Query" />);

        expect(screen.getByTestId('add-query-button')).toBeInTheDocument();
    });

    it('should hide add button when showButton is false', () => {
        renderWithProviders(<EmptyQueriesSection showButton={false} />);

        expect(screen.queryByTestId('add-query-button')).not.toBeInTheDocument();
    });
});
