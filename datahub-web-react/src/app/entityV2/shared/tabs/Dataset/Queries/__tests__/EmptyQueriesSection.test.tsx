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

vi.mock('@src/AppConfigProvider', () => ({
    default: ({ children }: { children: React.ReactNode }) => children,
}));

describe('EmptyQueriesSection', () => {
    it('should render default empty text when emptyText is not provided', () => {
        render(
            <TestPageContainer>
                <EmptyQueriesSection showButton={false} />
            </TestPageContainer>,
        );

        expect(screen.getByText('No highlighted queries yet')).toBeInTheDocument();
    });

    it('should render custom emptyText when provided', () => {
        render(
            <TestPageContainer>
                <EmptyQueriesSection
                    showButton={false}
                    emptyText="You don't have permission to view queries for this dataset."
                />
            </TestPageContainer>,
        );

        expect(screen.getByText("You don't have permission to view queries for this dataset.")).toBeInTheDocument();
        expect(screen.queryByText('No highlighted queries yet')).not.toBeInTheDocument();
    });

    it('should render section name when provided', () => {
        render(
            <TestPageContainer>
                <EmptyQueriesSection sectionName="Queries" showButton={false} />
            </TestPageContainer>,
        );

        expect(screen.getByText('Queries')).toBeInTheDocument();
    });

    it('should show add button when showButton is true', () => {
        render(
            <TestPageContainer>
                <EmptyQueriesSection showButton buttonLabel="Add Query" />
            </TestPageContainer>,
        );

        expect(screen.getByTestId('add-query-button')).toBeInTheDocument();
    });

    it('should hide add button when showButton is false', () => {
        render(
            <TestPageContainer>
                <EmptyQueriesSection showButton={false} />
            </TestPageContainer>,
        );

        expect(screen.queryByTestId('add-query-button')).not.toBeInTheDocument();
    });
});
