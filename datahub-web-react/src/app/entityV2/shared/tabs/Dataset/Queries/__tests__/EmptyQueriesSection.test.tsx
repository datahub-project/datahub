import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import EmptyQueriesSection from '@app/entityV2/shared/tabs/Dataset/Queries/EmptyQueriesSection';
import { mocks } from '@src/Mocks';
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

describe('EmptyQueriesSection', () => {
    it('should render default empty text when emptyText is not provided', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EmptyQueriesSection showButton={false} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('No highlighted queries yet')).toBeInTheDocument();
    });

    it('should render custom emptyText when provided', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EmptyQueriesSection
                        showButton={false}
                        emptyText="You don't have permission to view queries for this dataset."
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText("You don't have permission to view queries for this dataset.")).toBeInTheDocument();
        expect(screen.queryByText('No highlighted queries yet')).not.toBeInTheDocument();
    });

    it('should render section name when provided', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EmptyQueriesSection sectionName="Queries" showButton={false} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Queries')).toBeInTheDocument();
    });

    it('should show add button when showButton is true', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EmptyQueriesSection showButton buttonLabel="Add Query" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('add-query-button')).toBeInTheDocument();
    });

    it('should hide add button when showButton is false', () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <EmptyQueriesSection showButton={false} />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.queryByTestId('add-query-button')).not.toBeInTheDocument();
    });
});
