import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import QueriesTab from '@app/entityV2/shared/tabs/Dataset/Queries/QueriesTab';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: vi.fn(),
}));

vi.mock('@app/entity/shared/siblingUtils', () => ({
    useIsSeparateSiblingsMode: vi.fn().mockReturnValue(false),
}));

vi.mock('../useHighlightedQueries', () => ({
    useHighlightedQueries: vi.fn().mockReturnValue({
        highlightedQueries: [],
        client: {},
        loading: false,
        pagination: { count: 10 },
        total: 0,
        sorting: {},
    }),
}));

vi.mock('../usePopularQueries', () => ({
    usePopularQueries: vi.fn().mockReturnValue({
        selectedUsersFilter: [],
        setSelectedUsersFilter: vi.fn(),
        selectedColumnsFilter: [],
        setSelectedColumnsFilter: vi.fn(),
    }),
}));

vi.mock('../useDownstreamQueries', () => ({
    default: vi.fn().mockReturnValue({
        downstreamQueries: [],
        loading: false,
    }),
}));

vi.mock('../useRecentQueries', () => ({
    useRecentQueries: vi.fn().mockReturnValue({
        recentQueries: [],
        loading: false,
    }),
}));

vi.mock('../EmptyQueriesSection', () => ({
    default: ({ emptyText, sectionName }: { emptyText?: string; sectionName?: string }) => (
        <div data-testid="empty-queries-section">
            {sectionName && <span data-testid="section-name">{sectionName}</span>}
            {emptyText && <span data-testid="empty-text">{emptyText}</span>}
        </div>
    ),
}));

vi.mock('../QueriesListSection', () => ({
    default: ({ title }: { title: string }) => <div data-testid={`queries-list-${title}`}>{title}</div>,
}));

vi.mock('../QueryBuilderModal', () => ({
    default: () => <div data-testid="query-builder-modal">Modal</div>,
}));

vi.mock('@app/shared/Loading', () => ({
    default: () => <div data-testid="loading">Loading...</div>,
}));

describe('QueriesTab (v2)', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should show permission denied when canViewQueries is false', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
                privileges: {
                    canViewQueries: false,
                    canEditQueries: false,
                },
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <QueriesTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('empty-queries-section')).toBeInTheDocument();
        expect(screen.getByTestId('empty-text')).toHaveTextContent(/don't have permission to view queries/);
    });

    it('should show empty section when canViewQueries is true but no queries exist', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
                privileges: {
                    canViewQueries: true,
                    canEditQueries: false,
                },
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <QueriesTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('empty-queries-section')).toBeInTheDocument();
        expect(screen.getByTestId('section-name')).toHaveTextContent('Highlighted Queries');
    });

    it('should show empty section when privileges is undefined (backward compatibility)', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <QueriesTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByTestId('empty-queries-section')).toBeInTheDocument();
        expect(screen.getByTestId('section-name')).toHaveTextContent('Highlighted Queries');
    });
});
