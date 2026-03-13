import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import QueriesTab from '@app/entity/shared/tabs/Dataset/Queries/QueriesTab';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: vi.fn(),
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: vi.fn().mockReturnValue({ config: { visualConfig: { queriesTab: {} } } }),
}));

vi.mock('@graphql/dataset.generated', () => ({
    useGetRecentQueriesQuery: vi.fn().mockReturnValue({ data: null }),
}));

vi.mock('@graphql/query.generated', () => ({
    useListQueriesQuery: vi.fn().mockReturnValue({ data: null, client: {} }),
}));

vi.mock('../QueriesTabToolbar', () => ({
    default: () => <div data-testid="queries-tab-toolbar">Toolbar</div>,
}));

vi.mock('../QueriesListSection', () => ({
    default: ({ title }: { title: string }) => <div data-testid={`queries-list-${title}`}>{title}</div>,
}));

vi.mock('../QueryBuilderModal', () => ({
    default: () => <div data-testid="query-builder-modal">Modal</div>,
}));

vi.mock('../EmptyQueries', () => ({
    default: ({ emptyText }: { emptyText?: string }) => (
        <div data-testid="empty-queries">{emptyText || 'No queries'}</div>
    ),
}));

describe('QueriesTab', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render toolbar when canViewQueries is true', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
                privileges: {
                    canViewQueries: true,
                    canEditQueries: false,
                },
            },
        });

        render(<QueriesTab />);

        expect(screen.getByTestId('queries-tab-toolbar')).toBeInTheDocument();
    });

    it('should render toolbar when privileges is undefined (backward compatibility)', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
            },
        });

        render(<QueriesTab />);

        expect(screen.getByTestId('queries-tab-toolbar')).toBeInTheDocument();
    });

    it('should hide toolbar and show permission denied when canViewQueries is false', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                urn: 'urn:li:dataset:test',
                privileges: {
                    canViewQueries: false,
                    canEditQueries: false,
                },
            },
        });

        render(<QueriesTab />);

        expect(screen.queryByTestId('queries-tab-toolbar')).not.toBeInTheDocument();
        expect(screen.getByTestId('empty-queries')).toBeInTheDocument();
        expect(screen.getByText(/don't have permission to view queries/)).toBeInTheDocument();
    });
});
