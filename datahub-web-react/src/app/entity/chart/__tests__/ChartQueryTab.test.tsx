import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ChartQueryTab } from '@app/entity/chart/ChartQueryTab';
import { useBaseEntity } from '@app/entity/shared/EntityContext';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: vi.fn(),
}));

vi.mock('react-syntax-highlighter', () => ({
    Prism: ({ children }: { children: React.ReactNode }) => <pre data-testid="syntax-highlighter">{children}</pre>,
}));

vi.mock('@graphql/app.generated', () => ({
    useAppConfigQuery: vi.fn().mockReturnValue({ data: undefined, refetch: vi.fn() }),
}));

describe('ChartQueryTab', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render query when canViewQueries is true', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            chart: {
                query: {
                    rawQuery: 'SELECT * FROM users',
                    type: 'SQL',
                },
                privileges: {
                    canViewQueries: true,
                },
            },
        });

        render(
            <TestPageContainer>
                <ChartQueryTab />
            </TestPageContainer>,
        );

        expect(screen.getByText('SELECT * FROM users')).toBeInTheDocument();
        expect(screen.queryByText(/don't have permission/)).not.toBeInTheDocument();
    });

    it('should render query when privileges is undefined (backward compatibility)', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            chart: {
                query: {
                    rawQuery: 'SELECT * FROM orders',
                    type: 'SQL',
                },
            },
        });

        render(
            <TestPageContainer>
                <ChartQueryTab />
            </TestPageContainer>,
        );

        expect(screen.getByText('SELECT * FROM orders')).toBeInTheDocument();
        expect(screen.queryByText(/don't have permission/)).not.toBeInTheDocument();
    });

    it('should render permission denied message when canViewQueries is false', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            chart: {
                query: {
                    rawQuery: 'SELECT * FROM secret_data',
                    type: 'SQL',
                },
                privileges: {
                    canViewQueries: false,
                },
            },
        });

        render(
            <TestPageContainer>
                <ChartQueryTab />
            </TestPageContainer>,
        );

        expect(screen.getByText(/don't have permission to view the query/)).toBeInTheDocument();
        expect(screen.queryByText('SELECT * FROM secret_data')).not.toBeInTheDocument();
    });

    it('should render query type correctly', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            chart: {
                query: {
                    rawQuery: 'SELECT 1',
                    type: 'sql',
                },
                privileges: {
                    canViewQueries: true,
                },
            },
        });

        render(
            <TestPageContainer>
                <ChartQueryTab />
            </TestPageContainer>,
        );

        expect(screen.getByText('SQL')).toBeInTheDocument();
    });
});
