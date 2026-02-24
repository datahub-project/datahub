import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import ViewDefinitionTab from '@app/entity/shared/tabs/Dataset/View/ViewDefinitionTab';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useBaseEntity: vi.fn(),
}));

vi.mock('react-syntax-highlighter', () => ({
    Prism: ({ children }: { children: React.ReactNode }) => <pre data-testid="syntax-highlighter">{children}</pre>,
}));

describe('ViewDefinitionTab', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render SQL logic when canViewQueries is true', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                viewProperties: {
                    logic: 'SELECT * FROM users WHERE active = true',
                    materialized: false,
                    language: 'sql',
                },
                privileges: {
                    canViewQueries: true,
                },
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <ViewDefinitionTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('SELECT * FROM users WHERE active = true')).toBeInTheDocument();
        expect(screen.queryByText(/don't have permission/)).not.toBeInTheDocument();
    });

    it('should render SQL logic when privileges is undefined (backward compatibility)', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                viewProperties: {
                    logic: 'SELECT id FROM orders',
                    materialized: true,
                    language: 'sql',
                },
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <ViewDefinitionTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('SELECT id FROM orders')).toBeInTheDocument();
        expect(screen.queryByText(/don't have permission/)).not.toBeInTheDocument();
    });

    it('should render permission denied message when canViewQueries is false', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                viewProperties: {
                    logic: 'SELECT * FROM secret_table',
                    materialized: false,
                    language: 'sql',
                },
                privileges: {
                    canViewQueries: false,
                },
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <ViewDefinitionTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText(/don't have permission to view the SQL logic/)).toBeInTheDocument();
        expect(screen.queryByText('SELECT * FROM secret_table')).not.toBeInTheDocument();
    });

    it('should still show Details section when canViewQueries is false', () => {
        (useBaseEntity as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            dataset: {
                viewProperties: {
                    logic: 'SELECT 1',
                    materialized: true,
                    language: 'sql',
                },
                privileges: {
                    canViewQueries: false,
                },
            },
        });

        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <ViewDefinitionTab />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('Details')).toBeInTheDocument();
        expect(screen.getByText('True')).toBeInTheDocument();
        expect(screen.getByText('SQL')).toBeInTheDocument();
    });
});
