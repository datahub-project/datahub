import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import { useGetEntityWithSchema } from '@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema';
import ColumnStatsTable from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/columnStats/ColumnStatsTable';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

// Local type definition to match the component's interface
interface DatasetFieldProfile {
    fieldPath: string;
    nullCount?: number | null;
    nullProportion?: number | null;
    uniqueCount?: number | null;
    uniqueProportion?: number | null;
    min?: string | null;
    max?: string | null;
}

// Mock the hooks and dependencies
vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/useGetEntitySchema');
vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/useKeyboardControls', () => ({
    __esModule: true,
    default: () => ({
        selectPreviousField: vi.fn(),
        selectNextField: vi.fn(),
    }),
}));

const mockUseGetEntityWithSchema = vi.mocked(useGetEntityWithSchema);

describe('ColumnStatsTable', () => {
    const mockSchemaFields = [
        {
            fieldPath: 'customer_id',
            type: { type: 'STRING' },
            nativeDataType: 'VARCHAR',
            nullable: false,
            recursive: false,
        },
        {
            fieldPath: 'customer_details.email',
            type: { type: 'STRING' },
            nativeDataType: 'VARCHAR',
            nullable: true,
            recursive: false,
        },
    ];

    const mockColumnStats: DatasetFieldProfile[] = [
        {
            fieldPath: 'customer_id',
            nullCount: 0,
            nullProportion: 0.0,
            uniqueCount: 100,
            uniqueProportion: 1.0,
            min: '1',
            max: '100',
        },
        {
            fieldPath: 'customer_details.email',
            nullCount: 5,
            nullProportion: 0.05,
            uniqueCount: 95,
            uniqueProportion: 0.95,
            min: 'alice@example.com',
            max: 'zoe@example.com',
        },
    ];

    beforeEach(() => {
        mockUseGetEntityWithSchema.mockReturnValue({
            entityWithSchema: {
                schemaMetadata: {
                    fields: mockSchemaFields as any,
                    name: 'test_schema',
                    version: 1,
                    platformUrn: 'urn:li:dataPlatform:test',
                    hash: 'test_hash',
                } as any,
                editableSchemaMetadata: null,
            },
            loading: false,
            refetch: vi.fn(),
        });
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    it('renders basic column stats table', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ColumnStatsTable columnStats={mockColumnStats} searchQuery="" />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Verify field names are displayed
        expect(screen.getByText('customer_id')).toBeInTheDocument();
        expect(screen.getByText('customer_details.email')).toBeInTheDocument();

        // Verify statistics are displayed
        expect(screen.getByText('0%')).toBeInTheDocument();
        expect(screen.getByText('5.00%')).toBeInTheDocument();
    });

    it('filters results based on search query', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ColumnStatsTable columnStats={mockColumnStats} searchQuery="email" />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Verify only matching field is displayed (text split by highlighting)
        expect(screen.getByText('customer_details.')).toBeInTheDocument();
        expect(screen.getByText('email')).toBeInTheDocument();
        expect(screen.queryByText('customer_id')).not.toBeInTheDocument();
    });

    it('handles nested field paths correctly', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ColumnStatsTable columnStats={mockColumnStats} searchQuery="" />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Verify nested field row is rendered correctly
        const nestedFieldRow = screen.getByText('customer_details.email').closest('tr');
        expect(nestedFieldRow).toBeInTheDocument();

        // Verify View buttons are available for all fields
        const viewButtons = screen.getAllByText('View');
        expect(viewButtons.length).toBe(2);
    });

    it('shows empty state when no results found', () => {
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ColumnStatsTable columnStats={mockColumnStats} searchQuery="nonexistent" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(screen.getByText('No search results!')).toBeInTheDocument();
    });

    it('handles simple field paths', () => {
        const simpleStats: DatasetFieldProfile[] = [
            {
                fieldPath: 'simple_field',
                nullCount: 10,
                nullProportion: 0.1,
                uniqueCount: 90,
                uniqueProportion: 0.9,
            },
        ];

        const simpleFields = [
            {
                fieldPath: 'simple_field',
                type: { type: 'STRING' },
                nativeDataType: 'VARCHAR',
                nullable: true,
                recursive: false,
            },
        ];

        mockUseGetEntityWithSchema.mockReturnValue({
            entityWithSchema: {
                schemaMetadata: {
                    fields: simpleFields as any,
                    name: 'test_schema',
                    version: 1,
                    platformUrn: 'urn:li:dataPlatform:test',
                    hash: 'test_hash',
                } as any,
                editableSchemaMetadata: null,
            },
            loading: false,
            refetch: vi.fn(),
        });

        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ColumnStatsTable columnStats={simpleStats} searchQuery="" />
                </TestPageContainer>
            </MockedProvider>,
        );

        // Verify simple field is displayed correctly
        expect(screen.getByText('simple_field')).toBeInTheDocument();
        expect(screen.getByText('10.00%')).toBeInTheDocument();
        expect(screen.getByRole('table')).toBeInTheDocument();
    });
});
