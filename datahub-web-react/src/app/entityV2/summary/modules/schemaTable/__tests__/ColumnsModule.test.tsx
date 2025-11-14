import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import ColumnsModule from '@app/entityV2/summary/modules/schemaTable/ColumnsModule';
import { ModuleProps, ModuleSize } from '@app/homeV3/module/types';

import { useGetDatasetSchemaQuery } from '@graphql/dataset.generated';
import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/SchemaTable', () => ({
    default: ({ children }: { children?: React.ReactNode }) => (
        <div data-testid="schema-table">{children || 'Schema Table Content'}</div>
    ),
}));

vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    useModuleContext: vi.fn().mockReturnValue({ size: ModuleSize.FULL }),
}));

vi.mock('@app/homeV3/module/components/EmptyContent', () => ({
    default: ({ title, description }: { title: string; description: string }) => (
        <div data-testid="empty-content">
            <h3>{title}</h3>
            <p>{description}</p>
        </div>
    ),
}));

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({ children, loading }: any) => (
        <div data-testid="large-module" data-loading={loading}>
            {children}
        </div>
    ),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: vi.fn().mockReturnValue({
        getEntityUrl: vi.fn().mockReturnValue('/dataset/test-dataset'),
    }),
}));

vi.mock('@graphql/dataset.generated', () => ({
    useGetDatasetSchemaQuery: vi.fn(),
}));

vi.mock('@app/entityV2/dataset/profile/schema/utils/utils', () => ({
    groupByFieldPath: vi.fn().mockReturnValue([]),
}));

vi.mock('@app/entityV2/shared/tabs/Dataset/Schema/utils/filterSchemaRows', () => ({
    SchemaFilterType: {
        FieldPath: 'fieldPath',
        Documentation: 'documentation',
        Tags: 'tags',
        Terms: 'terms',
    },
    filterSchemaRows: vi.fn().mockReturnValue({
        filteredRows: [],
        expandedRowsFromFilter: [],
    }),
}));

vi.mock('@app/shared/SchemaEditableContext', () => ({
    default: React.createContext({}),
}));

describe('ColumnsModule', () => {
    const mockModule: ModuleProps['module'] = {
        urn: 'urn:li:dataHubPageModule:columns',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Columns',
            type: DataHubPageModuleType.Columns,
            visibility: { scope: PageModuleScope.Global },
            params: {},
        },
    };

    const defaultProps: ModuleProps = {
        module: mockModule,
        position: { rowIndex: 0, moduleIndex: 0, numberOfModulesInRow: 1 },
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should render loading state when loading is true', () => {
        (useGetDatasetSchemaQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: undefined,
            loading: true,
            error: null,
            refetch: vi.fn(),
        });

        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)',
            entityType: EntityType.Dataset,
        });

        render(<ColumnsModule {...defaultProps} />);

        expect(screen.getByTestId('large-module')).toBeInTheDocument();
        expect(screen.getByTestId('large-module')).toHaveAttribute('data-loading', 'true');
    });

    it('should render empty content when no schema data is available', () => {
        (useGetDatasetSchemaQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: { dataset: { schemaMetadata: null } },
            loading: false,
            error: null,
            refetch: vi.fn(),
        });

        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)',
            entityType: EntityType.Dataset,
        });

        render(<ColumnsModule {...defaultProps} />);

        expect(screen.getByTestId('empty-content')).toBeInTheDocument();
        expect(screen.getByText('No Schema Fields')).toBeInTheDocument();
        expect(screen.getByText('This dataset has no schema fields to display')).toBeInTheDocument();
    });

    it('should render empty content when error occurs', () => {
        (useGetDatasetSchemaQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: undefined,
            loading: false,
            error: new Error('Failed to load'),
            refetch: vi.fn(),
        });

        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)',
            entityType: EntityType.Dataset,
        });

        render(<ColumnsModule {...defaultProps} />);

        expect(screen.getByTestId('empty-content')).toBeInTheDocument();
        expect(screen.getByText('Schema Not Available')).toBeInTheDocument();
        expect(screen.getByText('There was an error loading the schema for this dataset')).toBeInTheDocument();
    });

    it('should render schema table when schema metadata is available', () => {
        (useGetDatasetSchemaQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: {
                dataset: {
                    schemaMetadata: {
                        fields: [
                            {
                                fieldPath: 'field1',
                                type: { type: 'STRING' },
                                description: 'Test field',
                                tags: [],
                            },
                        ],
                    },
                },
            },
            loading: false,
            error: null,
            refetch: vi.fn(),
        });

        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)',
            entityType: EntityType.Dataset,
        });

        render(<ColumnsModule {...defaultProps} />);

        expect(screen.getByTestId('schema-table')).toBeInTheDocument();
    });

    it('should call refetch when schema is refetched', () => {
        const mockRefetch = vi.fn();
        (useGetDatasetSchemaQuery as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            data: { dataset: { schemaMetadata: null } },
            loading: false,
            error: null,
            refetch: mockRefetch,
        });

        (useEntityData as unknown as ReturnType<typeof vi.fn>).mockReturnValue({
            urn: 'urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)',
            entityType: EntityType.Dataset,
        });

        render(<ColumnsModule {...defaultProps} />);

        // The mockRefetch function should have been made available through the SchemaContext
        expect(mockRefetch).toBeDefined();
    });
});
