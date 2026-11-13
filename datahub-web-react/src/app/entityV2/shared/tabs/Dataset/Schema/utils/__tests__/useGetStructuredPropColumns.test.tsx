import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { vi } from 'vitest';

import { Column } from '@components/components/Table/types';

import { ExtendedSchemaFields } from '@app/entityV2/dataset/profile/schema/utils/types';
import { useGetStructuredPropColumns } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetStructuredPropColumns';
import { SearchResult } from '@src/types.generated';
import { renderHook } from '@utils/test-utils/renderHook';

const testTheme = { colors: { bgSkeleton: '#f0f0f0', bgSkeletonShimmer: '#e0e0e0' } } as any;

// Renders both inputs so a test can tell that the hook handed the cell the right record
// (schemaFieldEntity) for the right property column, not just that something rendered.
vi.mock('@src/app/entityV2/dataset/profile/schema/components/StructuredPropValues', () => ({
    default: ({ schemaFieldEntity, propColumn }: any) => (
        <span data-testid="prop-values">
            {schemaFieldEntity?.urn}|{propColumn.entity.urn}
        </span>
    ),
}));

vi.mock('@src/app/govern/structuredProperties/utils', () => ({
    getDisplayName: (entity: any) => entity?.definition?.displayName ?? 'unnamed',
}));

const properties = [
    {
        entity: {
            urn: 'urn:li:structuredProperty:retention',
            type: 'STRUCTURED_PROPERTY',
            definition: { displayName: 'Retention' },
        },
    },
    {
        entity: {
            urn: 'urn:li:structuredProperty:steward',
            type: 'STRUCTURED_PROPERTY',
            definition: { displayName: 'Steward' },
        },
    },
] as unknown as SearchResult[];

// A schema row whose field entity the cell should hand to StructuredPropValues.
const ROW = { fieldPath: 'x', schemaFieldEntity: { urn: 'urn:li:schemaField:x' } } as unknown as ExtendedSchemaFields;
const renderCell = (column: Column<ExtendedSchemaFields> | undefined) => column?.render?.(ROW, 0);

describe('useGetStructuredPropColumns', () => {
    it('returns undefined when no properties are given', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(undefined));
        expect(result.current).toBeUndefined();
    });

    it('builds one column per structured property, titled by display name and keyed by urn', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(properties));
        expect(result.current).toHaveLength(2);
        expect(result.current?.map((c) => c.title)).toEqual(['Retention', 'Steward']);
        expect(result.current?.map((c) => c.key)).toEqual([
            'urn:li:structuredProperty:retention',
            'urn:li:structuredProperty:steward',
        ]);
    });

    it('renders real values once full metadata is loaded', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(properties, 'ready'));
        render(<ThemeProvider theme={testTheme}>{renderCell(result.current?.[1])}</ThemeProvider>);
        // The cell receives the row's schemaFieldEntity and its own (second) property column.
        expect(screen.getByTestId('prop-values')).toHaveTextContent(
            'urn:li:schemaField:x|urn:li:structuredProperty:steward',
        );
        expect(screen.queryByTestId('prop-cell-skeleton')).not.toBeInTheDocument();
    });

    it('renders an unavailable marker instead of values when full metadata failed', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(properties, 'error'));
        render(<ThemeProvider theme={testTheme}>{renderCell(result.current?.[0])}</ThemeProvider>);
        expect(screen.getByTestId('metadata-unavailable')).toBeInTheDocument();
        expect(screen.queryByTestId('prop-values')).not.toBeInTheDocument();
        expect(screen.queryByTestId('prop-cell-skeleton')).not.toBeInTheDocument();
    });

    it('renders skeleton placeholders while full metadata is still loading', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(properties, 'loading'));
        render(<ThemeProvider theme={testTheme}>{renderCell(result.current?.[0])}</ThemeProvider>);
        expect(screen.getByTestId('prop-cell-skeleton')).toBeInTheDocument();
        expect(screen.queryByTestId('prop-values')).not.toBeInTheDocument();
    });
});
