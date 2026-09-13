import { render, screen } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { vi } from 'vitest';

import { useGetStructuredPropColumns } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetStructuredPropColumns';
import { SearchResult } from '@src/types.generated';

const testTheme = { colors: { bgSkeleton: '#f0f0f0', bgSkeletonShimmer: '#e0e0e0' } } as any;

vi.mock('@src/app/entityV2/dataset/profile/schema/components/StructuredPropValues', () => ({
    default: ({ propColumn }: any) => <span data-testid="prop-values">{propColumn.entity.urn}</span>,
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
        expect(result.current?.every((c) => c.dataIndex === 'schemaFieldEntity')).toBe(true);
    });

    it('renders real values once full metadata is loaded', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(properties, false));
        render(
            <ThemeProvider theme={testTheme}>
                {result.current?.[0].render({ urn: 'urn:li:schemaField:x' })}
            </ThemeProvider>,
        );
        expect(screen.getByTestId('prop-values')).toHaveTextContent('urn:li:structuredProperty:retention');
    });

    it('renders skeleton placeholders while full metadata is still loading', () => {
        const { result } = renderHook(() => useGetStructuredPropColumns(properties, true));
        render(
            <ThemeProvider theme={testTheme}>
                {result.current?.[0].render({ urn: 'urn:li:schemaField:x' })}
            </ThemeProvider>,
        );
        expect(screen.getByTestId('prop-cell-skeleton')).toBeInTheDocument();
        expect(screen.queryByTestId('prop-values')).not.toBeInTheDocument();
    });
});
