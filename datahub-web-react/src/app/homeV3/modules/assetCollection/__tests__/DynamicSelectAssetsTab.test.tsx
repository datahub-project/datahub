import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import DynamicSelectAssetsTab from '@app/homeV3/modules/assetCollection/DynamicSelectAssetsTab';
import { LogicalOperatorType, LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

// The builder is heavy and irrelevant here — this test pins only the
// classifier-to-hint wiring (visibility + which message key is chosen);
// the classifier itself is covered in useAssetCollectionViewAll.test.ts.
vi.mock('@app/sharedV2/queryBuilder/LogicalFiltersBuilder', () => ({
    default: () => <div data-testid="filters-builder" />,
}));

vi.mock('@components', () => ({
    Alert: ({ title, 'data-testid': dataTestId }: { title: string; 'data-testid': string }) => (
        <div data-testid={dataTestId}>{title}</div>
    ),
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistryV2: () => ({ getTypeFromGraphName: () => undefined }),
}));

// t() returns the key so assertions pin exactly which message is selected.
vi.mock('react-i18next', () => ({
    useTranslation: () => ({ t: (key: string) => key }),
}));

const predicate = (operands: LogicalPredicate['operands']): LogicalPredicate => ({
    type: 'logical',
    operator: LogicalOperatorType.AND,
    operands,
});

describe('DynamicSelectAssetsTab View All hint', () => {
    it('shows no hint for representable filters, and the state-specific message otherwise', () => {
        const { rerender } = render(
            <DynamicSelectAssetsTab
                dynamicFilter={predicate([
                    { type: 'property', property: 'tags', operator: 'equals', values: ['urn:li:tag:a'] },
                ])}
                setDynamicFilter={vi.fn()}
            />,
        );
        expect(screen.queryByTestId('view-all-unsupported-hint')).not.toBeInTheDocument();

        rerender(
            <DynamicSelectAssetsTab
                dynamicFilter={predicate([{ type: 'property', property: 'owners' }])}
                setDynamicFilter={vi.fn()}
            />,
        );
        expect(screen.getByTestId('view-all-unsupported-hint')).toHaveTextContent(
            'assetCollection.viewAllIncompleteHint',
        );

        rerender(
            <DynamicSelectAssetsTab
                dynamicFilter={predicate([{ type: 'property', property: 'tags', operator: 'exists' }])}
                setDynamicFilter={vi.fn()}
            />,
        );
        expect(screen.getByTestId('view-all-unsupported-hint')).toHaveTextContent(
            'assetCollection.viewAllUnsupportedHint',
        );
    });
});
