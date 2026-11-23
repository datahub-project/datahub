import { render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import SubDataProductsModule from '@app/entityV2/summary/modules/subDataProducts/SubDataProductsModule';
import { useGetChildDataProductsOfDataProduct } from '@app/entityV2/summary/modules/subDataProducts/useGetChildDataProductsOfDataProduct';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, EntityType, PageModuleScope } from '@types';

vi.mock('@components', () => ({
    InfiniteScrollList: ({ totalItemCount }: { totalItemCount: number }) => (
        <div data-testid="infinite-scroll-list" data-total-item-count={totalItemCount} />
    ),
}));

vi.mock('@app/entityV2/summary/modules/subDataProducts/useGetChildDataProductsOfDataProduct', () => ({
    useGetChildDataProductsOfDataProduct: vi.fn(),
}));

vi.mock('@app/homeV3/module/components/EntityItem', () => ({
    default: () => <div data-testid="entity-item" />,
}));

vi.mock('@app/homeV3/module/components/LargeModule', () => ({
    default: ({ children, loading, dataTestId }: any) => (
        <div data-testid={dataTestId} data-loading={loading}>
            {children}
        </div>
    ),
}));

describe('SubDataProductsModule', () => {
    const module: ModuleProps['module'] = {
        urn: 'urn:li:dataHubPageModule:sub_data_products',
        type: EntityType.DatahubPageModule,
        properties: {
            name: 'Sub-products',
            type: DataHubPageModuleType.SubDataProducts,
            visibility: { scope: PageModuleScope.Global },
            params: {},
        },
    };

    const props: ModuleProps = {
        module,
        position: { rowIndex: 0, moduleIndex: 0, numberOfModulesInRow: 1 },
    };

    beforeEach(() => {
        vi.clearAllMocks();
        (useGetChildDataProductsOfDataProduct as unknown as any).mockReturnValue({
            loading: false,
            total: 2,
            fetchEntities: vi.fn(),
        });
    });

    it('returns null when there are no sub-products', () => {
        (useGetChildDataProductsOfDataProduct as unknown as any).mockReturnValueOnce({
            loading: false,
            total: 0,
            fetchEntities: vi.fn(),
        });

        const { container } = render(<SubDataProductsModule {...props} />);

        expect(container.firstChild).toBeNull();
    });

    it('renders the large module when sub-products exist', () => {
        render(<SubDataProductsModule {...props} />);

        expect(screen.getByTestId('sub-data-products-module')).toBeInTheDocument();
        expect(screen.getByTestId('infinite-scroll-list')).toHaveAttribute('data-total-item-count', '2');
    });
});
