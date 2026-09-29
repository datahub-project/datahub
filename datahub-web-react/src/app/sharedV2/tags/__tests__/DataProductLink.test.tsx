import { render, screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';

import { DataProductLink } from '@app/sharedV2/tags/DataProductLink';
import themeV2 from '@conf/theme/themeV2';

import { EntityType } from '@types';

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: vi.fn(() => 'Test Data Product'),
        getEntityUrl: vi.fn(() => '/dataProduct/test'),
        getIcon: vi.fn(() => null),
    }),
}));

vi.mock('@app/shared/useEmbeddedProfileLinkProps', () => ({
    useEmbeddedProfileLinkProps: () => ({}),
}));

vi.mock('@app/recommendations/renderer/component/HoverEntityTooltip', () => ({
    HoverEntityTooltip: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

vi.mock('@app/entityV2/shared/components/styled/DeprecationIcon', () => ({
    DeprecationIcon: () => <div data-testid="deprecation-icon" />,
}));

vi.mock('@app/sharedV2/icons/PillRemoveIcon', () => ({
    default: () => null,
}));

const baseDataProduct = {
    urn: 'urn:li:dataProduct:test',
    type: EntityType.DataProduct,
} as any;

const renderDataProductLink = (dataProduct: any) =>
    render(
        <ThemeProvider theme={themeV2}>
            <BrowserRouter>
                <DataProductLink dataProduct={dataProduct} />
            </BrowserRouter>
        </ThemeProvider>,
    );

describe('DataProductLink', () => {
    it('renders deprecation icon when data product is deprecated', () => {
        renderDataProductLink({
            ...baseDataProduct,
            deprecation: { deprecated: true, note: 'Sunset', actor: null, decommissionTime: null },
        });
        expect(screen.getByTestId('deprecation-icon')).toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecated=false', () => {
        renderDataProductLink({
            ...baseDataProduct,
            deprecation: { deprecated: false, note: null, actor: null, decommissionTime: null },
        });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is null', () => {
        renderDataProductLink({ ...baseDataProduct, deprecation: null });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });

    it('does not render deprecation icon when deprecation is absent', () => {
        renderDataProductLink({ ...baseDataProduct });
        expect(screen.queryByTestId('deprecation-icon')).not.toBeInTheDocument();
    });
});
