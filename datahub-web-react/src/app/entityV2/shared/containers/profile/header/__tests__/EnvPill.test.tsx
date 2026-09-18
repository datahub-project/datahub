import { render } from '@testing-library/react';
import React from 'react';

import EnvPill from '@app/entityV2/shared/containers/profile/header/EnvPill';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { FabricType } from '@types';

const mockUseAppConfig = vi.fn();
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => mockUseAppConfig(),
}));

// Pill reads the styled-components theme (theme.colors), so it needs the same
// CustomThemeProvider wrapper other alchemy-component tests use.
function renderPill(environment: FabricType | null, showEnvironmentBadge: boolean) {
    mockUseAppConfig.mockReturnValue({ config: { visualConfig: { showEnvironmentBadge } }, loaded: false });
    return render(
        <CustomThemeProvider>
            <EnvPill environment={environment} />
        </CustomThemeProvider>,
    );
}

describe('EnvPill', () => {
    it('renders the environment label when the toggle is on', () => {
        expect(renderPill(FabricType.Prod, true).getByText('PROD')).toBeInTheDocument();
    });

    it('renders multi-word fabrics without the underscore', () => {
        expect(renderPill(FabricType.NonProd, true).getByText('NON PROD')).toBeInTheDocument();
    });

    it('renders nothing when the toggle is off', () => {
        expect(renderPill(FabricType.Prod, false).container).toBeEmptyDOMElement();
    });

    it('renders nothing without a resolved environment', () => {
        expect(renderPill(null, true).container).toBeEmptyDOMElement();
    });
});
