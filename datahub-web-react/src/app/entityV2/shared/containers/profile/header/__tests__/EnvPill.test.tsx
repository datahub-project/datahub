import { render } from '@testing-library/react';
import React from 'react';

import EnvPill from '@app/entityV2/shared/containers/profile/header/EnvPill';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { FabricType } from '@types';

describe('EnvPill', () => {
    it('renders the environment label', () => {
        // Tooltip (via alchemy-components) reads styled-components theme context, so it
        // needs the same CustomThemeProvider wrapper other Tooltip-consuming tests use
        // (see DeprecationPill.test.tsx) — a bare render() throws on theme.colors.
        const { getByText } = render(
            <CustomThemeProvider>
                <EnvPill environment={FabricType.Prod} />
            </CustomThemeProvider>,
        );
        expect(getByText('PROD')).toBeInTheDocument();
    });
});
