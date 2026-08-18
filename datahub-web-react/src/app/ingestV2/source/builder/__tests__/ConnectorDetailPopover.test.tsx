import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { ConnectorDetailPopover } from '@app/ingestV2/source/builder/ConnectorDetailPopover';
import themes from '@conf/theme/themes';

const renderWithTheme = (ui: React.ReactElement) => render(<ThemeProvider theme={themes.themeV2}>{ui}</ThemeProvider>);

describe('ConnectorDetailPopover', () => {
    it('renders children directly with no popover when there is no enrichment data', () => {
        renderWithTheme(
            <ConnectorDetailPopover name="Snowflake">
                <button type="button">card</button>
            </ConnectorDetailPopover>,
        );
        expect(screen.getByText('card')).toBeInTheDocument();
        // No support status, capabilities, or docs → the capabilities section never renders.
        expect(screen.queryByText('Capabilities')).not.toBeInTheDocument();
    });

    it('reveals capabilities and a docs link on hover when enrichment data is present', async () => {
        renderWithTheme(
            <ConnectorDetailPopover
                name="Snowflake"
                supportStatus="CERTIFIED"
                capabilities={[
                    { capability: 'CONTAINERS', description: '', supported: true },
                    { capability: 'TEST_CONNECTION', description: '', supported: false },
                ]}
                docsUrl="https://docs.example.com/snowflake"
            >
                <button type="button">card</button>
            </ConnectorDetailPopover>,
        );

        fireEvent.mouseEnter(screen.getByText('card'));

        // Supported capability is labelled and shown under "Capabilities".
        expect(await screen.findByText('Capabilities')).toBeInTheDocument();
        expect(screen.getByText('Containers')).toBeInTheDocument();
        // Unsupported capability is shown under "Not Supported".
        expect(screen.getByText('Not Supported')).toBeInTheDocument();
        expect(screen.getByText('Test Connection')).toBeInTheDocument();
        expect(screen.getByText('View Documentation')).toBeInTheDocument();
    });
});
