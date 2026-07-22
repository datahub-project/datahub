import { render, screen } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { FILTER_OPTIONS, SupportStatusBadge } from '@app/ingestV2/source/builder/SupportStatusBadge';
import themes from '@conf/theme/themes';

const renderWithTheme = (ui: React.ReactElement) => render(<ThemeProvider theme={themes.themeV2}>{ui}</ThemeProvider>);

describe('SupportStatusBadge', () => {
    it('renders the translated label for a known status', () => {
        renderWithTheme(<SupportStatusBadge status="CERTIFIED" />);
        expect(screen.getByText('Certified')).toBeInTheDocument();
    });

    it('renders nothing when the status is unset or UNKNOWN', () => {
        const { container } = renderWithTheme(<SupportStatusBadge status={undefined} />);
        expect(container).toBeEmptyDOMElement();

        const { container: unknownContainer } = renderWithTheme(<SupportStatusBadge status="UNKNOWN" />);
        expect(unknownContainer).toBeEmptyDOMElement();
    });

    it('hides the text label but still renders when showLabel is false', () => {
        renderWithTheme(<SupportStatusBadge status="INCUBATING" showLabel={false} />);
        expect(screen.queryByText('Incubating')).not.toBeInTheDocument();
    });
});

describe('FILTER_OPTIONS', () => {
    it('exposes the ALL filter plus one entry per non-unknown support tier', () => {
        const keys = FILTER_OPTIONS.map((o) => o.key);
        expect(keys).toEqual(['ALL', 'CERTIFIED', 'INCUBATING', 'COMMUNITY']);
    });
});
