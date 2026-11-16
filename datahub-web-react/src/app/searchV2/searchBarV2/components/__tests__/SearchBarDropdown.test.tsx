import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';
import { describe, expect, it } from 'vitest';

import SearchBarDropdown from '@app/searchV2/searchBarV2/components/SearchBarDropdown';
import themeV2 from '@conf/theme/themeV2';

// The markup antd's Select renders: a screen-reader list with a group heading, and visible option rows.
// Injected as HTML because it is the invalid ARIA under test.
const ANTD_SELECT_MARKUP = `
    <div role="listbox"><div role="presentation" aria-selected="false" data-testid="a11y-heading"></div></div>
    <div class="ant-select-item ant-select-item-option" aria-selected="false" data-testid="option-row">orders</div>
`;
const menu = <div dangerouslySetInnerHTML={{ __html: ANTD_SELECT_MARKUP }} />;

describe('SearchBarDropdown', () => {
    it('corrects the ARIA antd puts on select options', async () => {
        render(
            <ThemeProvider theme={themeV2}>
                <SearchBarDropdown menu={menu} query="" />
            </ThemeProvider>,
        );

        await waitFor(() => expect(screen.getByTestId('option-row')).toHaveAttribute('role', 'option'));
        expect(screen.getByTestId('option-row')).toHaveAttribute('aria-selected', 'false');
        expect(screen.getByTestId('a11y-heading')).not.toHaveAttribute('aria-selected');
    });
});
