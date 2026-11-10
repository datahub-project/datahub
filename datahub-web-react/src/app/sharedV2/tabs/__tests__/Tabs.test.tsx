import { render, screen } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router';
import { ThemeProvider } from 'styled-components';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { Tabs } from '@app/sharedV2/tabs/Tabs';
import themeV2 from '@conf/theme/themeV2';

function Broken(): JSX.Element {
    throw new Error('tab crashed');
}

describe('Tabs', () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it('shows the app tab fallback instead of the crashed tab and keeps the tab bar', () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        vi.spyOn(console, 'group').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});

        render(
            <ThemeProvider theme={themeV2}>
                <MemoryRouter>
                    <Tabs tabs={[{ key: 'first', name: 'First tab', component: <Broken /> }]} selectedTab="first" />
                </MemoryRouter>
            </ThemeProvider>,
        );

        expect(screen.getByText('First tab')).toBeInTheDocument();
        expect(screen.getByText('Whoops!')).toBeInTheDocument();
    });
});
