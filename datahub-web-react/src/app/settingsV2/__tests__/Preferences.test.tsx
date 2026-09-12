import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import { Preferences } from '@app/settingsV2/Preferences';
import { useFeatureFlag } from '@app/sharedV2/hooks/useFeatureFlag';
import { THEME_DARK_MODE_FLAG } from '@app/theme/useIsDarkMode';
import themes from '@conf/theme/themes';

vi.mock('@app/context/useUserContext', () => ({
    useUserContext: () => ({ platformPrivileges: { manageFeatures: false } }),
}));

vi.mock('@app/i18n/hooks/useIsI18nEnabled', () => ({
    useIsI18nEnabled: () => false,
}));

vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({ config: { visualConfig: {} }, refreshContext: vi.fn() }),
}));

vi.mock('@app/sharedV2/hooks/useFeatureFlag', () => ({
    useFeatureFlag: vi.fn(() => true),
    loadFromLocalStorage: () => false,
}));

vi.mock('@graphql/app.generated', () => ({
    useUpdateApplicationsSettingsMutation: () => [vi.fn()],
}));

describe('Preferences', () => {
    beforeEach(() => {
        localStorage.clear();
        vi.mocked(useFeatureFlag).mockImplementation((key: string) => key === THEME_DARK_MODE_FLAG);
    });

    it('lets the user switch between light and dark mode when the flag is on', async () => {
        render(
            <ThemeProvider theme={themes.themeV2}>
                <Preferences />
            </ThemeProvider>,
        );

        const darkModeToggle = screen.getByRole('checkbox', { name: 'Dark mode' });
        expect(darkModeToggle).not.toBeChecked();

        fireEvent.click(darkModeToggle);

        // The toggle is deferred a couple of frames so the disabled state can paint
        // before the theme swap blocks the main thread.
        await waitFor(() => {
            expect(localStorage.getItem('isDarkModeEnabled')).toBe('true');
            expect(darkModeToggle).toBeChecked();
        });
    });

    it('disables the toggle while the theme swap is in flight', async () => {
        render(
            <ThemeProvider theme={themes.themeV2}>
                <Preferences />
            </ThemeProvider>,
        );

        const darkModeToggle = screen.getByRole('checkbox', { name: 'Dark mode' });
        expect(darkModeToggle).toBeEnabled();

        fireEvent.click(darkModeToggle);

        await waitFor(() => expect(screen.getByRole('checkbox', { name: 'Dark mode' })).toBeDisabled());
    });

    it('hides the dark mode toggle when the flag is off', () => {
        vi.mocked(useFeatureFlag).mockReturnValue(false);

        render(
            <ThemeProvider theme={themes.themeV2}>
                <Preferences />
            </ThemeProvider>,
        );

        expect(screen.queryByRole('checkbox', { name: 'Dark mode' })).not.toBeInTheDocument();
        expect(screen.getByText('No appearance settings found.')).toBeInTheDocument();
    });
});
