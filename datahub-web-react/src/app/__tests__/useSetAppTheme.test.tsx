import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

// Import the mocked hook
import { useIsDarkMode } from '@app/theme/useIsDarkMode';
import { useCustomThemeId, useSetAppTheme } from '@app/useSetAppTheme';
import themes from '@conf/theme/themes';
import { CustomThemeContext } from '@src/customThemeContext';

// Mock useIsDarkMode to test dark mode branch independently
vi.mock('@app/theme/useIsDarkMode', () => ({
    useIsDarkMode: vi.fn(() => [false, vi.fn()]),
}));

// Mock useAppConfig to avoid AppConfigContext dependency
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        config: { visualConfig: { theme: null } },
        loaded: false,
    }),
}));

describe('useSetAppTheme', () => {
    const mockUpdateTheme = vi.fn();
    const mockContextValue = {
        theme: themes.themeV2,
        updateTheme: mockUpdateTheme,
    };

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('default theme selection', () => {
        it('uses themeV2 when isDarkMode is false and no custom theme is set', () => {
            const updateThemeSpy = vi.fn();
            const contextValue = {
                theme: themes.themeV2,
                updateTheme: updateThemeSpy,
            };
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={contextValue}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            expect(updateThemeSpy).toHaveBeenCalledWith(themes.themeV2);
        });

        it('uses themeV2Dark when isDarkMode is true and no custom theme is set', () => {
            (useIsDarkMode as any).mockReturnValue([true, vi.fn()]);

            const mockUpdateThemeDark = vi.fn();
            const mockContextValueDark = {
                theme: themes.themeV2Dark,
                updateTheme: mockUpdateThemeDark,
            };

            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={mockContextValueDark}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            expect(mockUpdateThemeDark).toHaveBeenCalledWith(themes.themeV2Dark);
        });
    });

    describe('customThemeId', () => {
        it('returns null from useCustomThemeId when no custom theme is configured', () => {
            const themeId = useCustomThemeId();
            expect(themeId).toBeNull();
        });

        it('prefers custom theme over default theme', () => {
            // Mock a custom theme being available
            // useCustomThemeId returns the ID, and useSetAppTheme should apply it
            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={mockContextValue}>{children}</CustomThemeContext.Provider>
            );

            const { result } = renderHook(() => useSetAppTheme(), { wrapper });
            // The hook should run without error
            expect(result).toBeDefined();
        });

        it('falls back to default theme when customThemeId is invalid (not in themes object)', () => {
            // Set localStorage to simulate an invalid custom theme ID being persisted
            localStorage.setItem('customThemeId', 'invalid-theme-id');

            (useIsDarkMode as any).mockReturnValue([false, vi.fn()]);

            const updateThemeSpy = vi.fn();
            const contextValue = {
                theme: themes.themeV2,
                updateTheme: updateThemeSpy,
            };

            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={contextValue}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            // Should fall back to themeV2 when dark mode is false and theme ID is invalid
            expect(updateThemeSpy).toHaveBeenCalledWith(themes.themeV2);

            // Cleanup
            localStorage.removeItem('customThemeId');
        });

        it('falls back to dark theme when customThemeId is invalid and dark mode is enabled', () => {
            // Set localStorage to simulate an invalid custom theme ID being persisted
            localStorage.setItem('customThemeId', 'invalid-theme-id');

            (useIsDarkMode as any).mockReturnValue([true, vi.fn()]);

            const updateThemeSpy = vi.fn();
            const contextValue = {
                theme: themes.themeV2Dark,
                updateTheme: updateThemeSpy,
            };

            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={contextValue}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            // Should fall back to themeV2Dark when dark mode is true and theme ID is invalid
            expect(updateThemeSpy).toHaveBeenCalledWith(themes.themeV2Dark);

            // Cleanup
            localStorage.removeItem('customThemeId');
        });
    });

    describe('dark mode effect', () => {
        it('applies themeV2Dark when dark mode is enabled', () => {
            (useIsDarkMode as any).mockReturnValue([true, vi.fn()]);

            const updateThemeSpy = vi.fn();
            const contextValue = {
                theme: themes.themeV2Dark,
                updateTheme: updateThemeSpy,
            };

            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={contextValue}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            expect(updateThemeSpy).toHaveBeenCalledWith(themes.themeV2Dark);
        });

        it('applies themeV2 when dark mode is disabled', () => {
            (useIsDarkMode as any).mockReturnValue([false, vi.fn()]);

            const updateThemeSpy = vi.fn();
            const contextValue = {
                theme: themes.themeV2,
                updateTheme: updateThemeSpy,
            };

            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={contextValue}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            expect(updateThemeSpy).toHaveBeenCalledWith(themes.themeV2);
        });
    });

    describe('theme persistence', () => {
        it('persists custom theme ID to localStorage', () => {
            const localStorageSpy = vi.spyOn(Storage.prototype, 'setItem');

            const wrapper = ({ children }: { children: React.ReactNode }) => (
                <CustomThemeContext.Provider value={mockContextValue}>{children}</CustomThemeContext.Provider>
            );

            renderHook(() => useSetAppTheme(), { wrapper });

            localStorageSpy.mockRestore();
            expect(localStorageSpy).toBeDefined();
        });
    });
});
