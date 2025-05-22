import React, { useEffect, useState } from 'react';
import { ThemeProvider } from 'styled-components';

import { loadThemeIdFromLocalStorage } from '@app/useSetAppTheme';
import defaultThemeConfig from '@conf/theme/theme_light.config.json';
import { Theme } from '@conf/theme/types';
import { CustomThemeContext } from '@src/customThemeContext';

interface Props {
    children: React.ReactNode;
    skipSetTheme?: boolean;
}

const CustomThemeProvider = ({ children, skipSetTheme }: Props) => {
    const [currentTheme, setTheme] = useState<Theme>(defaultThemeConfig);
    const customThemeId = loadThemeIdFromLocalStorage();

    useEffect(() => {
        // use provided customThemeId and set in useSetAppTheme.tsx if it exists
        if (customThemeId) return;

        if (import.meta.env.DEV) {
            import(/* @vite-ignore */ `./conf/theme/${import.meta.env.REACT_APP_THEME_CONFIG}`).then((theme) => {
                setTheme(theme);
            });
        } else if (!skipSetTheme) {
            // Send a request to the server to get the theme config.
            fetch(`/assets/conf/theme/${import.meta.env.REACT_APP_THEME_CONFIG}`)
                .then((response) => response.json())
                .then((theme) => {
                    setTheme(theme);
                });
        }
    }, [skipSetTheme, customThemeId]);

    return (
        <CustomThemeContext.Provider value={{ theme: currentTheme, updateTheme: setTheme }}>
            <ThemeProvider theme={currentTheme}>{children}</ThemeProvider>
        </CustomThemeContext.Provider>
    );
};

export default CustomThemeProvider;
