import React, { useEffect, useState } from 'react';
import { ThemeProvider } from 'styled-components';
import { Theme } from './conf/theme/types';
import defaultThemeConfig from './conf/theme/theme_light.config.json';
import { CustomThemeContext } from './customThemeContext';

const CustomThemeProvider = ({ children }: { children: React.ReactNode }) => {
    const [currentTheme, setTheme] = useState<Theme>(defaultThemeConfig);

    useEffect(() => {
        if (import.meta.env.DEV) {
            import(/* @vite-ignore */ `./conf/theme/${import.meta.env.REACT_APP_THEME_CONFIG}`).then((theme) => {
                setTheme(theme);
            });
        } else {
            // Send a request to the server to get the theme config.
            fetch(`/assets/conf/theme/${import.meta.env.REACT_APP_THEME_CONFIG}`)
                .then((response) => response.json())
                .then((theme) => {
                    setTheme(theme);
                });
        }
    }, []);

    return (
        <CustomThemeContext.Provider value={{ theme: currentTheme, updateTheme: setTheme }}>
            <ThemeProvider theme={currentTheme}>{children}</ThemeProvider>
        </CustomThemeContext.Provider>
    );
};

export default CustomThemeProvider;
