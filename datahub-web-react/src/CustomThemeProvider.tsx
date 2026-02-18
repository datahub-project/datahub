import React, { useState } from 'react';
import { ThemeProvider } from 'styled-components';

import GlobalThemeStyles from '@app/GlobalThemeStyles';
import { loadIsDarkMode } from '@app/useIsDarkMode';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { useCustomThemeId } from '@app/useSetAppTheme';
import themes from '@conf/theme/themes';
import { Theme } from '@conf/theme/types';
import { CustomThemeContext } from '@src/customThemeContext';

interface Props {
    children: React.ReactNode;
}

const CustomThemeProvider = ({ children }: Props) => {
    // Note: AppConfigContext not provided yet, so both of these calls rely on the DEFAULT_APP_CONFIG
    const isThemeV2 = useIsThemeV2();
    const customThemeId = useCustomThemeId();
    const isDarkMode = loadIsDarkMode();

    // Note: If custom theme id is a json file, it will only be loaded later in useSetAppTheme
    let defaultTheme = themes.themeV1;
    if (isThemeV2 && isDarkMode) {
        defaultTheme = themes.themeV2Dark;
    } else if (isThemeV2) {
        defaultTheme = themes.themeV2;
    }
    const customTheme = customThemeId ? themes[customThemeId] : null;
    const [theme, setTheme] = useState<Theme>(customTheme ?? defaultTheme);

    return (
        <CustomThemeContext.Provider value={{ theme, updateTheme: setTheme }}>
            <ThemeProvider theme={theme}>
                <GlobalThemeStyles />
                {children}
            </ThemeProvider>
        </CustomThemeContext.Provider>
    );
};

export default CustomThemeProvider;
