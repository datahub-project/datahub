import React, { useContext } from 'react';

import { Theme } from '@conf/theme/types';

export const CustomThemeContext = React.createContext<{
    theme?: Theme;
    updateTheme: (theme: Theme) => void;
}>({ theme: undefined, updateTheme: (_) => null });

export function useCustomTheme() {
    return useContext(CustomThemeContext);
}
