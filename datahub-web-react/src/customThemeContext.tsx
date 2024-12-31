import React, { useContext } from 'react';

export const CustomThemeContext = React.createContext<{
    theme: any;
    updateTheme: (theme: any) => void;
}>({ theme: undefined, updateTheme: (_) => null });

export function useCustomTheme() {
    return useContext(CustomThemeContext);
}
