import { ConfigProvider, type ThemeConfig } from 'antd';
import React, { useMemo } from 'react';
import { useTheme } from 'styled-components';

import { antdTheme, getAntdTheme } from '@components/theme/antdTheme';

// message, notification and Modal.confirm render outside the React tree, so they don't see the
// ConfigProvider below. They read the active config from here each time they render.
let staticApiTheme: ThemeConfig = antdTheme;
ConfigProvider.config({
    holderRender: (children) => <ConfigProvider theme={staticApiTheme}>{children}</ConfigProvider>,
});

export function AntdThemeProvider({ children }: { children: React.ReactNode }) {
    const theme = useTheme();
    const config = useMemo(() => getAntdTheme(theme), [theme]);
    staticApiTheme = config;
    return <ConfigProvider theme={config}>{children}</ConfigProvider>;
}
