import { ConfigProvider } from 'antd';
import React from 'react';

import { antdTheme } from '@components/theme/antdTheme';

// message, notification and Modal.confirm render outside the React tree, so they don't see the
// ConfigProvider below and need the theme passed in separately
ConfigProvider.config({
    holderRender: (children) => <ConfigProvider theme={antdTheme}>{children}</ConfigProvider>,
});

export function AntdThemeProvider({ children }: { children: React.ReactNode }) {
    return <ConfigProvider theme={antdTheme}>{children}</ConfigProvider>;
}
