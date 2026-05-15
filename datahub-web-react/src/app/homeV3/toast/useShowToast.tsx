import '@app/homeV3/toast/notification-toast-styles.less';

import { Icon, Text } from '@components';
import { Info } from '@phosphor-icons/react/dist/csr/Info';
import { X } from '@phosphor-icons/react/dist/csr/X';
import { notification } from 'antd';
import React from 'react';
import { DefaultTheme, useTheme } from 'styled-components';

import CustomThemeProvider from '@src/CustomThemeProvider';

const notificationStyles = (theme: DefaultTheme) => ({
    backgroundColor: theme.colors.bgSurfaceInfo,
    borderRadius: 8,
    width: 'max-content',
    padding: '8px 4px',
    right: 50,
    bottom: -8,
});

function withTheme(children: React.ReactNode): React.ReactElement {
    return <CustomThemeProvider>{children}</CustomThemeProvider>;
}

export default function useShowToast() {
    const theme = useTheme();
    function showToast(title: string, description?: string, dataTestId?: string) {
        notification.open({
            message: withTheme(
                <Text color="blue" colorLevel={1000} weight="semiBold" lineHeight="sm" data-testid={dataTestId}>
                    {title}
                </Text>,
            ),
            description: withTheme(
                <Text color="blue" colorLevel={1000} lineHeight="sm">
                    {description}
                </Text>,
            ),
            placement: 'bottomRight',
            duration: 0,
            icon: withTheme(<Icon icon={Info} weight="fill" color="blue" />),
            closeIcon: withTheme(<Icon icon={X} color="blue" size="lg" data-testid="toast-notification-close-icon" />),
            style: notificationStyles(theme),
        });
    }
    return { showToast };
}
