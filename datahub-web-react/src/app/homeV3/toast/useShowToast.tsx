import '@app/homeV3/toast/notification-toast-styles.less';

import { Icon, Text } from '@components';
import { notification } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

const InfoText = styled(Text)`
    color: ${(props) => props.theme.colors.textInformation};
`;

export default function useShowToast() {
    const theme = useTheme();

    const notificationStyles = {
        backgroundColor: theme.colors.bgSurfaceInfo,
        borderRadius: 8,
        width: 'max-content',
        padding: '8px 4px',
        right: 50,
        bottom: -8,
    };

    function showToast(title: string, description?: string, dataTestId?: string) {
        notification.open({
            message: (
                <InfoText weight="semiBold" lineHeight="sm" data-testid={dataTestId}>
                    {title}
                </InfoText>
            ),
            description: <InfoText lineHeight="sm">{description}</InfoText>,
            placement: 'bottomRight',
            duration: 0,
            icon: <Icon icon="Info" weight="fill" source="phosphor" color="blue" />,
            closeIcon: (
                <Icon icon="X" source="phosphor" color="blue" size="lg" data-testid="toast-notification-close-icon" />
            ),
            style: notificationStyles,
        });
    }
    return { showToast };
}
