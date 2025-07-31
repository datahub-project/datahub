import '@app/homeV3/toast/notification-toast-styles.less';

import { Icon, Text, colors } from '@components';
import { notification } from 'antd';
import React from 'react';

const notificationStyles = {
    backgroundColor: colors.blue[0],
    borderRadius: 8,
    width: 475,
    padding: '8px 4px',
    right: 50,
    bottom: -8,
};

export default function useShowToast() {
    function showToast(title: string, description?: string) {
        notification.open({
            message: (
                <Text color="blue" colorLevel={1000} weight="semiBold" lineHeight="sm">
                    {title}
                </Text>
            ),
            description: (
                <Text color="blue" colorLevel={1000} lineHeight="sm">
                    {description}
                </Text>
            ),
            placement: 'bottomRight',
            duration: 0,
            icon: <Icon icon="Info" weight="fill" source="phosphor" color="blue" />,
            closeIcon: <Icon icon="X" source="phosphor" color="blue" size="lg" />,
            style: notificationStyles,
        });
    }
    return { showToast };
}
