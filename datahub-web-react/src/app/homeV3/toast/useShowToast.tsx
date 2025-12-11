/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import '@app/homeV3/toast/notification-toast-styles.less';

import { Icon, Text, colors } from '@components';
import { notification } from 'antd';
import React from 'react';

const notificationStyles = {
    backgroundColor: colors.blue[0],
    borderRadius: 8,
    width: 'max-content',
    padding: '8px 4px',
    right: 50,
    bottom: -8,
};

export default function useShowToast() {
    function showToast(title: string, description?: string, dataTestId?: string) {
        notification.open({
            message: (
                <Text color="blue" colorLevel={1000} weight="semiBold" lineHeight="sm" data-testid={dataTestId}>
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
            closeIcon: (
                <Icon icon="X" source="phosphor" color="blue" size="lg" data-testid="toast-notification-close-icon" />
            ),
            style: notificationStyles,
        });
    }
    return { showToast };
}
