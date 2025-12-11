/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { NotificationType } from '@components/components/Notification/types';
import { Text, TextProps } from '@components/components/Text';

interface Props {
    notificationType: NotificationType;
}

function getTextProps(notificationType: NotificationType): Partial<TextProps> {
    const defaultProps: Partial<TextProps> = {
        weight: 'semiBold',
    };

    if (notificationType === NotificationType.ERROR) {
        return {
            ...defaultProps,
            color: 'red',
            colorLevel: 1000,
        };
    }

    return defaultProps;
}

export function NotificationTitle({ children, notificationType }: React.PropsWithChildren<Props>) {
    const textProps = getTextProps(notificationType);

    return <Text {...textProps}>{children}</Text>;
}
