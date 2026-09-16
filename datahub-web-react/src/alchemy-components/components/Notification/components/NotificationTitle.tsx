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
