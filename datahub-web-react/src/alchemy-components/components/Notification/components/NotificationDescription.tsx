import React from 'react';

import { NotificationType } from '@components/components/Notification/types';
import { Text, TextProps } from '@components/components/Text';

function getTextProps(notificationType: NotificationType): Partial<TextProps> {
    const defaultProps: Partial<TextProps> = {
        size: 'sm',
        lineHeight: 'none',
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

interface Props {
    notificationType: NotificationType;
}

export function NotificationDescription({ children, notificationType }: React.PropsWithChildren<Props>) {
    const textProps = getTextProps(notificationType);

    return <Text {...textProps}>{children}</Text>;
}
