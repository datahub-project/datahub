import { notification as antdNotification } from 'antd';
import { ArgsProps } from 'antd/lib/notification';
import React from 'react';

import { NotificationCloseIcon } from '@components/components/Notification/components/NotificationCloseIcon';
import { NotificationDescription } from '@components/components/Notification/components/NotificationDescription';
import { NotificationIcon } from '@components/components/Notification/components/NotificationIcon';
import { NotificationTitle } from '@components/components/Notification/components/NotificationTitle';
import { defaults } from '@components/components/Notification/defaults';
import { NotificationType } from '@components/components/Notification/types';

interface PropsWithNotificationType extends ArgsProps {
    notificationType: NotificationType;
    showDefaultIcon?: boolean;
}

function getProps({
    notificationType,
    message,
    description,
    showDefaultIcon,
    ...props
}: PropsWithNotificationType): ArgsProps {
    return {
        message: <NotificationTitle notificationType={notificationType}>{message}</NotificationTitle>,
        description: description ? (
            <NotificationDescription notificationType={notificationType}>{description}</NotificationDescription>
        ) : undefined,
        ...(showDefaultIcon ? {} : { icon: <NotificationIcon notificationType={notificationType} /> }),
        closeIcon: <NotificationCloseIcon notificationType={notificationType} />,
        ...props,
    };
}

export const notification = {
    ...antdNotification,
    success: (props: ArgsProps) => {
        antdNotification.success(
            getProps({ notificationType: NotificationType.SUCCESS, showDefaultIcon: true, ...defaults, ...props }),
        );
    },
    error: (props: ArgsProps) => {
        antdNotification.error(getProps({ notificationType: NotificationType.ERROR, ...defaults, ...props }));
    },
    info: (props: ArgsProps) => {
        antdNotification.info(
            getProps({ notificationType: NotificationType.INFO, showDefaultIcon: true, ...defaults, ...props }),
        );
    },
    warning: (props: ArgsProps) => {
        antdNotification.warning(
            getProps({ notificationType: NotificationType.WARNING, showDefaultIcon: true, ...defaults, ...props }),
        );
    },
};
