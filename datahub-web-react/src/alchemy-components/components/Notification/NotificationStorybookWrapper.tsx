/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ArgsProps } from 'antd/lib/notification';
import React from 'react';

import { NotificationGlobalStyle } from '@components/components/Notification/components/NotificationGlobalStyle';
import { notification } from '@components/components/Notification/notification';
import { NotificationProps, NotificationType } from '@components/components/Notification/types';

interface Props extends NotificationProps {
    buttonText: string;
    notificationType: NotificationType;
}

export function NotificationStorybookWrapper({ notificationType, buttonText, ...props }: Props) {
    const onClick = () => {
        let fn: (args: ArgsProps) => void = () => {};
        if (notificationType === NotificationType.SUCCESS) {
            fn = notification.success;
        } else if (notificationType === NotificationType.ERROR) {
            fn = notification.error;
        } else if (notificationType === NotificationType.INFO) {
            fn = notification.info;
        } else if (notificationType === NotificationType.WARNING) {
            fn = notification.warning;
        }

        fn(props);
    };

    return (
        <>
            <NotificationGlobalStyle />
            <button type="button" onClick={onClick}>
                {buttonText}
            </button>
        </>
    );
}
