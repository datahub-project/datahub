/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { Icon, IconProps } from '@components/components/Icon';
import { NotificationType } from '@components/components/Notification/types';

const IconWrapper = styled.div`
    display: flex;
    align-items: center;
    height: 100%;
`;

function getIconProps(notificationType: NotificationType): IconProps | undefined {
    const defaultProps: Partial<IconProps> = {
        size: '2xl',
    };

    if (notificationType === NotificationType.ERROR) {
        return {
            ...defaultProps,
            color: 'red',
            colorLevel: 1200,
            weight: 'fill',
            source: 'phosphor',
            icon: 'ExclamationMark',
        };
    }

    return undefined;
}

interface Props {
    notificationType: NotificationType;
}

export function NotificationIcon({ notificationType }: Props) {
    const iconProps = getIconProps(notificationType);

    if (iconProps === undefined) return null;

    return (
        <IconWrapper>
            <Icon {...iconProps} />
        </IconWrapper>
    );
}
