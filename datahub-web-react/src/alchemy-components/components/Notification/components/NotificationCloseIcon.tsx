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

function getIconProps(notificationType: NotificationType): IconProps {
    const defaultProps: IconProps = {
        source: 'phosphor',
        icon: 'X',
        size: 'lg',
    };

    if (notificationType === NotificationType.ERROR) {
        return {
            ...defaultProps,
            color: 'red',
            colorLevel: 1200,
        };
    }

    return defaultProps;
}

interface Props {
    notificationType: NotificationType;
}

export function NotificationCloseIcon({ notificationType }: Props) {
    const iconProps = getIconProps(notificationType);

    return (
        <IconWrapper>
            <Icon {...iconProps} />
        </IconWrapper>
    );
}
