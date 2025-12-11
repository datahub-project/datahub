/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { createGlobalStyle } from 'styled-components';

import { colors } from '@components/theme';

export const NotificationGlobalStyle = createGlobalStyle`
    .ant-notification {
        z-index: 1013; // one above antd modal (which is 1012)
    }

    .datahub-notification.ant-notification-notice {
        padding: 8px;
        border-radius: 8px;
    }
    
    .datahub-notification .ant-notification-notice-icon {
        height: calc(100% - 16px); // to vertically center the icon. 16px - sum of top and bottom padding (8px)
    }

    .datahub-notification .ant-notification-notice-close {
        top: 8px;
        height: calc(100% - 16px); // to vertically center the close icon. 16px - sum of top and bottom padding (8px)
    }

    // Error styles
    .datahub-notification.ant-notification-notice-error {
        background-color: ${colors.red[0]};
    }
`;
