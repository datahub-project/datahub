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
