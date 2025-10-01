import { Button, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entity/shared/constants';

/**
 * Tooltip message for when user already has access
 */
export const ACCESS_GRANTED_TOOLTIP = 'You already have access to this role';

/**
 * Styled button component for access management actions.
 * Supports both enabled (request) and disabled (granted) states.
 */
export const AccessButton = styled(Button)`
    background-color: ${REDESIGN_COLORS.BLUE};
    color: ${ANTD_GRAY[1]};
    width: 80px;
    height: 30px;
    border-radius: 3.5px;
    border: none;
    font-weight: bold;

    &:hover {
        background-color: ${(props) => props.theme.styles['primary-color'] || '#18baff'};
        color: ${ANTD_GRAY[1]};
        border: none;
    }

    /* Disabled state when user already has access */
    &:disabled {
        background-color: ${ANTD_GRAY[3]};
        color: ${ANTD_GRAY[6]};
        cursor: not-allowed;
        border: 1px solid ${ANTD_GRAY[5]};

        &:hover {
            background-color: ${ANTD_GRAY[3]};
            color: ${ANTD_GRAY[6]};
            border: 1px solid ${ANTD_GRAY[5]};
        }
    }
`;

/**
 * Interface for role access data
 */
export interface RoleAccessData {
    hasAccess: boolean;
    url?: string;
    name?: string;
}

/**
 * Handles the click event for access request buttons.
 * Only opens the URL if the user doesn't already have access.
 */
export const handleAccessButtonClick = (hasAccess: boolean, url?: string) => (e: React.MouseEvent) => {
    if (!hasAccess && url) {
        e.preventDefault();
        window.open(url);
    }
};

/**
 * Renders an access button with appropriate state and tooltip.
 * Shows "Granted" (disabled) if user has access, "Request" (enabled) if they don't.
 */
export const renderAccessButton = (roleData: RoleAccessData): React.ReactElement | null => {
    const { hasAccess, url } = roleData;

    // Only show button if there's a URL to request access or user already has access
    if (!url && !hasAccess) {
        return null;
    }

    const button = (
        <AccessButton
            disabled={hasAccess}
            onClick={handleAccessButtonClick(hasAccess, url)}
            aria-label={hasAccess ? 'Access already granted' : 'Request access'}
        >
            {hasAccess ? 'Granted' : 'Request'}
        </AccessButton>
    );

    // Wrap with tooltip if user already has access
    return hasAccess ? (
        <Tooltip title={ACCESS_GRANTED_TOOLTIP} placement="top">
            {button}
        </Tooltip>
    ) : (
        button
    );
};

/**
 * Determines the button text based on access status
 */
export const getAccessButtonText = (hasAccess: boolean): string => (hasAccess ? 'Granted' : 'Request');

/**
 * Determines if the button should be disabled
 */
export const isAccessButtonDisabled = (hasAccess: boolean): boolean => hasAccess;
