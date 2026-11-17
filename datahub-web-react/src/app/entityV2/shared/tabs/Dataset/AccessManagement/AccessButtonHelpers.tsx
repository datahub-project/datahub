import { Tooltip } from '@components';
import { Button } from 'antd';
import i18next from 'i18next';
import React from 'react';
import styled from 'styled-components';

/**
 * Styled button component for access management actions.
 * Supports both enabled (request) and disabled (granted) states.
 */
export const AccessButton = styled(Button)`
    background-color: ${(props) => props.theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    min-width: 80px;
    height: 30px;
    border-radius: 3.5px;
    border: none;
    font-weight: bold;

    &:hover {
        background-color: ${(props) => props.theme.colors.buttonSurfaceBrandHover};
        color: ${(props) => props.theme.colors.textOnFillBrand};
        border: none;
    }

    /* Disabled state when user already has access */
    &:disabled {
        background-color: ${(props) => props.theme.colors.bgSurface};
        color: ${(props) => props.theme.colors.textDisabled};
        cursor: not-allowed;
        border: 1px solid ${(props) => props.theme.colors.border};

        &:hover {
            background-color: ${(props) => props.theme.colors.bgSurface};
            color: ${(props) => props.theme.colors.textDisabled};
            border: 1px solid ${(props) => props.theme.colors.border};
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
 * Determines the button text based on access status
 */
export const getAccessButtonText = (hasAccess: boolean, url?: string): string => {
    if (hasAccess) return i18next.t('entity.profile.access:accessManagement.granted');
    if (!url) return i18next.t('entity.profile.access:accessManagement.notGranted');
    return i18next.t('entity.profile.access:accessManagement.request');
};

/**
 * Determines if the button should be disabled
 */
export const isAccessButtonDisabled = (hasAccess: boolean, url?: string): boolean => hasAccess || !url;

/**
 * Determines the button aria-label based on access status
 */
const getAccessButtonAriaLabel = (hasAccess: boolean, url?: string): string => {
    if (hasAccess) return i18next.t('entity.profile.access:accessManagement.accessAlreadyGranted');
    if (!url) return i18next.t('entity.profile.access:accessManagement.accessNotGranted');
    return i18next.t('entity.profile.access:accessManagement.requestAccess');
};

/**
 * Renders an access button with appropriate state and tooltip.
 * Shows "Granted" (disabled) if user has access, "Request" (enabled) if they don't and a request URL
 * is configured, and "Not granted" (disabled) if they don't and there is no request URL.
 */
export const renderAccessButton = (roleData: RoleAccessData): React.ReactElement => {
    const { hasAccess, url } = roleData;

    const button = (
        <AccessButton
            disabled={isAccessButtonDisabled(hasAccess, url)}
            onClick={handleAccessButtonClick(hasAccess, url)}
            aria-label={getAccessButtonAriaLabel(hasAccess, url)}
        >
            {getAccessButtonText(hasAccess, url)}
        </AccessButton>
    );

    // Only requestable roles (no access, request URL set) render without a tooltip
    if (!isAccessButtonDisabled(hasAccess, url)) {
        return button;
    }

    return (
        <Tooltip
            title={
                hasAccess
                    ? i18next.t('entity.profile.access:accessManagement.accessGrantedTooltip')
                    : i18next.t('entity.profile.access:accessManagement.accessNotGrantedTooltip')
            }
            placement="top"
        >
            {button}
        </Tooltip>
    );
};
