import { Button, Tooltip } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

/**
 * Styled button component for access management actions.
 * Supports both enabled (request) and disabled (granted) states.
 */
const StyledAccessButton = styled(Button)`
    background-color: ${(props) => props.theme.colors.buttonFillBrand};
    color: ${(props) => props.theme.colors.textOnFillBrand};
    width: 80px;
    height: 30px;
    border-radius: 3.5px;
    border: none;
    font-weight: bold;

    &:hover {
        background-color: ${(props) => props.theme.styles['primary-color'] || props.theme.colors.bgSurfaceBrandHover};
        color: ${(props) => props.theme.colors.textOnFillBrand};
        border: none;
    }

    /* Disabled state when user already has access */
    &:disabled {
        background-color: ${(props) => props.theme.colors.bgDisabled};
        color: ${(props) => props.theme.colors.textDisabled};
        cursor: not-allowed;
        border: 1px solid ${(props) => props.theme.colors.borderDisabled};

        &:hover {
            background-color: ${(props) => props.theme.colors.bgDisabled};
            color: ${(props) => props.theme.colors.textDisabled};
            border: 1px solid ${(props) => props.theme.colors.borderDisabled};
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
const handleAccessButtonClick = (hasAccess: boolean, url?: string) => (e: React.MouseEvent) => {
    if (!hasAccess && url) {
        e.preventDefault();
        window.open(url);
    }
};

type AccessButtonProps = {
    roleData: RoleAccessData;
    fallback?: React.ReactElement | null;
};

/**
 * Renders an access button with appropriate state and tooltip.
 * Shows "Granted" (disabled) if user has access, "Request" (enabled) if they don't.
 */
export const AccessButton = ({ roleData, fallback = null }: AccessButtonProps): React.ReactElement | null => {
    const { t } = useTranslation('entity.profile.access');
    const { hasAccess, url } = roleData;

    // Only show button if there's a URL to request access or user already has access
    if (!url && !hasAccess) {
        return fallback;
    }

    const button = (
        <StyledAccessButton
            disabled={hasAccess}
            onClick={handleAccessButtonClick(hasAccess, url)}
            aria-label={hasAccess ? t('accessManagement.accessAlreadyGranted') : t('accessManagement.requestAccess')}
        >
            {hasAccess ? t('accessManagement.granted') : t('accessManagement.request')}
        </StyledAccessButton>
    );

    // Wrap with tooltip if user already has access
    return hasAccess ? (
        <Tooltip title={t('accessManagement.accessGrantedTooltip')} placement="top">
            {button}
        </Tooltip>
    ) : (
        button
    );
};
