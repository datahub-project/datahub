import { Button, Icon, colors } from '@components';
import React, { useState } from 'react';
import styled, { keyframes } from 'styled-components';

import { FontColorLevelOptions, FontColorOptions } from '@components/theme/config';

import { useGlobalSettingsContext } from '@app/context/GlobalSettings/GlobalSettingsContext';

import { MaintenanceSeverity } from '@types';

const slideDown = keyframes`
    from {
        transform: translateY(-100%);
        opacity: 0;
    }
    to {
        transform: translateY(0);
        opacity: 1;
    }
`;

type SeverityConfig = {
    backgroundColor: string;
    textColor: string;
    iconName: 'WarningCircle' | 'Warning' | 'Info';
    iconColor: FontColorOptions;
    iconColorLevel: FontColorLevelOptions;
};

function getSeverityConfig(severity?: MaintenanceSeverity | null): SeverityConfig {
    switch (severity) {
        case MaintenanceSeverity.Critical:
            return {
                backgroundColor: colors.red[0],
                textColor: colors.red[1000],
                iconName: 'WarningCircle',
                iconColor: 'red',
                iconColorLevel: 1000,
            };
        case MaintenanceSeverity.Warning:
            return {
                backgroundColor: colors.yellow[0],
                textColor: colors.yellow[1000],
                iconName: 'Warning',
                iconColor: 'yellow',
                iconColorLevel: 1000,
            };
        case MaintenanceSeverity.Info:
        default:
            return {
                backgroundColor: colors.blue[0],
                textColor: colors.blue[1000],
                iconName: 'Info',
                iconColor: 'blue',
                iconColorLevel: 1000,
            };
    }
}

const BannerContainer = styled.div<{ $backgroundColor: string }>`
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 1000;
    width: 100%;
    animation: ${slideDown} 0.3s ease-out;
    background-color: ${({ $backgroundColor }) => $backgroundColor};
    border-bottom: 1px solid rgba(0, 0, 0, 0.06);
`;

const BannerContent = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 10px 48px;
    position: relative;
`;

const IconWrapper = styled.div`
    position: absolute;
    left: 16px;
    display: flex;
    align-items: center;
`;

const MessageWrapper = styled.span<{ $textColor: string }>`
    font-weight: 500;
    color: ${({ $textColor }) => $textColor};
`;

const BannerLink = styled.a<{ $textColor: string }>`
    margin-left: 12px;
    padding: 2px 10px;
    border-radius: 4px;
    text-decoration: none;
    font-weight: 500;
    color: ${({ $textColor }) => $textColor};
    background-color: rgba(0, 0, 0, 0.06);
    transition: background-color 0.2s ease;

    &:hover {
        text-decoration: none;
        background-color: rgba(0, 0, 0, 0.12);
    }
`;

const CloseButtonWrapper = styled.div`
    position: absolute;
    right: 8px;
`;

/**
 * A fixed banner displayed at the top of the application during maintenance windows.
 * Floats on top of everything with position: fixed.
 * Reads from GlobalSettingsContext and only renders when maintenance mode is enabled.
 * Users can dismiss the banner for the current session.
 */
export default function MaintenanceBanner() {
    const { globalSettings, loading } = useGlobalSettingsContext();
    const maintenanceWindow = globalSettings?.maintenanceWindow;
    const [isDismissed, setIsDismissed] = useState(false);

    // Don't render while loading or if maintenance is not enabled
    if (loading || !maintenanceWindow?.enabled) {
        return null;
    }

    const { message, severity, linkUrl, linkText } = maintenanceWindow;

    // Don't render if there's no message or if dismissed
    if (!message || isDismissed) {
        return null;
    }

    const severityConfig = getSeverityConfig(severity);
    const displayLinkText = linkText || 'Learn more';

    return (
        <BannerContainer
            $backgroundColor={severityConfig.backgroundColor}
            data-testid="maintenance-banner"
            data-severity={severity?.toLowerCase() ?? 'info'}
        >
            <BannerContent>
                <IconWrapper>
                    <Icon
                        icon={severityConfig.iconName}
                        source="phosphor"
                        color={severityConfig.iconColor}
                        colorLevel={severityConfig.iconColorLevel}
                        size="lg"
                    />
                </IconWrapper>
                <MessageWrapper $textColor={severityConfig.textColor}>
                    {message}
                    {linkUrl && (
                        <BannerLink
                            $textColor={severityConfig.textColor}
                            href={linkUrl}
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            {displayLinkText}
                        </BannerLink>
                    )}
                </MessageWrapper>
                <CloseButtonWrapper>
                    <Button
                        variant="text"
                        icon={{ icon: 'X', source: 'phosphor', size: 'lg' }}
                        onClick={() => setIsDismissed(true)}
                        aria-label="Dismiss maintenance banner"
                    />
                </CloseButtonWrapper>
            </BannerContent>
        </BannerContainer>
    );
}
