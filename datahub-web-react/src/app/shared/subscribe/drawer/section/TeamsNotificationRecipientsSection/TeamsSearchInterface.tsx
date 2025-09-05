import { Typography } from 'antd';
import React, { useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import TeamsUserChannelSearch from '@app/shared/subscribe/drawer/components/TeamsUserChannelSearch';
import { TeamsSearchResult } from '@app/shared/subscribe/drawer/teams-search-client';

import { useGetUserNotificationSettingsQuery } from '@graphql/settings.generated';

import teamsLogo from '@images/teamslogo.png';

const LEFT_PADDING = 36;

const StyledTeamsSection = styled.div`
    padding-left: ${LEFT_PADDING}px;
    margin-top: 8px;
`;

const PersonalTeamsInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px;
    background: #f6f7f9;
    border-radius: 6px;
    margin-bottom: 12px;
`;

const TeamsIcon = styled.img`
    height: 16px;
    width: 16px;
`;

const ConnectChannelLink = styled.span`
    color: #1890ff;
    text-decoration: underline;
    cursor: pointer;
    font-size: 12px;
    margin-top: 8px;

    &:hover {
        color: #40a9ff;
    }
`;

const SetupMessage = styled.div`
    padding: 12px;
    background: #fffbe6;
    border: 1px solid #ffe58f;
    border-radius: 6px;
    margin-bottom: 12px;
`;

type TeamsSearchInterfaceProps = {
    onSelectResult: (result: TeamsSearchResult) => void;
    selectedResult?: TeamsSearchResult | null;
    teamsSinkSupported: boolean;
    onSwitchToPersonalUser?: () => void;
};

export default function TeamsSearchInterface({
    onSelectResult,
    selectedResult,
    teamsSinkSupported,
    onSwitchToPersonalUser,
}: TeamsSearchInterfaceProps) {
    const [showChannelSearch, setShowChannelSearch] = useState(false);
    const hasAutoPopulated = useRef(false);

    // Get user's personal notification settings to check if Teams is configured
    const { data: userNotificationSettings } = useGetUserNotificationSettingsQuery();
    const teamsSettings = userNotificationSettings?.getUserNotificationSettings?.teamsSettings;
    const isPersonalTeamsConfigured = !!teamsSettings?.user;

    // Auto-populate user info from personal notification settings
    React.useEffect(() => {
        if (isPersonalTeamsConfigured && teamsSettings?.user && !selectedResult && !hasAutoPopulated.current) {
            hasAutoPopulated.current = true;
            // Create a TeamsSearchResult from the personal notification settings
            const personalTeamsUser: TeamsSearchResult = {
                id: teamsSettings.user.azureUserId || teamsSettings.user.email || '',
                displayName: teamsSettings.user.displayName || 'You',
                email: teamsSettings.user.email || '',
                type: 'user',
            };
            onSelectResult(personalTeamsUser);
        }

        // Reset the ref if selectedResult changes to null (user cleared selection)
        if (!selectedResult) {
            hasAutoPopulated.current = false;
        }
    }, [isPersonalTeamsConfigured, teamsSettings, selectedResult, onSelectResult]);

    if (!teamsSinkSupported) {
        return null; // This case is handled by the parent component
    }

    if (!isPersonalTeamsConfigured) {
        return (
            <StyledTeamsSection>
                <SetupMessage>
                    <Typography.Text>
                        Before you can subscribe, you need to{' '}
                        <Link to="/settings/personal-notifications" style={{ color: '#1890ff' }}>
                            connect your Teams ID
                        </Link>
                        .
                    </Typography.Text>
                </SetupMessage>
            </StyledTeamsSection>
        );
    }

    if (showChannelSearch) {
        return (
            <StyledTeamsSection>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    <TeamsUserChannelSearch
                        onSelectResult={onSelectResult}
                        selectedResult={selectedResult}
                        supportedTypes="channels" // Only channels when in this mode
                        disabled={!teamsSinkSupported}
                        placeholder="Search for channels..."
                    />
                    <ConnectChannelLink
                        onClick={() => {
                            setShowChannelSearch(false);
                            // Switch to personal user mode via callback
                            onSwitchToPersonalUser?.();
                        }}
                    >
                        Notify my Teams user instead
                    </ConnectChannelLink>
                </div>
            </StyledTeamsSection>
        );
    }

    return (
        <StyledTeamsSection>
            <PersonalTeamsInfo>
                <TeamsIcon src={teamsLogo} alt="Teams" />
                <Typography.Text>
                    Notifications will be sent to: <strong>{teamsSettings?.user?.displayName || 'You'}</strong>
                </Typography.Text>
            </PersonalTeamsInfo>
            <ConnectChannelLink
                onClick={() => {
                    setShowChannelSearch(true);
                }}
            >
                Notify a Teams channel instead
            </ConnectChannelLink>
        </StyledTeamsSection>
    );
}
