import { Button, Card, Tooltip, colors } from '@components';
import { Form, Space, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { TEAMS_CONNECTION_URN } from '@app/settingsV2/teams/utils';
import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import TeamsUserChannelSearch from '@app/shared/subscribe/drawer/components/TeamsUserChannelSearch';
import { TeamsSearchResult } from '@app/shared/subscribe/drawer/teams-search-client';
import { useUserContext } from '@src/app/context/useUserContext';
import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import { useCustomTheme } from '@src/customThemeContext';

const InputDiv = styled.div`
    width: 100%;
    max-width: 600px;
`;

const MessageDiv = styled.div`
    color: ${ANTD_GRAY[7]};
    font-size: 12px;
    margin-top: 4px;
    display: flex;
    align-items: center;
    gap: 4px;
`;

const StyledLabel = styled(Typography.Text)`
    display: inline-block;
    font-size: 14px;
`;

const SinkButtonsContainer = styled.div`
    display: flex;
    gap: 8px;
    margin-top: 12px;
`;

const HelperText = styled.div`
    color: ${colors.gray[1700]};
    margin-top: 6px;
    font-size: 12px;
`;

type Props = {
    isTeamsEnabled: boolean;
    channel?: string;
    channelName?: string;
    onChange: (channelId: string, displayName: string) => void;
};

export const TeamsDefaults = ({ isTeamsEnabled = false, channel, channelName, onChange }: Props) => {
    const hasChannel = !!channel;
    const { theme } = useCustomTheme();
    const [editing, setEditing] = useState<boolean>(isTeamsEnabled && !hasChannel);
    const [selectedValue, setSelectedValue] = useState(channel); // This stores the ID for backend
    const [selectedOption, setSelectedOption] = useState<TeamsSearchResult | null>(
        // Initialize with channel info if we have both ID and name
        channel && channelName
            ? {
                  id: channel,
                  type: 'channel' as const,
                  displayName: channelName,
                  teamName: '', // We don't have this info, but it's required
              }
            : null,
    );
    const me = useUserContext();
    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;
    const unsupportedSinkDescription = `In order to enable, ask your DataHub admin to setup the Teams integration.`;

    useEffect(() => {
        setEditing(isTeamsEnabled && !hasChannel);
    }, [hasChannel, isTeamsEnabled]);

    useEffect(() => {
        setSelectedValue(channel);
        // Update selectedOption when channel or channelName changes
        if (channel && channelName) {
            setSelectedOption({
                id: channel,
                type: 'channel' as const,
                displayName: channelName,
                teamName: '', // We don't have this info, but it's required
            });
        } else if (!channel) {
            setSelectedOption(null);
        }
    }, [channel, channelName]);

    const handleSelectResult = (result: TeamsSearchResult) => {
        setSelectedOption(result);
        setSelectedValue(result.id);
    };

    const onSave = async () => {
        onChange(selectedValue || '', selectedOption?.displayName || selectedValue || '');
        setEditing(false);
    };

    const onCancel = () => {
        setSelectedValue(channel);
        setSelectedOption(null);
        setEditing(false);
    };

    const renderSinkDescription = () => {
        if (!isTeamsEnabled && isAdminAccess) {
            return (
                <>
                    Teams is currently disabled.&nbsp;
                    <Link to="/settings/integrations/microsoft-teams" style={{ color: colors.violet['500'] }}>
                        Click here
                    </Link>{' '}
                    to setup the Teams integration.
                </>
            );
        }
        if (!isTeamsEnabled && !isAdminAccess) {
            return <>{unsupportedSinkDescription}</>;
        }
        return 'Receive Teams notifications when important changes occur across the platform.';
    };

    return (
        <Card title="Teams Notifications" subTitle={renderSinkDescription()}>
            <InputDiv>
                <Space direction="vertical" style={{ width: '100%' }}>
                    {!editing ? (
                        <>
                            <Space direction="horizontal">
                                <StyledLabel
                                    disabled={!isTeamsEnabled}
                                    style={{ marginRight: '16px', display: 'inline-block' }}
                                >
                                    {selectedOption?.displayName || selectedValue || 'None'}
                                </StyledLabel>
                                <Tooltip
                                    title={!isTeamsEnabled ? 'Teams notifications are currently disabled.' : undefined}
                                >
                                    <Button
                                        variant="outline"
                                        disabled={!isTeamsEnabled || editing}
                                        onClick={() => setEditing(true)}
                                        data-testid="teams-notifications-edit-button"
                                    >
                                        Edit
                                    </Button>
                                </Tooltip>
                            </Space>

                            {isTeamsEnabled && hasChannel && (
                                <TestNotificationButton
                                    integration="teams"
                                    connectionUrn={TEAMS_CONNECTION_URN}
                                    destinationSettings={{
                                        channels: [
                                            {
                                                id: selectedValue || '',
                                                name: selectedOption?.displayName || selectedValue || '',
                                            },
                                        ],
                                    }}
                                />
                            )}
                        </>
                    ) : (
                        <>
                            <Form.Item label="Search Channels">
                                <TeamsUserChannelSearch
                                    onSelectResult={handleSelectResult}
                                    selectedResult={selectedOption}
                                    supportedTypes="channels"
                                    placeholder="Search for Teams channels..."
                                    disabled={!isTeamsEnabled}
                                />
                            </Form.Item>

                            <SinkButtonsContainer>
                                <Button
                                    onClick={onSave}
                                    data-testid="teams-notifications-save-button"
                                    disabled={!selectedValue}
                                >
                                    Save
                                </Button>
                                <Button
                                    variant="outline"
                                    color="gray"
                                    onClick={onCancel}
                                    data-testid="teams-notifications-cancel-button"
                                >
                                    Cancel
                                </Button>
                            </SinkButtonsContainer>

                            <HelperText>
                                Search and select a Teams channel. Ensure the DataHub bot has been added to the channel.
                            </HelperText>
                        </>
                    )}
                </Space>
            </InputDiv>
            {!isTeamsEnabled &&
                (isAdminAccess ? (
                    <MessageDiv>
                        Teams is currently disabled.&nbsp;
                        <Link
                            to="/settings/integrations/microsoft-teams"
                            style={{ color: theme?.styles['primary-color'] }}
                        >
                            Click here
                        </Link>{' '}
                        to setup the Teams integration.
                    </MessageDiv>
                ) : (
                    <MessageDiv>{unsupportedSinkDescription}</MessageDiv>
                ))}
        </Card>
    );
};
