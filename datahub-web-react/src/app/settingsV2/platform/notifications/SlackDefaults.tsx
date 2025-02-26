import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { Typography, Input, Space } from 'antd';
import { Button, Card, colors, Tooltip } from '@components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { TestNotificationButton } from '@src/app/shared/notifications/TestNotificationButton';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useUserContext } from '@src/app/context/useUserContext';
import { SLACK_CONNECTION_URN } from '../slack/constants';

const InputDiv = styled.div`
    width: 360px;
`;

const MessageDiv = styled.div`
    margin-top: 5px;
    color: ${colors.gray[1700]};
    font-size: 14px;
`;

const StyledInput = styled(Input)`
    max-width: 271px;
    font-size: 14px;
    .ant-input {
        font-size: 14px;
    }
`;

const StyledLabel = styled(Typography.Text)`
    display: inline-block;
    font-size: 14px;
`;

const StyledButton = styled(Button)``;

const StyledInfoCircleOutlined = styled(InfoCircleOutlined)<{ $isChannelUpdated: boolean }>`
    margin-right: 5px;
    color: ${(props) => (props.$isChannelUpdated ? REDESIGN_COLORS.RED_NORMAL : ANTD_GRAY[7])};
`;

const SlackChannelLabel = styled(Typography.Paragraph)`
    margin-top: 5px;
`;

const SlackChannelLabelText = styled(Typography.Text)<{ $isChannelUpdated: boolean }>`
    color: ${(props) => (props.$isChannelUpdated ? REDESIGN_COLORS.RED_NORMAL : ANTD_GRAY[7])};
`;

interface Props {
    isSlackEnabled?: boolean;
    channel?: string;
    onChange: (newChannel: string | undefined) => void;
    botToken?: string;
}

export const SlackDefaults = ({ isSlackEnabled = false, channel, onChange, botToken }: Props) => {
    const hasChannel = !!channel;
    const [editing, setEditing] = useState<boolean>(isSlackEnabled && !hasChannel);
    const [inputValue, setInputValue] = useState<string | undefined>(channel);
    const [isChannelUpdated, setIsChannelUpdated] = useState<boolean>(false);
    const me = useUserContext();
    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;
    const unsupportedSinkDescription = `In order to enable, ask your DataHub admin to setup the Slack integration.`;

    useEffect(() => {
        setEditing(isSlackEnabled && !hasChannel);
    }, [hasChannel, isSlackEnabled]);

    useEffect(() => {
        setInputValue(channel);
    }, [channel]);

    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setInputValue(e.target.value);
        setIsChannelUpdated(e.target.value !== channel);
    };

    const onSave = async () => {
        const sanitizedValue = inputValue?.trim()?.replace(/^#+/, ''); // Remove leading # and trim spaces
        onChange(sanitizedValue || '');
        setEditing(false);
        setIsChannelUpdated(false);
    };

    return (
        <Card title="Slack Notifications" subTitle="Receive Slack notifications when important changes occur.">
            <InputDiv>
                <Space direction="horizontal">
                    {!editing ? (
                        <>
                            <StyledLabel
                                disabled={!isSlackEnabled}
                                style={{ marginRight: '16px', display: 'inline-block' }}
                            >
                                {channel ? `#${channel}` : 'None'}
                            </StyledLabel>
                            <Tooltip
                                title={!isSlackEnabled ? 'Slack notifications are currently disabled.' : undefined}
                            >
                                <Button
                                    variant="outline"
                                    disabled={!isSlackEnabled || editing}
                                    onClick={() => setEditing(true)}
                                >
                                    Edit
                                </Button>
                            </Tooltip>
                        </>
                    ) : (
                        <>
                            <StyledInput
                                placeholder="data-notifications"
                                addonBefore="#"
                                value={inputValue}
                                disabled={!isSlackEnabled}
                                onChange={handleInputChange}
                                onKeyPress={(e) => {
                                    if (e.key === 'Enter') {
                                        onSave();
                                    }
                                }}
                            />
                            <StyledButton onClick={onSave} disabled={!isSlackEnabled}>
                                Save
                            </StyledButton>
                        </>
                    )}
                </Space>
                {editing && (
                    <Space direction="vertical">
                        <SlackChannelLabel>
                            <StyledInfoCircleOutlined $isChannelUpdated={isChannelUpdated} />
                            <SlackChannelLabelText $isChannelUpdated={isChannelUpdated}>
                                Please ensure the slack bot has been added to this channel
                            </SlackChannelLabelText>
                        </SlackChannelLabel>
                    </Space>
                )}
            </InputDiv>
            <TestNotificationButton
                hidden={!isSlackEnabled || !inputValue?.length}
                integration="slack"
                connectionUrn={SLACK_CONNECTION_URN}
                destinationSettings={{
                    channels: [inputValue ?? ''],
                }}
            />
            {!botToken &&
                (isAdminAccess ? (
                    <MessageDiv>
                        Slack is currently disabled.&nbsp;
                        <Link to="/settings/integrations/slack" style={{ color: REDESIGN_COLORS.BLUE }}>
                            click here to setup the Slack integration
                        </Link>
                    </MessageDiv>
                ) : (
                    <MessageDiv>{unsupportedSinkDescription}</MessageDiv>
                ))}
        </Card>
    );
};
