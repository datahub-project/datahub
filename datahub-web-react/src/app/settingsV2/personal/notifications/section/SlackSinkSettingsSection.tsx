import { MoreOutlined } from '@ant-design/icons';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Typography, Switch, Input, Button, Form } from 'antd';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { SlackNotificationSettings, SlackNotificationSettingsInput } from '../../../../../types.generated';
import { getSlackSettingsChannel } from '../../../../shared/subscribe/drawer/utils';

const SinkSettings = styled.div`
    margin-top: 12px;
    display: flex;
    flex-direction: row;
    align-items: start;
`;

const SinkTextContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding-left: 14px;
    gap: 4px;
`;

const SinkTitle = styled(Typography.Text)`
    font-size: 16px;
`;

const SinkDescription = styled(Typography.Text)`
    font-size: 14px;
`;

const SinkEditButton = styled(Button)`
    padding: 0 4px;
`;

const EditDescription = styled(Typography.Text)`
    text-decoration: underline;
    color: ${(props) => props.theme.styles['primary-color']};
`;

const SinkButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
    align-items: start;
    margin-top: 8px;
    gap: 2px;
`;

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 0px;
`;

const StyledInput = styled(Input)`
    width: 220px;
    border-color: ${ANTD_GRAY[8]};
`;

const SaveButton = styled(Button)`
    margin: 0 4px;
`;

const CancelButton = styled(Button)``;

const HelperText = styled.div`
    color: ${ANTD_GRAY[8]};
    margin-top: 6px;
    font-size: 14px;
`;

type Props = {
    isPersonal: boolean;
    sinkSupported: boolean;
    sinkEnabled: boolean;
    updateSinkSetting: (input?: SlackNotificationSettingsInput) => void;
    toggleSink: (enabled: boolean) => void;
    settings?: SlackNotificationSettings;
    groupName?: string;
};

/**
 * Personal or Group Slack settings section
 */
export const SlackSinkSettingsSection = ({
    isPersonal,
    sinkSupported, // Whether this sink is supported. If not, the user will not be able to enable it.
    sinkEnabled,
    settings,
    updateSinkSetting,
    toggleSink,
    groupName,
}: Props) => {
    const channelOrUserId = getSlackSettingsChannel(isPersonal, settings);
    const [editing, setIsEditing] = useState<boolean>(false);
    const [inputValue, setInputValue] = useState(channelOrUserId);
    const [form] = Form.useForm();

    form.setFieldsValue({ slackFormValue: inputValue });

    useEffect(() => {
        if (!channelOrUserId) {
            setIsEditing(true);
        } else {
            setIsEditing(false);
        }
        setInputValue(channelOrUserId);
    }, [channelOrUserId]);

    const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
    const supportedSinkDescription = `Receive Slack notifications for entities ${actorDescription} subscribed to at Slack ${
        isPersonal ? 'member' : 'channel'
    } ID: `;
    const unsupportedSinkDescription = `In order to enable, ask your DataHub admin to setup the Slack integration.`;
    const slackInputPlaceholder = isPersonal ? 'Slack Member ID' : 'Slack Channel ID';

    const saveSettings = () => {
        const input = isPersonal ? { userHandle: inputValue } : { channels: inputValue ? [inputValue] : [] };
        updateSinkSetting(input);
    };

    const saveButtonOnClick = () => {
        if (inputValue) {
            saveSettings();
        }
        setIsEditing(false);
    };

    const cancelButtonOnClick = () => {
        setInputValue(channelOrUserId);
        setIsEditing(false);
    };

    const onToggle = (enabled: boolean) => {
        toggleSink(enabled);
    };

    return (
        <SinkSettings>
            <Switch disabled={!sinkSupported} checked={sinkEnabled} onChange={onToggle} />
            <SinkTextContainer>
                <SinkTitle strong>Slack Notifications</SinkTitle>
                <SinkDescription>
                    {sinkSupported ? supportedSinkDescription : unsupportedSinkDescription}
                    {sinkEnabled && inputValue && !editing && (
                        <>
                            <strong>{inputValue}</strong>
                            <SinkEditButton type="link" onClick={() => setIsEditing(true)}>
                                <EditDescription strong>edit</EditDescription>
                            </SinkEditButton>
                        </>
                    )}
                </SinkDescription>
                {sinkEnabled && (
                    <>
                        <SinkButtonsContainer>
                            {editing && (
                                <>
                                    <Form form={form}>
                                        <StyledFormItem name="slackFormValue">
                                            <StyledInput
                                                placeholder={slackInputPlaceholder}
                                                value={inputValue}
                                                onChange={(e) => setInputValue(e.target.value)}
                                            />
                                        </StyledFormItem>
                                    </Form>
                                    <SaveButton
                                        type="primary"
                                        onClick={saveButtonOnClick}
                                        disabled={!inputValue || inputValue.length < 2}
                                    >
                                        Save
                                    </SaveButton>
                                    <CancelButton onClick={cancelButtonOnClick}>Cancel</CancelButton>
                                </>
                            )}
                        </SinkButtonsContainer>
                        {editing && (
                            <HelperText>
                                {isPersonal ? (
                                    <>
                                        Find a member ID from the <MoreOutlined /> menu in your Slack profile
                                    </>
                                ) : (
                                    <>Find a channel ID at the bottom of the &quot;About&quot; tab for a channel</>
                                )}
                            </HelperText>
                        )}
                    </>
                )}
            </SinkTextContainer>
        </SinkSettings>
    );
};
