import { MoreOutlined } from '@ant-design/icons';
import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { Typography, Switch, Input, Button, Form } from 'antd';
import { useUserContext } from '@src/app/context/useUserContext';
import { TestNotificationButton } from '@src/app/shared/notifications/TestNotificationButton';
import { SLACK_CONNECTION_URN } from '@src/app/settings/platform/slack/constants';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../entity/shared/constants';
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
    const me = useUserContext();

    form.setFieldsValue({ slackFormValue: inputValue });

    useEffect(() => {
        setInputValue(channelOrUserId);
    }, [channelOrUserId]);

    const hasSetEditingRef = useRef(false);
    const hasComponentMountedRef = useRef(false);
    useEffect(() => {
        if (!hasComponentMountedRef.current) {
            hasComponentMountedRef.current = true;
            return;
        }
        if (!hasSetEditingRef.current) {
            setIsEditing(!channelOrUserId && sinkEnabled);
            hasSetEditingRef.current = true;
        }
    }, [channelOrUserId, sinkEnabled]);

    const slackInputPlaceholder = isPersonal ? 'Slack Member ID' : 'Slack Channel';

    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;

    const saveSettings = () => {
        const input = isPersonal ? { userHandle: inputValue } : { channels: inputValue ? [inputValue] : [] };
        updateSinkSetting(input);
    };

    const saveButtonOnClick = () => {
        saveSettings();
        setIsEditing(false);
    };

    const cancelButtonOnClick = () => {
        setInputValue(channelOrUserId);
        setIsEditing(false);
    };

    const onToggle = (enabled: boolean) => {
        toggleSink(enabled);
        setIsEditing(enabled && !channelOrUserId);
    };

    const renderSinkDescription = () => {
        const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
        const supportedSinkDescription = `Receive Slack notifications for entities ${actorDescription} subscribed to at Slack ${
            isPersonal ? 'member ID' : 'channel'
        }: `;
        const unsupportedSinkDescription = `In order to enable, ask your DataHub admin to setup the Slack integration.`;

        let description = <>{supportedSinkDescription}</>;
        if (!sinkSupported && isAdminAccess) {
            description = (
                <>
                    In order to enable,&nbsp;
                    <Link to="/settings/integrations/slack" style={{ color: REDESIGN_COLORS.BLUE }}>
                        click here to setup a Slack integration
                    </Link>
                </>
            );
        }
        if (!sinkSupported && !isAdminAccess) {
            description = <>{unsupportedSinkDescription}</>;
        }
        return description;
    };

    return (
        <SinkSettings>
            <Switch disabled={!sinkSupported} checked={sinkEnabled} onChange={onToggle} />
            <SinkTextContainer>
                <SinkTitle strong>Slack Notifications</SinkTitle>
                <SinkDescription>
                    {renderSinkDescription()}
                    {!editing && (
                        <strong>
                            <br /> {inputValue || 'Not set.'}
                        </strong>
                    )}
                    {sinkEnabled && !editing && (
                        <>
                            <SinkEditButton type="link" onClick={() => setIsEditing(true)}>
                                <EditDescription strong>edit</EditDescription>
                            </SinkEditButton>
                        </>
                    )}
                </SinkDescription>
                {sinkEnabled && (
                    <>
                        {editing ? (
                            <>
                                <SinkButtonsContainer>
                                    <Form form={form}>
                                        <StyledFormItem name="slackFormValue">
                                            <StyledInput
                                                placeholder={slackInputPlaceholder}
                                                value={inputValue}
                                                onChange={(e) => setInputValue(e.target.value)}
                                            />
                                        </StyledFormItem>
                                    </Form>
                                    <SaveButton type="primary" onClick={saveButtonOnClick}>
                                        Save
                                    </SaveButton>
                                    <CancelButton onClick={cancelButtonOnClick}>Cancel</CancelButton>
                                </SinkButtonsContainer>
                                <HelperText>
                                    {isPersonal ? (
                                        <>
                                            Find a member ID from the <MoreOutlined /> menu in your Slack profile.
                                            <a
                                                target="_blank"
                                                rel="noreferrer"
                                                href="https://datahubproject.io/docs/managed-datahub/slack/saas-slack-setup/#how-to-find-user-id-in-slack"
                                            >
                                                {' '}
                                                See instructions.
                                            </a>
                                        </>
                                    ) : (
                                        <>
                                            If this is a private channel, ensure the DataHub Slack bot has been added to
                                            it.
                                        </>
                                    )}
                                </HelperText>
                            </>
                        ) : null}
                        {inputValue && (
                            <TestNotificationButton
                                integration="slack"
                                connectionUrn={SLACK_CONNECTION_URN}
                                destinationSettings={
                                    isPersonal ? { userHandle: inputValue || '' } : { channels: [inputValue || ''] }
                                }
                            />
                        )}
                    </>
                )}
            </SinkTextContainer>
        </SinkSettings>
    );
};
