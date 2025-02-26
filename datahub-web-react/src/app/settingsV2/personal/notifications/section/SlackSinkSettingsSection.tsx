import React, { useEffect, useRef, useState } from 'react';
import { MoreOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { Button, colors, ToggleCard } from '@components';
import { Form } from 'antd';
import { useUserContext } from '@src/app/context/useUserContext';
import { TestNotificationButton } from '@src/app/shared/notifications/TestNotificationButton';
import { SLACK_CONNECTION_URN } from '@src/app/settingsV2/platform/slack/constants';
import { SlackNotificationSettings, SlackNotificationSettingsInput } from '../../../../../types.generated';
import { getSlackSettingsChannel } from '../../../../shared/subscribe/drawer/utils';
import {
    CancelButton,
    SaveButton,
    SinkButtonsContainer,
    SinkConfigurationContainer,
    StyledFormItem,
    StyledInput,
} from './styledComponents';

const HelperText = styled.div`
    color: ${colors.gray[1700]};
    margin-top: 6px;
    font-size: 12px;
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
                    Slack is currently disabled.&nbsp;
                    <Link to="/settings/integrations/slack" style={{ color: colors.violet['500'] }}>
                        Click here
                    </Link>{' '}
                    to setup the Slack integration.
                </>
            );
        }
        if (!sinkSupported && !isAdminAccess) {
            description = <>{unsupportedSinkDescription}</>;
        }
        return description;
    };

    return (
        <ToggleCard
            title="Slack Notifications"
            subTitle={renderSinkDescription()}
            disabled={!sinkSupported}
            value={sinkEnabled}
            onToggle={toggleSink}
            toggleDataTestId="slack-notifications-enabled-switch"
        >
            {sinkEnabled && (
                <SinkConfigurationContainer>
                    {inputValue && !editing && (
                        <strong>
                            <br /> {inputValue || 'No Slack channel or id set.'}
                        </strong>
                    )}
                    {!editing && (
                        <Button
                            variant="text"
                            onClick={() => setIsEditing(true)}
                            data-testid="email-notifications-edit-slack-button"
                        >
                            Edit
                        </Button>
                    )}
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
                                <SaveButton
                                    onClick={saveButtonOnClick}
                                    data-testid="email-notifications-save-slack-button"
                                >
                                    Save
                                </SaveButton>
                                <CancelButton
                                    variant="outline"
                                    color="gray"
                                    onClick={cancelButtonOnClick}
                                    data-testid="email-notifications-cancel-slack-button"
                                >
                                    Cancel
                                </CancelButton>
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
                                        If this is a private channel, ensure the DataHub Slack bot has been added to it.
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
                </SinkConfigurationContainer>
            )}
        </ToggleCard>
    );
};
