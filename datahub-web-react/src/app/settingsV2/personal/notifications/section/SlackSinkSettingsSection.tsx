import { Button, ToggleCard, colors } from '@components';
import { Form } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useSlackOAuth } from '@app/settingsV2/personal/notifications/hooks/useSlackOAuth';
import { SlackLegacyManualInput } from '@app/settingsV2/personal/notifications/section/SlackLegacyManualInput';
import { SlackOAuthConnection } from '@app/settingsV2/personal/notifications/section/SlackOAuthConnection';
import {
    CancelButton,
    SaveButton,
    SinkButtonsContainer,
    SinkConfigurationContainer,
    StyledFormItem,
    StyledInput,
} from '@app/settingsV2/personal/notifications/section/styledComponents';
import { SLACK_CONNECTION_URN } from '@app/settingsV2/slack/utils';
import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import { getSlackSettingsChannel } from '@app/shared/subscribe/drawer/utils';
import { useAppConfig } from '@app/useAppConfig';
import { useUserContext } from '@src/app/context/useUserContext';
import { SlackNotificationSettings, SlackNotificationSettingsInput } from '@src/types.generated';

const HelperText = styled.div`
    color: ${colors.gray[1700]};
    margin-top: 6px;
    font-size: 12px;
`;

const CurrentValue = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 12px;
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

type Props = {
    isPersonal: boolean;
    sinkSupported: boolean;
    sinkEnabled: boolean;
    updateSinkSetting: (input?: SlackNotificationSettingsInput) => void;
    toggleSink: (enabled: boolean) => void;
    settings?: SlackNotificationSettings;
    groupName?: string;
    isSlackPlatformConfigured: boolean;
};

/**
 * Personal or Group Slack settings section.
 *
 * For personal notifications:
 * - OAuth mode (requireSlackOAuthBinding=true): Users must connect via OAuth
 * - Legacy mode (requireSlackOAuthBinding=false): Users manually enter Slack Member ID
 *
 * For group notifications: Edit/save Slack channel (original behavior preserved).
 */
export const SlackSinkSettingsSection = ({
    isPersonal,
    sinkSupported,
    sinkEnabled,
    settings,
    updateSinkSetting,
    toggleSink,
    groupName,
    isSlackPlatformConfigured,
}: Props) => {
    const { config } = useAppConfig();
    const me = useUserContext();

    const requireOAuthBinding = config?.featureFlags?.requireSlackOAuthBinding || false;

    const {
        startOAuthFlow,
        isLoading: isOAuthLoading,
        shouldAutoConnect,
    } = useSlackOAuth({
        isSlackPlatformConfigured,
        autoConnectEnabled: requireOAuthBinding && isPersonal,
    });

    // Track if we've handled auto-enable
    const hasAutoEnabledRef = useRef(false);

    useEffect(() => {
        if (
            shouldAutoConnect &&
            requireOAuthBinding &&
            isPersonal &&
            sinkSupported &&
            !sinkEnabled &&
            !hasAutoEnabledRef.current
        ) {
            hasAutoEnabledRef.current = true;
            toggleSink(true);
        }
    }, [shouldAutoConnect, requireOAuthBinding, isPersonal, sinkSupported, sinkEnabled, toggleSink]);

    // --- Group channel editing state ---
    const channelOrUserId = getSlackSettingsChannel(isPersonal, settings);
    const [editing, setIsEditing] = useState<boolean>(false);
    const [inputValue, setInputValue] = useState(channelOrUserId);
    const [form] = Form.useForm();

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
        if (!hasSetEditingRef.current && !isPersonal) {
            setIsEditing(!channelOrUserId && sinkEnabled);
            hasSetEditingRef.current = true;
        }
    }, [channelOrUserId, sinkEnabled, isPersonal]);

    const slackUser = isPersonal ? settings?.user : null;
    const legacyUserHandle = settings?.userHandle;
    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;
    const platformNotConfigured = !isSlackPlatformConfigured;

    const saveGroupSettings = () => {
        updateSinkSetting({ channels: inputValue ? [inputValue] : [] });
    };

    const saveButtonOnClick = () => {
        saveGroupSettings();
        setIsEditing(false);
    };

    const cancelButtonOnClick = () => {
        setInputValue(channelOrUserId);
        setIsEditing(false);
    };

    const renderSinkDescription = () => {
        const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
        const supportedSinkDescription = `Receive Slack notifications for entities ${actorDescription} subscribed to & important events.`;

        if (platformNotConfigured) {
            if (isAdminAccess) {
                return (
                    <>
                        Slack notifications are available, but the platform integration is not configured.&nbsp;
                        <Link to="/settings/integrations/slack" style={{ color: colors.violet['500'] }}>
                            Click here to set up Slack integration
                        </Link>{' '}
                        in Platform Settings.
                    </>
                );
            }
            return (
                <>
                    Slack notifications are available, but your admin needs to configure the platform integration first.
                    Ask your DataHub admin to set up Slack integration in Platform Settings.
                </>
            );
        }

        if (!sinkSupported && isAdminAccess) {
            return (
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
            return <>In order to enable, ask your DataHub admin to setup the Slack integration.</>;
        }

        return <>{supportedSinkDescription}</>;
    };

    const renderContent = () => {
        // --- Group notifications: channel edit/save UI (original behavior) ---
        if (!isPersonal) {
            return (
                <>
                    {!editing && (
                        <CurrentValue>
                            {inputValue ? <strong>{inputValue}</strong> : 'No Slack channel set.'}
                        </CurrentValue>
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
                                            placeholder="Slack Channel"
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
                                If this is a private channel, ensure the DataHub Slack bot has been added to it.
                            </HelperText>
                        </>
                    ) : null}
                    {inputValue && (
                        <TestNotificationButton
                            integration="slack"
                            connectionUrn={SLACK_CONNECTION_URN}
                            destinationSettings={{ channels: [inputValue || ''] }}
                        />
                    )}
                </>
            );
        }

        // --- Personal notifications: OAuth mode ---
        if (requireOAuthBinding) {
            return (
                <SlackOAuthConnection slackUser={slackUser} onConnect={startOAuthFlow} isConnecting={isOAuthLoading} />
            );
        }

        // --- Personal notifications: Legacy manual input mode ---
        return (
            <SlackLegacyManualInput
                userHandle={legacyUserHandle}
                sinkEnabled={sinkEnabled}
                updateSinkSetting={updateSinkSetting}
            />
        );
    };

    return (
        <ToggleCard
            title="Slack Notifications"
            subTitle={renderSinkDescription()}
            disabled={!sinkSupported || (isPersonal && platformNotConfigured)}
            value={sinkEnabled}
            onToggle={toggleSink}
            toggleDataTestId="slack-notifications-enabled-switch"
        >
            {sinkEnabled && <SinkConfigurationContainer>{renderContent()}</SinkConfigurationContainer>}
        </ToggleCard>
    );
};
