import { MoreOutlined } from '@ant-design/icons';
import { Button, Input, Text, colors } from '@components';
import { Form, message } from 'antd';
import { PencilSimple } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { SLACK_CONNECTION_URN } from '@app/settings/platform/slack/constants';
import { isSlackSinkSupported as isSlackSinkSupportedGlobally } from '@app/settings/utils';
import { TestNotificationButton } from '@app/shared/notifications/TestNotificationButton';
import { useAppConfig } from '@app/useAppConfig';

import { GetGlobalSettingsQuery } from '@graphql/settings.generated';
import { NotificationSinkType, SlackNotificationSettingsInput } from '@types';

const SinkContainer = styled.div`
    margin-bottom: 16px;

    &:last-child {
        margin-bottom: 0;
    }
`;

const SinkTypeText = styled(Text)`
    margin-bottom: 0px;
`;

const InputSection = styled.div`
    margin-top: 0px;
    flex: 1;
    margin-left: 12px;
`;

const InlineContainer = styled.div`
    display: flex;
    align-items: center;
`;

const InputWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledInput = styled(Input)`
    width: 200px;
`;

const SaveButton = styled(Button)`
    margin-left: 8px;
`;

const EditText = styled(Button)``;

const DisabledText = styled(Text)`
    margin-top: 8px;
    font-size: 14px;
    color: ${colors.gray[500]};
`;

const ButtonActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 0;
`;

const BulletDivider = styled.span`
    display: inline-block;
    width: 4px;
    height: 4px;
    border-radius: 50%;
    background-color: ${colors.gray[200]};
`;

const PlaceholderValue = styled(Text)`
    font-weight: 400;
    color: ${colors.gray[400]};
    font-style: italic;
`;

const InstructionsText = styled(Text)`
    font-size: 12px;
    color: ${colors.gray[500]};
    margin-top: 8px;
    display: block;

    a {
        color: ${REDESIGN_COLORS.BLUE};
        text-decoration: none;

        &:hover {
            text-decoration: underline;
        }
    }
`;

interface Props {
    isPersonal: boolean;
    slackSettings?: SlackNotificationSettingsInput;
    sinkTypes?: NotificationSinkType[];
    globalSettings?: GetGlobalSettingsQuery;
    onUpdateSinkSettings: (input: {
        sinkTypes: NotificationSinkType[];
        slackSettings?: SlackNotificationSettingsInput;
    }) => Promise<void>;
}

export const SlackNotificationSettings: React.FC<Props> = ({
    isPersonal,
    slackSettings,
    sinkTypes,
    globalSettings,
    onUpdateSinkSettings,
}) => {
    const { config } = useAppConfig();
    const userContext = useUserContext();
    const [form] = Form.useForm();

    // Local state for editing
    const [isSlackEditing, setIsSlackEditing] = useState(false);
    const [slackValue, setSlackValue] = useState('');

    // Check if slack sink is enabled globally
    const isSlackSinkSupported = isSlackSinkSupportedGlobally(globalSettings?.globalSettings, config);

    const isSlackCurrentlyEnabled = sinkTypes?.includes(NotificationSinkType.Slack);

    // Admin access check
    const isAdminAccess = userContext?.platformPrivileges?.manageGlobalSettings || false;

    const toggleSlack = (enabled: boolean) => {
        if (enabled) {
            onUpdateSinkSettings({
                sinkTypes: [...(sinkTypes || []), NotificationSinkType.Slack],
                slackSettings: isPersonal ? { userHandle: slackValue } : { channels: [slackValue] },
            }).then(() => {
                message.success('Slack notifications enabled');
            });
        } else {
            onUpdateSinkSettings({
                sinkTypes: sinkTypes?.filter((type) => type !== NotificationSinkType.Slack) || [],
                slackSettings: isPersonal ? { userHandle: slackValue } : { channels: [slackValue] },
            }).then(() => {
                message.success('Slack notifications disabled');
            });
        }
    };

    // Initialize local state from settings and automatically enable slack if slack exists
    useEffect(() => {
        const currentSlackValue = slackSettings?.userHandle || slackSettings?.channels?.[0];
        if (currentSlackValue) {
            setSlackValue(currentSlackValue);
        }
    }, [slackSettings, isSlackSinkSupported, isSlackCurrentlyEnabled, sinkTypes, onUpdateSinkSettings, isPersonal]);

    const handleSlackSave = () => {
        const trimmedSlackValue = slackValue.trim();
        const newSinkTypes = trimmedSlackValue
            ? [...(sinkTypes?.filter((type) => type !== NotificationSinkType.Slack) || []), NotificationSinkType.Slack]
            : (sinkTypes || []).filter((type) => type !== NotificationSinkType.Slack);

        let updatedSlackSettings;
        if (trimmedSlackValue) {
            updatedSlackSettings = isPersonal ? { userHandle: trimmedSlackValue } : { channels: [trimmedSlackValue] };
        } else {
            updatedSlackSettings = undefined;
        }

        onUpdateSinkSettings({
            sinkTypes: newSinkTypes,
            slackSettings: updatedSlackSettings,
        });

        // Update local state with trimmed value
        setSlackValue(trimmedSlackValue);
        setIsSlackEditing(false);
    };

    const handleSlackCancel = () => {
        setIsSlackEditing(false);
        const currentValue = isPersonal ? slackSettings?.userHandle : slackSettings?.channels?.[0];
        setSlackValue(currentValue || '');
    };

    // Get current slack destination for test button
    const currentSlackDestination = isPersonal ? slackSettings?.userHandle : slackSettings?.channels?.[0];

    return (
        <SinkContainer>
            {isSlackSinkSupported ? (
                <>
                    <InlineContainer>
                        <SinkTypeText size="md" color="gray" colorLevel={700} weight="semiBold">
                            Slack
                        </SinkTypeText>
                        <InputSection>
                            {isSlackEditing ? (
                                <Form form={form}>
                                    <InputWrapper>
                                        <StyledInput
                                            label=""
                                            value={slackValue}
                                            setValue={setSlackValue}
                                            placeholder={isPersonal ? 'Member ID' : '#channel-name'}
                                        />
                                        <SaveButton
                                            variant="filled"
                                            size="sm"
                                            onClick={handleSlackSave}
                                            disabled={!slackValue.trim()}
                                        >
                                            Save
                                        </SaveButton>
                                        <Button size="sm" onClick={handleSlackCancel} variant="outline">
                                            Cancel
                                        </Button>
                                    </InputWrapper>
                                </Form>
                            ) : (
                                <InputWrapper>
                                    {currentSlackDestination ? (
                                        <Text size="md" color="gray" colorLevel={600} weight="medium">
                                            {currentSlackDestination}
                                        </Text>
                                    ) : (
                                        <PlaceholderValue>
                                            {isPersonal ? 'Member ID' : '#channel-name'}
                                        </PlaceholderValue>
                                    )}
                                    <EditText
                                        variant={currentSlackDestination ? 'link' : 'text'}
                                        onClick={() => setIsSlackEditing(true)}
                                        color={currentSlackDestination ? 'black' : 'primary'}
                                    >
                                        {currentSlackDestination ? <PencilSimple size={14} /> : 'Add'}
                                    </EditText>
                                    {/* Toggle sink */}
                                    {currentSlackDestination && [
                                        <BulletDivider />,
                                        <Button
                                            variant="text"
                                            onClick={() => {
                                                toggleSlack(!isSlackCurrentlyEnabled);
                                            }}
                                            color={isSlackCurrentlyEnabled ? 'red' : 'primary'}
                                        >
                                            {isSlackCurrentlyEnabled ? 'Disable' : 'Enable'}
                                        </Button>,
                                    ]}
                                </InputWrapper>
                            )}
                        </InputSection>
                    </InlineContainer>

                    {isPersonal && isSlackEditing && (
                        <InstructionsText>
                            Find a member ID from the <MoreOutlined /> menu in your Slack profile.
                            <a
                                target="_blank"
                                rel="noreferrer"
                                href="https://datahubproject.io/docs/managed-datahub/slack/saas-slack-setup/#how-to-find-user-id-in-slack"
                            >
                                {' '}
                                See instructions.
                            </a>
                        </InstructionsText>
                    )}

                    {slackValue && isSlackEditing && (
                        <ButtonActionsContainer>
                            {/* Test Notification Button for Slack */}
                            <TestNotificationButton
                                integration="slack"
                                connectionUrn={SLACK_CONNECTION_URN}
                                destinationSettings={
                                    isPersonal ? { userHandle: slackValue || '' } : { channels: [slackValue] }
                                }
                            />
                        </ButtonActionsContainer>
                    )}
                </>
            ) : (
                <>
                    <SinkTypeText size="md" color="gray" colorLevel={700} weight="semiBold">
                        Slack
                    </SinkTypeText>
                    <DisabledText>
                        {isAdminAccess ? (
                            <>
                                Slack notifications are disabled. In order to enable,{' '}
                                <Link to="/settings/integrations/slack" style={{ color: REDESIGN_COLORS.BLUE }}>
                                    setup a Slack integration.
                                </Link>
                            </>
                        ) : (
                            'Slack notifications are disabled. Reach out to your DataHub admins for more information.'
                        )}
                    </DisabledText>
                </>
            )}
        </SinkContainer>
    );
};
