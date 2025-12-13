import { Button, Input, Text, colors } from '@components';
import { Form, message } from 'antd';
import { PencilSimple } from 'phosphor-react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { isEmailSinkSupportedGlobally } from '@app/settings/utils';
import { useAppConfig } from '@app/useAppConfig';

import { GetGlobalSettingsQuery } from '@graphql/settings.generated';
import { EmailNotificationSettingsInput, NotificationSinkType } from '@types';

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
    margin-top: 0;
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

const DisabledText = styled(Text)`
    margin-left: 0;
    margin-top: 8px;
    font-size: 14px;
    color: ${colors.gray[500]};
`;

const PlaceholderValue = styled(Text)`
    font-weight: 400;
    color: ${colors.gray[400]};
    font-style: italic;
`;

const BulletDivider = styled.span`
    display: inline-block;
    width: 4px;
    height: 4px;
    border-radius: 50%;
    background-color: ${colors.gray[200]};
`;

interface Props {
    isPersonal: boolean;
    emailSettings?: EmailNotificationSettingsInput;
    sinkTypes?: NotificationSinkType[];
    globalSettings?: GetGlobalSettingsQuery;
    onUpdateSinkSettings: (input: {
        sinkTypes: NotificationSinkType[];
        emailSettings?: EmailNotificationSettingsInput;
    }) => Promise<void>;
}

export const EmailNotificationSettings: React.FC<Props> = ({
    isPersonal,
    emailSettings,
    sinkTypes,
    globalSettings,
    onUpdateSinkSettings,
}) => {
    const { config } = useAppConfig();
    const [form] = Form.useForm();

    // Local state for editing
    const [isEmailEditing, setIsEmailEditing] = useState(false);
    const [emailValue, setEmailValue] = useState('');

    // Check if email sink is enabled globally
    const isEmailSinkSupported = isEmailSinkSupportedGlobally(globalSettings?.globalSettings, config);

    const isEmailCurrentlyEnabled = sinkTypes?.includes(NotificationSinkType.Email);

    const toggleEmail = (enabled: boolean) => {
        if (enabled) {
            onUpdateSinkSettings({
                sinkTypes: [...(sinkTypes || []), NotificationSinkType.Email],
                emailSettings: { email: emailValue },
            }).then(() => {
                message.success('Email notifications enabled');
            });
        } else {
            onUpdateSinkSettings({
                sinkTypes: sinkTypes?.filter((type) => type !== NotificationSinkType.Email) || [],
                emailSettings: { email: emailValue },
            }).then(() => {
                message.success('Email notifications disabled');
            });
        }
    };

    // Initialize local state from settings and automatically enable email if email exists
    useEffect(() => {
        if (emailSettings?.email) {
            setEmailValue(emailSettings.email);
        }
    }, [emailSettings, isEmailSinkSupported, isEmailCurrentlyEnabled, sinkTypes, onUpdateSinkSettings]);

    const handleEmailSave = () => {
        const trimmedEmail = emailValue.trim();
        const newSinkTypes = trimmedEmail
            ? [...(sinkTypes?.filter((type) => type !== NotificationSinkType.Email) || []), NotificationSinkType.Email]
            : (sinkTypes || []).filter((type) => type !== NotificationSinkType.Email);

        onUpdateSinkSettings({
            sinkTypes: newSinkTypes,
            emailSettings: trimmedEmail ? { email: trimmedEmail } : undefined,
        });
        setIsEmailEditing(false);
    };

    const handleEmailCancel = () => {
        setIsEmailEditing(false);
        setEmailValue(emailSettings?.email || '');
    };

    return (
        <SinkContainer>
            {isEmailSinkSupported ? (
                <InlineContainer>
                    <SinkTypeText size="md" color="gray" colorLevel={700} weight="semiBold">
                        Email
                    </SinkTypeText>
                    <InputSection>
                        {isEmailEditing ? (
                            <Form form={form}>
                                <InputWrapper>
                                    <StyledInput
                                        label=""
                                        value={emailValue}
                                        setValue={setEmailValue}
                                        placeholder={isPersonal ? 'your@email.com' : 'group@email.com'}
                                    />
                                    <SaveButton
                                        variant="filled"
                                        size="sm"
                                        onClick={handleEmailSave}
                                        disabled={!emailValue.trim()}
                                    >
                                        Save
                                    </SaveButton>
                                    <Button size="sm" onClick={handleEmailCancel} variant="outline">
                                        Cancel
                                    </Button>
                                </InputWrapper>
                            </Form>
                        ) : (
                            <InputWrapper>
                                {emailSettings?.email ? (
                                    <Text size="md" color="gray" colorLevel={600} weight="medium">
                                        {emailSettings.email}
                                    </Text>
                                ) : (
                                    <PlaceholderValue>
                                        {isPersonal ? 'your@email.com' : 'group@email.com'}
                                    </PlaceholderValue>
                                )}
                                <Button
                                    variant={emailSettings?.email ? 'link' : 'text'}
                                    color={emailSettings?.email ? 'black' : 'primary'}
                                    onClick={() => {
                                        setIsEmailEditing(true);
                                    }}
                                >
                                    {emailSettings?.email ? <PencilSimple size={14} /> : 'Add'}
                                </Button>

                                {/* Toggle sink */}
                                {emailSettings?.email && [
                                    <BulletDivider />,
                                    <Button
                                        variant="text"
                                        onClick={() => {
                                            toggleEmail(!isEmailCurrentlyEnabled);
                                        }}
                                        color={isEmailCurrentlyEnabled ? 'red' : 'primary'}
                                    >
                                        {isEmailCurrentlyEnabled ? 'Disable' : 'Enable'}
                                    </Button>,
                                ]}
                            </InputWrapper>
                        )}
                    </InputSection>
                </InlineContainer>
            ) : (
                <>
                    <SinkTypeText size="md" color="gray" colorLevel={700} weight="semiBold">
                        Email
                    </SinkTypeText>
                    <DisabledText>
                        Email notifications are disabled. Reach out to your DataHub admins for more information.
                    </DisabledText>
                </>
            )}
        </SinkContainer>
    );
};
