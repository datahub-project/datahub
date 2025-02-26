import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Button, colors, ToggleCard } from '@components';
import { Form } from 'antd';
import { EmailNotificationSettings, EmailNotificationSettingsInput } from '../../../../../types.generated';
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
    font-size: 14px;
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
    sinkEnabled: boolean;
    sinkSupported: boolean;
    updateSinkSetting: (input?: EmailNotificationSettingsInput) => void;
    toggleSink: (enabled: boolean) => void;
    settings?: EmailNotificationSettings;
    groupName?: string;
};

/**
 * Personal or Group Email settings section
 */
export const EmailSinkSettingsSection = ({
    isPersonal,
    sinkSupported,
    sinkEnabled,
    settings,
    updateSinkSetting,
    toggleSink,
    groupName,
}: Props) => {
    const email = settings?.email;
    const [editing, setIsEditing] = useState<boolean>(false);
    const [inputValue, setInputValue] = useState(email);
    const [form] = Form.useForm();

    form.setFieldsValue({ emailFormValue: inputValue });

    useEffect(() => {
        if (!email) {
            setIsEditing(true);
        } else {
            setIsEditing(false);
        }
        setInputValue(email);
    }, [email]);

    const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
    const supportedSinkDescription = `Receive Email notifications for assets ${actorDescription} subscribed to & important events.`;
    const unsupportedSinkDescription = `In order to enable, ask your DataHub admin to enable Email notifications${
        !isPersonal ? ' and ensure you have permission to manage notifications for this group' : ''
    }.`;

    const saveSettings = () => {
        const input = inputValue ? { email: inputValue } : undefined;
        updateSinkSetting(input);
    };

    const saveButtonOnClick = () => {
        if (inputValue) {
            saveSettings();
        }
        setIsEditing(false);
    };

    const cancelButtonOnClick = () => {
        setInputValue(email);
        setIsEditing(false);
    };

    return (
        <ToggleCard
            title="Email Notifications"
            subTitle={sinkSupported ? supportedSinkDescription : unsupportedSinkDescription}
            disabled={!sinkSupported}
            value={sinkEnabled}
            onToggle={toggleSink}
            toggleDataTestId="email-notifications-enabled-switch"
        >
            {sinkEnabled ? (
                <SinkConfigurationContainer>
                    {sinkEnabled && inputValue && !editing && (
                        <CurrentValue>
                            <strong>{inputValue}</strong>
                            <Button
                                variant="text"
                                onClick={() => setIsEditing(true)}
                                data-testid="email-notifications-edit-email-button"
                            >
                                Edit
                            </Button>
                        </CurrentValue>
                    )}
                    {editing && (
                        <>
                            <SinkButtonsContainer>
                                <Form form={form}>
                                    <StyledFormItem name="emailFormValue">
                                        <StyledInput
                                            placeholder="Email Address"
                                            value={inputValue}
                                            onChange={(e) => setInputValue(e.target.value)}
                                            data-testid="email-notifications-edit-email-input"
                                        />
                                    </StyledFormItem>
                                </Form>
                                <SaveButton
                                    onClick={saveButtonOnClick}
                                    disabled={!inputValue || inputValue.length < 2}
                                    data-testid="email-notifications-save-email-button"
                                >
                                    Save
                                </SaveButton>
                                <CancelButton
                                    variant="outline"
                                    color="gray"
                                    onClick={cancelButtonOnClick}
                                    data-testid="email-notifications-cancel-email-button"
                                >
                                    Cancel
                                </CancelButton>
                            </SinkButtonsContainer>
                            <HelperText>The email address where notifications will be sent.</HelperText>
                        </>
                    )}
                </SinkConfigurationContainer>
            ) : null}
        </ToggleCard>
    );
};
