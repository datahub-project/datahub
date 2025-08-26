import { Button, Form, Input, Switch, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';

import { EmailNotificationSettings, EmailNotificationSettingsInput } from '@types';

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
    const supportedSinkDescription = `Receive Email notifications for entities ${actorDescription} subscribed to.`;
    const unsupportedSinkDescription = `In order to enable, ask your DataHub admin to enable Email notifications.`;

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

    const onToggle = (enabled: boolean) => {
        toggleSink(enabled);
    };

    return (
        <SinkSettings>
            <Switch disabled={!sinkSupported} checked={sinkEnabled} onChange={onToggle} />
            <SinkTextContainer>
                <SinkTitle strong>Email Notifications</SinkTitle>
                <SinkDescription>
                    {sinkSupported ? supportedSinkDescription : unsupportedSinkDescription}
                    {sinkEnabled && inputValue && !editing && (
                        <>
                            <br />
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
                                        <StyledFormItem name="emailFormValue">
                                            <StyledInput
                                                placeholder="Email Address"
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
                        {editing && <HelperText>This is the email address where notifications will be sent</HelperText>}
                    </>
                )}
            </SinkTextContainer>
        </SinkSettings>
    );
};
