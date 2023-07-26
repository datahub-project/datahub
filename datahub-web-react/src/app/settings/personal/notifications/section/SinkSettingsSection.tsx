import { MoreOutlined } from '@ant-design/icons';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Typography, Switch, Input, Button, Form } from 'antd';
import { ANTD_GRAY } from '../../../../entity/shared/constants';

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
    sinkName: string;
    updateSinkSetting: (sinkSettingValue: string) => void;
    sinkSettingValue?: string;
    groupName?: string;
};

/**
 * Notification sink settings section component
 */
export const SinkSettingsSection = ({
    isPersonal,
    sinkEnabled,
    sinkName,
    sinkSettingValue,
    updateSinkSetting,
    groupName,
}: Props) => {
    const [editing, setIsEditing] = useState<boolean>(false);
    const [allowEditing, setAllowEditing] = useState<boolean>(!!sinkSettingValue);
    const [inputValue, setInputValue] = useState(sinkSettingValue);
    const [form] = Form.useForm();
    form.setFieldsValue({ slackFormValue: inputValue });
    useEffect(() => {
        if (sinkSettingValue === undefined) {
            setIsEditing(true);
            setAllowEditing(false);
        } else {
            setIsEditing(false);
            setAllowEditing(true);
        }
        setInputValue(sinkSettingValue);
    }, [sinkSettingValue]);

    const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
    const sinkEnabledDescription = `Receive ${sinkName} notifications for entities ${actorDescription} subscribed to. ${sinkName} ${
        isPersonal ? 'member' : 'channel'
    } ID: `;
    const sinkDisabledDescription = `In order to enable, ask your DataHub admin to setup the ${sinkName} integration.`;
    const saveButtonOnClick = () => {
        if (inputValue) {
            updateSinkSetting(inputValue);
        }
        setIsEditing(false);
    };
    const cancelButtonOnClick = () => {
        setInputValue(sinkSettingValue);
        setIsEditing(false);
    };

    return (
        <SinkSettings>
            <Switch
                disabled={!sinkEnabled}
                checked={allowEditing && sinkEnabled}
                onChange={(checked) => setAllowEditing(checked)}
            />
            <SinkTextContainer>
                <SinkTitle strong>{`${sinkName} Notifications`}</SinkTitle>
                <SinkDescription>
                    {sinkEnabled ? sinkEnabledDescription : sinkDisabledDescription}
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
                                                placeholder="ABC12345678"
                                                value={inputValue}
                                                onChange={(e) => setInputValue(e.target.value)}
                                                disabled={!allowEditing}
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
