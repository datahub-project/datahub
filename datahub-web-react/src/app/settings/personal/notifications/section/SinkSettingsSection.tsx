import { MoreOutlined } from '@ant-design/icons';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Typography, Switch, Input, Button, Form } from 'antd';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { UpdateSettingsInput } from '../../../../shared/subscribe/drawer/useSinkSettings';
import { NotificationSinkType } from '../../../../../types.generated';

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
    updateSinkSetting: (input: UpdateSettingsInput) => void;
    sinkSettingValue?: string;
    groupName?: string;
    sinkTypes?: NotificationSinkType[];
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
    sinkTypes,
}: Props) => {
    const [editing, setIsEditing] = useState<boolean>(false);
    const [allowEditing, setAllowEditing] = useState<boolean>(!!sinkTypes?.includes(NotificationSinkType.Slack));
    const [inputValue, setInputValue] = useState(sinkSettingValue);
    const [form] = Form.useForm();
    form.setFieldsValue({ slackFormValue: inputValue });
    useEffect(() => {
        if (!sinkSettingValue) {
            setIsEditing(true);
        } else {
            setIsEditing(false);
        }
        setInputValue(sinkSettingValue);
    }, [sinkSettingValue]);
    useEffect(() => {
        if (sinkTypes) {
            setAllowEditing(sinkTypes.includes(NotificationSinkType.Slack));
        }
    }, [sinkTypes]);

    const actorDescription = isPersonal ? 'you are' : `${groupName || 'the group'} is`;
    const sinkEnabledDescription = `Receive ${sinkName} notifications for entities ${actorDescription} subscribed to at ${sinkName} ${
        isPersonal ? 'member' : 'channel'
    } ID: `;
    const sinkDisabledDescription = `In order to enable, ask your DataHub admin to setup the ${sinkName} integration.`;
    const slackInputPlaceholder = isPersonal ? 'Slack Member ID' : 'Slack Channel ID';

    const saveButtonOnClick = () => {
        if (inputValue) {
            updateSinkSetting({ text: inputValue, sinkTypes: allowEditing ? [NotificationSinkType.Slack] : [] });
        }
        setIsEditing(false);
    };

    const cancelButtonOnClick = () => {
        setInputValue(sinkSettingValue);
        setIsEditing(false);
    };

    const onToggle = (enabled: boolean) => {
        setAllowEditing(enabled);
        updateSinkSetting({ text: sinkSettingValue || '', sinkTypes: enabled ? [NotificationSinkType.Slack] : [] });
    };

    return (
        <SinkSettings>
            <Switch disabled={!sinkEnabled} checked={allowEditing && sinkEnabled} onChange={onToggle} />
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
                                                placeholder={slackInputPlaceholder}
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
