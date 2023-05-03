import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';
import { Typography, Switch, Input, Button, Form } from 'antd';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { validateGroupSlackChannel, validateSlackUserHandle } from '../../utils';

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
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const SinkDescription = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 500;
    max-width: 500px;
`;

const SinkEditButton = styled(Button)`
    margin-left: -6px;
`;

const EditDescription = styled(Typography.Text)`
    font-weight: 700;
    text-decoration: underline;
`;

const SinkSettingValueText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
    max-width: 500px;
`;

const SinkButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
    align-items: center;
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
    display: inline-flex;
    align-items: center;
    background-color: #078781;
    > .ant-btn {
        background-color: #078781;
    }
    > .ant-btn:focus {
        background-color: #078781;
    }
    > .ant-btn:hover {
        background-color: #078781;
    }
`;

const SaveButtonText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 500;
    font-size: 14px;
    line-height: 22px;
    color: white;
`;

const CancelButton = styled(Button)`
    display: inline-flex;
    align-items: center;
`;

const CancelButtonText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-weight: 500;
    font-size: 14px;
    line-height: 22px;
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
    const sinkEnabledDescription = `Receive ${sinkName} notifications for entities ${actorDescription} subscribed to at`;
    const sinkDisabledDescription = `In order to enable, ask your DataHub admin to setup the ${sinkName} integration.`;
    const inputPlaceholder = isPersonal ? '@your name' : '#engineering';
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
            <Switch checked={allowEditing} disabled={!sinkEnabled} onChange={(checked) => setAllowEditing(checked)} />
            <SinkTextContainer>
                <SinkTitle>{`${sinkName} Notifications`}</SinkTitle>
                <SinkDescription>{sinkEnabled ? sinkEnabledDescription : sinkDisabledDescription}</SinkDescription>
                {sinkEnabled && (
                    <SinkButtonsContainer>
                        {editing ? (
                            <>
                                <Form form={form}>
                                    <StyledFormItem
                                        name="slackFormValue"
                                        rules={[
                                            ({ getFieldValue }) => ({
                                                validator() {
                                                    const fieldValue = getFieldValue('slackFormValue');
                                                    return isPersonal
                                                        ? validateSlackUserHandle(fieldValue)
                                                        : validateGroupSlackChannel(fieldValue);
                                                },
                                            }),
                                        ]}
                                    >
                                        <StyledInput
                                            placeholder={inputPlaceholder}
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
                                    <SaveButtonText style={{ color: 'white' }}>Save</SaveButtonText>
                                </SaveButton>
                                <CancelButton onClick={cancelButtonOnClick}>
                                    <CancelButtonText>Cancel</CancelButtonText>
                                </CancelButton>
                            </>
                        ) : (
                            <>
                                <SinkSettingValueText>{inputValue}</SinkSettingValueText>
                                <SinkEditButton type="link" onClick={() => setIsEditing(true)}>
                                    <EditDescription>edit</EditDescription>
                                </SinkEditButton>
                            </>
                        )}
                    </SinkButtonsContainer>
                )}
            </SinkTextContainer>
        </SinkSettings>
    );
};
