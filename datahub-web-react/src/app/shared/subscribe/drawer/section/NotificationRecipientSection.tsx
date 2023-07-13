import uniq from 'lodash/uniq';
import React, { useState } from 'react';
import { Checkbox, Form, Input, Radio, RadioChangeEvent, Space, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useGetGlobalSettingsQuery } from '../../../../../graphql/settings.generated';
import { NotificationSinkType } from '../../../../../types.generated';
import {
    isGroupSlackChannelValid,
    isUserSlackHandleValid,
    validateGroupSlackChannel,
    validateSlackUserHandle,
} from '../../../../settings/personal/utils';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../../settings/platform/types';
import { isSinkEnabled } from '../../../../settings/utils';

const NotificationRecipientContainer = styled.div`
    margin-top: 32px;
    margin-left: 8px;
`;

const NotificationRecipientTitle = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 700;
`;

const NotificationSwitchContainer = styled.div`
    display: grid;
    grid-template-columns: 1fr 15fr;
    column-gap: 8px;
    row-gap: 8px;
    margin-top: 16px;
    align-items: center;
`;

const StyledSwitch = styled(Switch)`
    grid-column: 1;
`;

const StyledRadioGroup = styled(Radio.Group)`
    grid-column: 2;
`;

const NotificationTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
    grid-column: 2;
`;

const DisabledText = styled(Typography.Text)`
    font-weight: 500;
    grid-column: 2;
`;

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 0px;
`;

const StyledInput = styled(Input)`
    grid-column: 2;
    max-width: 300px;
    border-color: ${ANTD_GRAY[8]};
`;

const StyledCheckbox = styled(Checkbox)`
    grid-column: 2;
    border: 0.3px solid #ffffff;
`;

const SaveAsDefaultText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
    color: ${ANTD_GRAY[8]};
`;

interface Props {
    isPersonal: boolean;
    slackSinkDefaultValue?: string;
    notificationSinkTypes: NotificationSinkType[];
    setNotificationSinkTypes: (notificationSinkTypes: NotificationSinkType[]) => void;
    allowEditing: boolean;
    setAllowEditing: (allowEditing: boolean) => void;
    setCustomSlackSink: (customSlackSink: string | undefined) => void;
    saveSlackSinkAsDefault: boolean;
    setSaveSlackSinkAsDefault: (saveSlackSinkAsDefault: boolean) => void;
}

export default function NotificationRecipientSection({
    isPersonal,
    slackSinkDefaultValue,
    notificationSinkTypes,
    setNotificationSinkTypes,
    allowEditing,
    setAllowEditing,
    setCustomSlackSink,
    saveSlackSinkAsDefault,
    setSaveSlackSinkAsDefault,
}: Props) {
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const [inputSlackValue, setInputSlackValue] = useState<string>(isPersonal ? '@' : '#');
    const [useDefaultSlackSink, setUseDefaultSlackSink] = useState<boolean>(true);
    const defaultText = `Use default: ${slackSinkDefaultValue}`;
    const inputSlackValueIsValid = isPersonal
        ? isUserSlackHandleValid(inputSlackValue)
        : isGroupSlackChannelValid(inputSlackValue);

    const onChangeSlackSwitch = (checked: boolean) => {
        setAllowEditing(checked);
        setNotificationSinkTypes(uniq(checked ? [...notificationSinkTypes, NotificationSinkType.Slack] : []));
    };

    const onChangeSlackRadioGroup = ({ target: { value } }: RadioChangeEvent) => {
        if (value === 'default') {
            setUseDefaultSlackSink(true);
            setCustomSlackSink(undefined);
        } else if (value === 'custom') {
            setUseDefaultSlackSink(false);
            setCustomSlackSink(inputSlackValue);
        }
    };

    const onFocusChannelInput = () => {
        setUseDefaultSlackSink(false);
        setCustomSlackSink(inputSlackValue);
    };

    // what if check the bottom checkbox, desiring that we apply this one to the personal default
    // we'll only pass the new value through if we aren't on the default option, which is correct
    const onChangeChannelInput = ({ target: { value } }: React.ChangeEvent<HTMLInputElement>) => {
        setInputSlackValue(value);
        // todo - we can probably remove this, but replace it with a validation check
        // todo - the subscribe btn should be disabled unless we made a change on the form anywhere
        // todo - disable subscribe btn if we have an invalid form somehow
        if (!useDefaultSlackSink) setCustomSlackSink(value);
    };

    const onChangeDefaultCheckbox = () => {
        const newSaveSlackSinkAsDefault = !saveSlackSinkAsDefault;
        setSaveSlackSinkAsDefault(newSaveSlackSinkAsDefault);
        setCustomSlackSink(newSaveSlackSinkAsDefault ? inputSlackValue : undefined);
    };

    return (
        <>
            <NotificationRecipientContainer>
                <NotificationRecipientTitle>Send notifications via</NotificationRecipientTitle>
                <NotificationSwitchContainer>
                    <StyledSwitch
                        disabled={!slackSinkEnabled}
                        size="small"
                        checked={allowEditing}
                        onChange={onChangeSlackSwitch}
                    />
                    <NotificationTypeText>Slack Notifications</NotificationTypeText>
                    {slackSinkEnabled ? (
                        <StyledRadioGroup
                            disabled={!allowEditing || !slackSinkEnabled}
                            value={useDefaultSlackSink && slackSinkDefaultValue ? 'default' : 'custom'}
                            onChange={onChangeSlackRadioGroup}
                        >
                            <Space direction="vertical">
                                {slackSinkDefaultValue && <Radio value="default">{defaultText}</Radio>}
                                <Radio value="custom">
                                    <Form>
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
                                                placeholder={isPersonal ? '@user' : '#channel'}
                                                disabled={!allowEditing || !slackSinkEnabled}
                                                value={inputSlackValue}
                                                status={inputSlackValueIsValid ? undefined : 'error'}
                                                onFocus={onFocusChannelInput}
                                                onChange={onChangeChannelInput}
                                            />
                                        </StyledFormItem>
                                    </Form>
                                </Radio>
                            </Space>
                        </StyledRadioGroup>
                    ) : (
                        <DisabledText>
                            Reach out to your admin to enable your Slack integration to turn on Slack notifications.
                        </DisabledText>
                    )}
                    {!slackSinkDefaultValue && (
                        <StyledCheckbox onChange={onChangeDefaultCheckbox}>
                            <SaveAsDefaultText>Save as default</SaveAsDefaultText>
                        </StyledCheckbox>
                    )}
                </NotificationSwitchContainer>
            </NotificationRecipientContainer>
        </>
    );
}
