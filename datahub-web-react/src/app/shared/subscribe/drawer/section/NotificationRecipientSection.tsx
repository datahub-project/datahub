import React, { useEffect, useRef } from 'react';
import { Checkbox, Form, Input, InputRef, Radio, RadioChangeEvent, Space, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import { useForm } from 'antd/lib/form/Form';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useGetGlobalSettingsQuery } from '../../../../../graphql/settings.generated';
import {
    isGroupSlackChannelValid,
    isUserSlackHandleValid,
    validateGroupSlackChannel,
    validateSlackUserHandle,
} from '../../../../settings/personal/utils';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../../settings/platform/types';
import { isSinkEnabled } from '../../../../settings/utils';
import { useDrawerState } from '../state/context';
import useDrawerActions from '../state/actions';

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
    margin-left: 24px;
    border: 0.3px solid #ffffff;
`;

const SaveAsDefaultText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
    color: ${ANTD_GRAY[8]};
`;

export default function NotificationRecipientSection() {
    const [form] = useForm();
    const actions = useDrawerActions();

    const { isPersonal, slack } = useDrawerState();

    const [isSettingsChannel, isSubscriptionChannel] = [
        slack.channelSelection === 'settings',
        slack.channelSelection === 'subscription',
    ];

    const channelInputRef = useRef<InputRef>(null);
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    useEffect(() => {
        form.setFieldsValue({ slackFormValue: slack.subscription.channel });
    }, [form, slack.subscription.channel]);

    useEffect(() => {
        if (isSubscriptionChannel) channelInputRef.current?.focus();
    }, [isSubscriptionChannel]);

    const customSlackSinkIsValid = isPersonal
        ? isUserSlackHandleValid(slack.subscription.channel ?? '')
        : isGroupSlackChannelValid(slack.subscription.channel ?? '');

    const onChangeSlackSwitch = (checked: boolean) => {
        actions.setSlackEnabled(checked);
    };

    const onChangeSlackRadioGroup = ({ target: { value } }: RadioChangeEvent) => {
        actions.setChannelSelection(value);
    };

    const onChangeChannelInput = ({ target: { value } }: React.ChangeEvent<HTMLInputElement>) => {
        actions.setSubscriptionChannel(value);
    };

    const onChangeSaveAsDefaultCheckbox = ({ target: { checked } }: CheckboxChangeEvent) => {
        actions.setSaveAsDefault(checked);
    };

    return (
        <>
            <NotificationRecipientContainer>
                <NotificationRecipientTitle>Send notifications via</NotificationRecipientTitle>
                <NotificationSwitchContainer>
                    <StyledSwitch
                        disabled={!slackSinkEnabled}
                        size="small"
                        checked={slack.enabled}
                        onChange={onChangeSlackSwitch}
                    />
                    <NotificationTypeText>Slack Notifications</NotificationTypeText>
                    {slackSinkEnabled ? (
                        <StyledRadioGroup
                            disabled={!slack.enabled || !slackSinkEnabled}
                            value={slack.channelSelection}
                            onChange={onChangeSlackRadioGroup}
                        >
                            <Space direction="vertical">
                                {slack.settings.channel && (
                                    <Radio value="settings">Use default: {slack.settings.channel}</Radio>
                                )}
                                <Radio value="subscription">
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
                                                ref={channelInputRef}
                                                placeholder={isPersonal ? '@user' : '#channel'}
                                                disabled={!slack.enabled || !slackSinkEnabled || isSettingsChannel}
                                                value={slack.subscription.channel}
                                                status={customSlackSinkIsValid ? undefined : 'error'}
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
                    {isSubscriptionChannel && slack.settings.channel && (
                        <StyledCheckbox
                            disabled={!slack.enabled || !slackSinkEnabled}
                            checked={slack.subscription.saveAsDefault}
                            onChange={onChangeSaveAsDefaultCheckbox}
                        >
                            <SaveAsDefaultText>Save as default</SaveAsDefaultText>
                        </StyledCheckbox>
                    )}
                </NotificationSwitchContainer>
            </NotificationRecipientContainer>
        </>
    );
}
