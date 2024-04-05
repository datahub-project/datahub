import React, { useEffect, useRef } from 'react';
import { Alert, Checkbox, Form, Input, InputRef, Radio, RadioChangeEvent, Space, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import { useForm } from 'antd/lib/form/Form';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useGetGlobalSettingsQuery } from '../../../../../graphql/settings.generated';
import { EMAIL_SINK, NOTIFICATION_SINKS } from '../../../../settings/platform/types';
import { isSinkEnabled } from '../../../../settings/utils';
import useDrawerActions from '../state/actions';
import { ChannelSelections } from '../state/types';
import {
    useDrawerSelector,
    selectEmail,
    selectEmailSettingsChannel,
    selectShouldShowUpdateEmailSettingsWarning,
} from '../state/selectors';
import { useAppConfig } from '../../../../useAppConfig';

const LEFT_PADDING = 36;

const NotificationSwitchContainer = styled.div`
    margin-top: 16px;
    display: flex;
    flex-direction: column;
    justify-content: center;
`;

const StyledSwitch = styled(Switch)`
    margin-right: 8px;
`;

const StyledRadioGroup = styled(Radio.Group)`
    padding-left: ${LEFT_PADDING}px;
    margin-top: 8px;
`;

const SwitchWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const SinkTypeText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 700;
`;

const DisabledText = styled(Typography.Text)`
    font-weight: 500;
    padding-left: ${LEFT_PADDING}px;
    margin-top: 8px;
`;

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 0px;
`;

const StyledInput = styled(Input)`
    font-size: 14px;
    width: 200px;
    border-color: ${ANTD_GRAY[8]};
`;

const StyledCheckbox = styled(Checkbox)`
    margin-left: ${LEFT_PADDING + 24}px;
    margin-top: 8px;
    border: 0.3px solid #ffffff;
`;

const SaveAsDefaultText = styled(Typography.Text)`
    font-family: 'Mulish', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
`;

const UseDefaultText = styled(Typography.Text)`
    font-size: 14px;
`;

const SettingsEmailChannel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
`;

const StyledAlert = styled(Alert)`
    margin: 8px 0 0 ${LEFT_PADDING}px;
`;

export default function EmailNotificationRecipientSection() {
    const { config } = useAppConfig(); 
    const [form] = useForm();
    const actions = useDrawerActions();

    const email = useDrawerSelector(selectEmail);
    const emailEmailChannel = useDrawerSelector(selectEmailSettingsChannel);
    const shouldShowUpdateEmailSettingsWarning = useDrawerSelector(selectShouldShowUpdateEmailSettingsWarning);

    const [isSettingsChannelSelected, isSubscriptionChannelSelected] = [
        email.channelSelection === ChannelSelections.SETTINGS,
        email.channelSelection === ChannelSelections.SUBSCRIPTION,
    ];

    const channelInputRef = useRef<InputRef>(null);
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );
    const emailSinkSupported = globallyEnabledSinks.some((sink) => sink.id === EMAIL_SINK.id);
    const emailInputPlaceholder = 'Alterate Email address';

    useEffect(() => {
        form.setFieldsValue({ emailFormValue: email.subscription.channel });
    }, [form, email.subscription.channel]);

    useEffect(() => {
        if (email.enabled && isSubscriptionChannelSelected) channelInputRef.current?.focus();
    }, [isSubscriptionChannelSelected, email.enabled]);

    const onChangeEmailSwitch = (checked: boolean) => {
        actions.setEmailEnabled(checked);
    };

    const onChangeEmailRadioGroup = ({ target: { value } }: RadioChangeEvent) => {
        actions.setEmailChannelSelection(value);
    };

    const onChangeChannelInput = ({ target: { value } }: React.ChangeEvent<HTMLInputElement>) => {
        actions.setEmailSubscriptionChannel(value);
    };

    const onChangeSaveAsDefaultCheckbox = ({ target: { checked } }: CheckboxChangeEvent) => {
        actions.setEmailSaveAsDefault(checked);
    };

    console.log(email.enabled);

    return (
        <NotificationSwitchContainer>
            <SwitchWrapper>
                <StyledSwitch
                    disabled={!emailSinkSupported}
                    size="small"
                    checked={email.enabled}
                    onChange={onChangeEmailSwitch}
                />
                <SinkTypeText>Email</SinkTypeText>
            </SwitchWrapper>
            {shouldShowUpdateEmailSettingsWarning && (
                <StyledAlert
                    type="warning"
                    message="Your Email notifications are currently disabled. Subscribing to this entity will
                        automatically re-enable them."
                    showIcon
                />
            )}
            {emailSinkSupported ? (
                <StyledRadioGroup
                    disabled={!email.enabled || !emailSinkSupported}
                    value={email.channelSelection}
                    onChange={onChangeEmailRadioGroup}
                >
                    <Space direction="vertical">
                        {emailEmailChannel && (
                            <Radio value={ChannelSelections.SETTINGS}>
                                <UseDefaultText>
                                    Use default: <SettingsEmailChannel>{emailEmailChannel}</SettingsEmailChannel>
                                </UseDefaultText>
                            </Radio>
                        )}
                        <Radio value={ChannelSelections.SUBSCRIPTION} data-testid="alternative-email-radio">
                            <Form form={form}>
                                <StyledFormItem name="emailFormValue">
                                    <StyledInput
                                        size="small"
                                        ref={channelInputRef}
                                        placeholder={emailInputPlaceholder}
                                        data-testid="alternative-email"
                                        disabled={!email.enabled || !emailSinkSupported || isSettingsChannelSelected}
                                        value={email.subscription.channel}
                                        onChange={onChangeChannelInput}
                                        status={
                                            isSubscriptionChannelSelected && !email.subscription.channel
                                                ? 'error'
                                                : undefined
                                        }
                                    />
                                </StyledFormItem>
                            </Form>
                        </Radio>
                    </Space>
                </StyledRadioGroup>
            ) : (
                <DisabledText>
                    Email notifications are disabled. Reach out to your Acryl admins for more information.
                </DisabledText>
            )}
            {isSubscriptionChannelSelected && (
                <StyledCheckbox
                    disabled={!email.enabled || !emailSinkSupported}
                    checked={email.subscription.saveAsDefault}
                    onChange={onChangeSaveAsDefaultCheckbox}
                >
                    <SaveAsDefaultText>Save as default</SaveAsDefaultText>
                </StyledCheckbox>
            )}
        </NotificationSwitchContainer>
    );
}
