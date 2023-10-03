import React, { useEffect, useRef } from 'react';
import { Alert, Checkbox, Form, Input, InputRef, Radio, RadioChangeEvent, Space, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import { useForm } from 'antd/lib/form/Form';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useGetGlobalSettingsQuery } from '../../../../../graphql/settings.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../../settings/platform/types';
import { isSinkEnabled } from '../../../../settings/utils';
import useDrawerActions from '../state/actions';
import { ChannelSelections } from '../state/types';
import {
    selectShouldShowUpdateSlackSettingsWarning,
    useDrawerSelector,
    selectIsPersonal,
    selectSettingsSlackChannel,
    selectSlack,
} from '../state/selectors';

const LEFT_PADDING = 36;

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
    font-family: 'Manrope', sans-serif;
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
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 22px;
    font-weight: 400;
`;

const UseDefaultText = styled(Typography.Text)`
    font-size: 14px;
`;

const SettingsSlackChannel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
`;

const StyledAlert = styled(Alert)`
    margin: 8px 0 0 ${LEFT_PADDING}px;
`;

export default function NotificationRecipientSection() {
    const [form] = useForm();
    const actions = useDrawerActions();

    const slack = useDrawerSelector(selectSlack);
    const isPersonal = useDrawerSelector(selectIsPersonal);
    const settingsSlackChannel = useDrawerSelector(selectSettingsSlackChannel);
    const shouldShowUpdateSlackSettingsWarning = useDrawerSelector(selectShouldShowUpdateSlackSettingsWarning);

    const [isSettingsChannelSelected, isSubscriptionChannelSelected] = [
        slack.channelSelection === ChannelSelections.SETTINGS,
        slack.channelSelection === ChannelSelections.SUBSCRIPTION,
    ];

    const channelInputRef = useRef<InputRef>(null);
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);
    const slackInputPlaceholder = isPersonal ? 'Alternate Slack Member ID' : 'Alternate Slack Channel ID';

    useEffect(() => {
        form.setFieldsValue({ slackFormValue: slack.subscription.channel });
    }, [form, slack.subscription.channel]);

    useEffect(() => {
        if (slack.enabled && isSubscriptionChannelSelected) channelInputRef.current?.focus();
    }, [isSubscriptionChannelSelected, slack.enabled]);

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
                    <SwitchWrapper>
                        <StyledSwitch
                            disabled={!slackSinkEnabled}
                            size="small"
                            checked={slack.enabled}
                            onChange={onChangeSlackSwitch}
                        />
                        <SinkTypeText>Slack Notifications</SinkTypeText>
                    </SwitchWrapper>
                    {shouldShowUpdateSlackSettingsWarning && (
                        <StyledAlert
                            type="warning"
                            message="Your Slack notifications are currently disabled. Subscribing to this entity will
                        automatically re-enable them."
                            showIcon
                        />
                    )}
                    {slackSinkEnabled ? (
                        <StyledRadioGroup
                            disabled={!slack.enabled || !slackSinkEnabled}
                            value={slack.channelSelection}
                            onChange={onChangeSlackRadioGroup}
                        >
                            <Space direction="vertical">
                                {settingsSlackChannel && (
                                    <Radio value={ChannelSelections.SETTINGS}>
                                        <UseDefaultText>
                                            Use default:{' '}
                                            <SettingsSlackChannel>{settingsSlackChannel}</SettingsSlackChannel>
                                        </UseDefaultText>
                                    </Radio>
                                )}
                                <Radio value={ChannelSelections.SUBSCRIPTION} data-testid="alternative-slack-radio">
                                    <Form form={form}>
                                        <StyledFormItem name="slackFormValue">
                                            <StyledInput
                                                size="small"
                                                ref={channelInputRef}
                                                placeholder={slackInputPlaceholder}
                                                data-testid="alternative-slack-member-id"
                                                disabled={
                                                    !slack.enabled || !slackSinkEnabled || isSettingsChannelSelected
                                                }
                                                value={slack.subscription.channel}
                                                onChange={onChangeChannelInput}
                                                status={
                                                    isSubscriptionChannelSelected && !slack.subscription.channel
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
                            Reach out to your admin to enable your Slack integration to turn on Slack notifications.
                        </DisabledText>
                    )}
                    {isSubscriptionChannelSelected && (
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
