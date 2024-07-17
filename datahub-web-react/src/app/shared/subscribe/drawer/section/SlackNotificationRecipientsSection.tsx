import React, { useEffect, useRef } from 'react';
import {
    Alert,
    Checkbox,
    Form,
    Input,
    InputRef,
    Radio,
    RadioChangeEvent,
    Space,
    Switch,
    Tooltip,
    Typography,
} from 'antd';
import styled from 'styled-components/macro';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import { useForm } from 'antd/lib/form/Form';
import { trim } from 'lodash';
import { Link } from 'react-router-dom';
import { InfoCircleOutlined, MoreOutlined } from '@ant-design/icons';
import { useUserContext } from '@src/app/context/useUserContext';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { TestNotificationButton } from '@src/app/shared/notifications/TestNotificationButton';
import { SLACK_CONNECTION_URN } from '@src/app/settings/platform/slack/constants';
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
    selectSlackSettingsChannel,
    selectSlack,
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

const MemberIdInstructionText = styled(Typography.Paragraph)`
    margin-left: ${LEFT_PADDING + 24}px;
    margin-top: 6px;
    margin-bottom: 0px !important;
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

const SettingsSlackChannel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
`;

const StyledAlert = styled(Alert)`
    margin: 8px 0 0 ${LEFT_PADDING}px;
`;
const TestNotificationButtonWrapper = styled.div`
    margin-left: ${LEFT_PADDING + 24}px;
`;

export default function SlackNotificationRecipientSection() {
    const { config } = useAppConfig();
    const [form] = useForm();
    const actions = useDrawerActions();
    const me = useUserContext();

    const slack = useDrawerSelector(selectSlack);
    const isPersonal = useDrawerSelector(selectIsPersonal);
    const settingsSlackChannel = useDrawerSelector(selectSlackSettingsChannel);
    const shouldShowUpdateSlackSettingsWarning = useDrawerSelector(selectShouldShowUpdateSlackSettingsWarning);

    const [isSettingsChannelSelected, isSubscriptionChannelSelected] = [
        slack.channelSelection === ChannelSelections.SETTINGS,
        slack.channelSelection === ChannelSelections.SUBSCRIPTION,
    ];

    const channelInputRef = useRef<InputRef>(null);
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );
    const slackSinkSupported = globallyEnabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    const slackInputPlaceholder = isPersonal ? 'Alternate Slack Member ID' : '#my-team-channel';

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
        actions.setSlackChannelSelection(value);
    };

    const onChangeChannelInput = ({ target: { value } }: React.ChangeEvent<HTMLInputElement>) => {
        let channelName = value;
        if (!isPersonal) {
            // trim # from string and add only one # in front
            channelName = '#'.concat(trim(value, '#'));
        }
        actions.setSlackSubscriptionChannel(channelName);
    };

    const onChangeSaveAsDefaultCheckbox = ({ target: { checked } }: CheckboxChangeEvent) => {
        actions.setSlackSaveAsDefault(checked);
    };
    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;

    const renderSlackSink = () => {
        let slackSinkHtml = (
            <StyledRadioGroup
                disabled={!slack.enabled || !slackSinkSupported}
                value={slack.channelSelection}
                onChange={onChangeSlackRadioGroup}
            >
                <Space direction="vertical">
                    {settingsSlackChannel && (
                        <Radio value={ChannelSelections.SETTINGS}>
                            <UseDefaultText>
                                Use default: <SettingsSlackChannel>{settingsSlackChannel}</SettingsSlackChannel>
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
                                    disabled={!slack.enabled || !slackSinkSupported || isSettingsChannelSelected}
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
        );
        if (!slackSinkSupported) {
            slackSinkHtml = isAdminAccess ? (
                <DisabledText>
                    Slack notifications are disabled. In order to enable,{' '}
                    <Link to="/settings/integrations/slack" style={{ color: REDESIGN_COLORS.BLUE }}>
                        setup a Slack integration.
                    </Link>
                </DisabledText>
            ) : (
                <DisabledText>
                    Slack notifications are disabled. Reach out to your Acryl admins for more information.
                </DisabledText>
            );
        }
        return slackSinkHtml;
    };

    return (
        <NotificationSwitchContainer>
            <SwitchWrapper>
                <StyledSwitch
                    disabled={!slackSinkSupported}
                    size="small"
                    checked={slack.enabled}
                    onChange={onChangeSlackSwitch}
                />
                <SinkTypeText>Slack</SinkTypeText>
            </SwitchWrapper>
            {shouldShowUpdateSlackSettingsWarning && (
                <StyledAlert
                    type="warning"
                    message="Your Slack notifications are currently disabled. Subscribing to this entity will
                        automatically re-enable them."
                    showIcon
                />
            )}
            {renderSlackSink()}
            {isSubscriptionChannelSelected && slackSinkSupported && (
                <>
                    <StyledCheckbox
                        disabled={!slack.enabled || !slackSinkSupported}
                        checked={slack.subscription.saveAsDefault}
                        onChange={onChangeSaveAsDefaultCheckbox}
                    >
                        <SaveAsDefaultText>
                            Save as default{' '}
                            <Tooltip title="You can manage defaults under the 'Settings' page > 'My Notifications' tab">
                                <InfoCircleOutlined />
                            </Tooltip>
                        </SaveAsDefaultText>
                    </StyledCheckbox>
                    <MemberIdInstructionText>
                        {isPersonal ? (
                            <>
                                Find a member ID from the <MoreOutlined /> menu in your Slack profile.
                                <a
                                    target="_blank"
                                    rel="noreferrer"
                                    href="https://datahubproject.io/docs/managed-datahub/slack/saas-slack-setup/#how-to-find-user-id-in-slack"
                                >
                                    {' '}
                                    See instructions.
                                </a>
                            </>
                        ) : (
                            <>Ensure the Slack bot has been added to this channel</>
                        )}
                    </MemberIdInstructionText>
                </>
            )}
            {slackSinkSupported && (
                <TestNotificationButtonWrapper>
                    <TestNotificationButton
                        integration="slack"
                        connectionUrn={SLACK_CONNECTION_URN}
                        hidden={!(isSubscriptionChannelSelected ? slack.subscription.channel : settingsSlackChannel)}
                        destinationSettings={
                            isPersonal
                                ? {
                                      userHandle:
                                          (isSubscriptionChannelSelected
                                              ? slack.subscription.channel
                                              : settingsSlackChannel) || '',
                                  }
                                : {
                                      channels: [
                                          (isSubscriptionChannelSelected
                                              ? slack.subscription.channel
                                              : settingsSlackChannel) || '',
                                      ],
                                  }
                        }
                    />
                </TestNotificationButtonWrapper>
            )}
        </NotificationSwitchContainer>
    );
}
