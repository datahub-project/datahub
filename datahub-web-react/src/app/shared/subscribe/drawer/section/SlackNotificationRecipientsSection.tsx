import React, { useEffect, useRef, useState } from 'react';
import { Alert, Form, Input, InputRef, Space, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { useForm } from 'antd/lib/form/Form';
import { Link } from 'react-router-dom';
import { EditTwoTone, MoreOutlined } from '@ant-design/icons';
import { useUserContext } from '@src/app/context/useUserContext';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { TestNotificationButton } from '@src/app/shared/notifications/TestNotificationButton';
import { SLACK_CONNECTION_URN } from '@src/app/settings/platform/slack/constants';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useGetGlobalSettingsQuery } from '../../../../../graphql/settings.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../../../../settings/platform/types';
import { isSinkEnabled } from '../../../../settings/utils';
import useDrawerActions from '../state/actions';
import { ChannelSelections, SlackState } from '../state/types';
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

const StyledSlackSection = styled.div`
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
    margin-left: ${LEFT_PADDING}px;
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

const UseDefaultText = styled(Typography.Text)`
    font-size: 14px;
`;

const SettingsSlackChannel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 700;
    margin-left: 5px;
`;

// const BorderLessButton = styled(Typography.Text)<{ $isPrimary?: boolean }>`
//     color: ${(props) => (props.$isPrimary ? REDESIGN_COLORS.BLUE : REDESIGN_COLORS.RED_NORMAL)};
//     margin-left: 10px;
//     font-size: 14px;
//     cursor: pointer;
// `;

const EditText = styled.span`
    margin-left: 10px;
    color: ${REDESIGN_COLORS.BLUE};
    cursor: pointer;
`;

const StyledAlert = styled(Alert)`
    margin: 8px 0 0 ${LEFT_PADDING}px;
`;
const TestNotificationButtonWrapper = styled.div`
    margin-left: ${LEFT_PADDING}px;
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
    const [isSlackEditing, setIsSlackEditing] = useState(false);
    const [previousSlackChannelName, setPreviousSlackChannelName] = useState({ isUpdated: false, text: '' });
    const [slackChannelName, setSlackChannelName] = useState('');
    const [slackInputPlaceholder, setSlackInputPlaceholder] = useState('');
    const [isSubscriptionChannelSelected] = [slack.channelSelection === ChannelSelections.SUBSCRIPTION];

    const channelInputRef = useRef<InputRef>(null);
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );
    const slackSinkSupported = globallyEnabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    useEffect(() => {
        form.setFieldsValue({ slackFormValue: slack.subscription.channel });
    }, [form, slack.subscription.channel]);

    const updateSlackInState = (channelName: string) => {
        const slackData: SlackState = {
            enabled: true,
            channelSelection: 'SETTINGS',
            subscription: {
                saveAsDefault: true,
                channel: channelName,
            },
        };
        actions.setWholeSlackObject(slackData);
    };

    useEffect(() => {
        if (slack.enabled && isSubscriptionChannelSelected) {
            updateSlackInState(settingsSlackChannel || '');
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isSubscriptionChannelSelected, slack.enabled]);

    const onChangeSlackSwitch = (checked: boolean) => {
        actions.setSlackEnabled(checked);
        if (checked && !settingsSlackChannel) {
            setIsSlackEditing(true);
        }
    };

    const onChangeChannelInput = ({ target: { value } }: React.ChangeEvent<HTMLInputElement>) => {
        const channelName = value;
        // if (!isPersonal) {
        //     // trim # from string and add only one # in front
        //     channelName = '#'.concat(trim(value, '#'));
        // }
        setSlackChannelName(channelName);
        updateSlackInState(channelName);
    };

    useEffect(() => {
        if (!settingsSlackChannel && isPersonal) {
            setSlackInputPlaceholder('Set Slack Member ID');
        } else if (settingsSlackChannel && isPersonal) {
            setSlackInputPlaceholder('Update Slack Member ID');
        } else {
            setSlackInputPlaceholder('#my-team-channel');
        }
        setSlackChannelName(settingsSlackChannel ?? '');
        if (!previousSlackChannelName.isUpdated && settingsSlackChannel) {
            setPreviousSlackChannelName({ isUpdated: true, text: settingsSlackChannel ?? '' });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [settingsSlackChannel]);

    const isAdminAccess = me?.platformPrivileges?.manageGlobalSettings || false;

    // const handleRevertChannelName = () => {
    //     setSlackChannelName(previousSlackChannelName.text);
    //     updateSlackInState(previousSlackChannelName.text);
    //     setIsSlackEditing(false);
    // };

    const renderSlackSink = () => {
        let slackSinkHtml = (
            <StyledSlackSection>
                <Space direction="vertical">
                    {slackSinkSupported && !isSlackEditing ? (
                        <UseDefaultText>
                            To:&nbsp;
                            <>
                                <SettingsSlackChannel style={{ opacity: slackChannelName ? 1 : 0.3 }}>
                                    {slackChannelName || (isPersonal ? 'Member ID' : '#slack-channel')}
                                </SettingsSlackChannel>
                                <EditText
                                    data-testid="slack-channel-edit-button"
                                    onClick={() => setIsSlackEditing(true)}
                                >
                                    <EditTwoTone style={{ marginRight: 3 }} /> Edit
                                </EditText>
                            </>
                        </UseDefaultText>
                    ) : (
                        <>
                            <Form form={form}>
                                <StyledFormItem name="slackFormValue">
                                    <UseDefaultText>
                                        To:&nbsp;
                                        <StyledInput
                                            size="small"
                                            ref={channelInputRef}
                                            placeholder={slackInputPlaceholder}
                                            data-testid="alternative-slack-member-id"
                                            disabled={!slack.enabled || !slackSinkSupported}
                                            value={slackChannelName}
                                            onChange={onChangeChannelInput}
                                            status={!slackChannelName ? 'error' : undefined}
                                        />
                                        {/* <BorderLessButton onClick={handleRevertChannelName}>Revert</BorderLessButton> */}
                                    </UseDefaultText>
                                </StyledFormItem>
                            </Form>
                        </>
                    )}
                </Space>
            </StyledSlackSection>
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
            {isSlackEditing && slackSinkSupported && (
                <>
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
                        hidden={!settingsSlackChannel && !slack.subscription.channel}
                        destinationSettings={
                            isPersonal
                                ? {
                                      userHandle: slack.subscription.channel || settingsSlackChannel || '',
                                  }
                                : {
                                      channels: [slack.subscription.channel || settingsSlackChannel || ''],
                                  }
                        }
                    />
                </TestNotificationButtonWrapper>
            )}
        </NotificationSwitchContainer>
    );
}
