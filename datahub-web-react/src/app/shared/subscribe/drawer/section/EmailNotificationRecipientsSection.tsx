import React, { useEffect, useRef, useState } from 'react';
import { Alert, Form, Input, InputRef, Space, Switch, Typography } from 'antd';
import styled from 'styled-components/macro';
import { useForm } from 'antd/lib/form/Form';
import { EditTwoTone } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useGetGlobalSettingsQuery } from '../../../../../graphql/settings.generated';
import { EMAIL_SINK, NOTIFICATION_SINKS } from '../../../../settings/platform/types';
import { isSinkEnabled } from '../../../../settings/utils';
import useDrawerActions from '../state/actions';
import { ChannelSelections, EmailState } from '../state/types';
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

const StyledEmailSection = styled.div`
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
// const BorderLessButton = styled(Typography.Text)<{ $isPrimary?: boolean }>`
//     color: ${(props) => (props.$isPrimary ? REDESIGN_COLORS.BLUE : REDESIGN_COLORS.GREY_500)};
//     margin-left: 10px;
//     font-size: 14px;
//     cursor: pointer;
// `;

const EditText = styled.span`
    margin-left: 10px;
    color: ${REDESIGN_COLORS.BLUE};
    cursor: pointer;
`;
interface Props {
    isPersonal: boolean;
}

export default function EmailNotificationRecipientSection({ isPersonal }: Props) {
    const { config } = useAppConfig();
    const [form] = useForm();
    const actions = useDrawerActions();

    const email = useDrawerSelector(selectEmail);
    const emailEmailChannel = useDrawerSelector(selectEmailSettingsChannel);
    const shouldShowUpdateEmailSettingsWarning = useDrawerSelector(selectShouldShowUpdateEmailSettingsWarning);
    const [isEmailEditing, setIsEmailEditing] = useState(false);
    const [emailInputPlaceholder, setEmailInputPlaceholder] = useState('');

    const [isSubscriptionChannelSelected] = [email.channelSelection === ChannelSelections.SUBSCRIPTION];
    const [previousEmailChannelName, setPreviousEmailChannelName] = useState({ isUpdated: false, text: '' });
    const [emailChannelName, setEmailChannelName] = useState('');

    const channelInputRef = useRef<InputRef>(null);
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    const globallyEnabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, globalSettings?.globalSettings, config),
    );
    const emailSinkSupported = globallyEnabledSinks.some((sink) => sink.id === EMAIL_SINK.id);

    const updateEmailInState = (channelName: string) => {
        const emailData: EmailState = {
            enabled: true,
            channelSelection: 'SETTINGS',
            subscription: {
                saveAsDefault: true,
                channel: channelName,
            },
        };
        actions.setWholeEmailObject(emailData);
    };

    useEffect(() => {
        form.setFieldsValue({ emailFormValue: email.subscription.channel });
    }, [form, email.subscription.channel]);

    useEffect(() => {
        if (email.enabled && isSubscriptionChannelSelected) {
            updateEmailInState(emailEmailChannel || '');
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isSubscriptionChannelSelected, email.enabled]);

    const onChangeEmailSwitch = (checked: boolean) => {
        actions.setEmailEnabled(checked);
    };

    const onChangeChannelInput = ({ target: { value } }: React.ChangeEvent<HTMLInputElement>) => {
        setEmailChannelName(value);
        updateEmailInState(value);
    };

    useEffect(() => {
        if (!emailEmailChannel && isPersonal) {
            setEmailInputPlaceholder('Your Email');
        } else if (emailEmailChannel && isPersonal) {
            setEmailInputPlaceholder('Update Email');
        } else if (!emailEmailChannel && !isPersonal) {
            setEmailInputPlaceholder('Set Group Email');
        } else {
            setEmailInputPlaceholder('Update Group Email');
        }
        setEmailChannelName(emailEmailChannel ?? '');
        if (!previousEmailChannelName.isUpdated && emailEmailChannel) {
            setPreviousEmailChannelName({ isUpdated: true, text: emailEmailChannel ?? '' });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [emailEmailChannel]);

    // const handleRevertChannelName = () => {
    //     setEmailChannelName(previousEmailChannelName.text);
    //     updateEmailInState(previousEmailChannelName.text);
    //     setIsEmailEditing(false);
    // };
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
                <StyledEmailSection>
                    <Space direction="vertical">
                        {emailEmailChannel && !isEmailEditing ? (
                            <UseDefaultText>
                                To:&nbsp;
                                <>
                                    <SettingsEmailChannel>{emailChannelName}</SettingsEmailChannel>
                                    <EditText
                                        data-testid="email-channel-edit-button"
                                        onClick={() => setIsEmailEditing(true)}
                                    >
                                        <EditTwoTone style={{ marginRight: 3 }} /> Edit
                                    </EditText>
                                </>
                            </UseDefaultText>
                        ) : (
                            <>
                                <Form form={form}>
                                    <StyledFormItem name="emailFormValue">
                                        <UseDefaultText>To:&nbsp;</UseDefaultText>
                                        <StyledInput
                                            size="small"
                                            ref={channelInputRef}
                                            placeholder={emailInputPlaceholder}
                                            data-testid="alternative-email"
                                            disabled={!email.enabled || !emailSinkSupported}
                                            defaultValue={emailChannelName}
                                            onChange={onChangeChannelInput}
                                            status={!emailChannelName ? 'error' : undefined}
                                        />
                                        {/* {emailEmailChannel ? <BorderLessButton onClick={handleRevertChannelName}>Cancel</BorderLessButton> : null} */}
                                    </StyledFormItem>
                                </Form>
                            </>
                        )}
                    </Space>
                </StyledEmailSection>
            ) : (
                <DisabledText>
                    Email notifications are disabled. Reach out to your Acryl admins for more information.
                </DisabledText>
            )}
        </NotificationSwitchContainer>
    );
}
