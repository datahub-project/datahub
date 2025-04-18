import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { Divider, Typography, Card, Image, Alert } from 'antd';
import { InfoCircleFilled } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Message } from '../../../shared/Message';
import { NotificationTypeOptionsButton } from './NotificationTypeOptionButton';
import { PlatformNotificationOptionsModal } from './PlatformNotificationOptionsModal';
import {
    RECOMMENDED_PLATFORM_NOTIFICATIONS,
    NON_RECOMMENDED_PLATFORM_NOTIFICATIONS,
    NOTIFICATION_SINKS,
    PlatformNotificationOptions,
} from '../types';
import { useUpdateGlobalNotificationSettingsMutation } from '../../../../graphql/settings.generated';
import { GlobalSettings, NotificationScenarioType, StringMapEntry } from '../../../../types.generated';
import { isSinkEnabled } from '../../utils';
import {
    updateNotificationTypeParams,
    SLACK_CHANNEL_PARAM_NAME,
    buildNotificationSettingsMap,
    EMAIL_ADDRESS_PARAM_NAME,
} from './settingUtils';
import { NotificationSettingValue } from './NotificationSettingValue';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { useAppConfig } from '../../../useAppConfig';

const StyledCard = styled(Card)``;

const SettingsHeader = styled.div`
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SettingsTitle = styled(Typography.Text)`
    font-size: 18px;
`;

const SettingsSection = styled.div`
    margin-bottom: 15px;
`;

const OptionsPlaceholder = styled.div`
    width: 66px;
`;

const Setting = styled.div`
    padding-top: 6px;
    padding-bottom: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    font-size: 14px;
`;

const SettingValues = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

const NotificationTypeDescription = styled(Typography.Text)`
    color: ${ANTD_GRAY[8]};
`;

const NotificationSinkHeaders = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
`;

const NotificationSinkHeader = styled.div`
    width: 64px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const NotificationSinkName = styled(Typography.Text)`
    && {
        margin-left: 4px;
        font-size: 14px;
    }
`;

const GlobalNotificationsBanner = styled.div`
    background: ${REDESIGN_COLORS.YELLOW_200};
    border-radius: 8px;
    border: 1px solid ${REDESIGN_COLORS.YELLOW_600};
    padding: 8px 16px;
    margin: 18px 0 25px;
    font-size: 14px;
    .anticon-info-circle {
        color: ${REDESIGN_COLORS.YELLOW_600};
    }
`;

const InfoIcon = styled(InfoCircleFilled)`
    color: #7532a4;
    margin-right: 8px;
`;

type Props = {
    loading: boolean;
    error: any;
    refetch: () => void;
    globalSettings?: Partial<GlobalSettings>;
};

type INotificationGroup = {
    title: string;
    notifications: {
        type: NotificationScenarioType;
        description: string;
    }[];
};

export const PlatformNotificationsConfigurationCard = ({ globalSettings, loading, error, refetch }: Props) => {
    const { config } = useAppConfig();

    // Determine which notification sinks are enabled
    const [showNotificationOptions, setShowNotificationOptions] = useState(false);
    const [focusedNotificationType, setFocusedNotificationType] = useState<NotificationScenarioType | undefined>(
        undefined,
    );

    const openNotificationOptions = (type: NotificationScenarioType) => {
        setShowNotificationOptions(true);
        setFocusedNotificationType(type);
    };

    const closeNotificationOptions = () => {
        setShowNotificationOptions(false);
        setFocusedNotificationType(undefined);
    };

    const [updateGlobalNotificationSettings] = useUpdateGlobalNotificationSettingsMutation();

    const globalNotificationSettings = globalSettings?.notificationSettings;

    // Extract the notification settings and format them for easier access.
    const formattedNotificationSettings = useMemo(
        () => buildNotificationSettingsMap(globalNotificationSettings),
        [globalNotificationSettings],
    );

    const getDefaultNotificationTypeOptions = (type: NotificationScenarioType) => {
        const currSlackChannel = formattedNotificationSettings.get(type)?.params?.get(SLACK_CHANNEL_PARAM_NAME) || null;
        const currEmail = formattedNotificationSettings.get(type)?.params?.get(EMAIL_ADDRESS_PARAM_NAME) || null;
        return {
            slackChannel: currSlackChannel,
            email: currEmail,
        };
    };

    const updateNotificationTypeOptions = (type: NotificationScenarioType, options: PlatformNotificationOptions) => {
        const newParams: StringMapEntry[] = [];

        newParams.push({ key: SLACK_CHANNEL_PARAM_NAME, value: options.slackChannel });
        newParams.push({ key: EMAIL_ADDRESS_PARAM_NAME, value: options.email });

        updateNotificationTypeParams(
            type,
            newParams,
            refetch,
            updateGlobalNotificationSettings,
            globalNotificationSettings,
        );

        closeNotificationOptions();
    };

    /**
     * A list of the enabled notification sinks. Sinks are destinations
     * to which notifications are routed.
     */
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings, config));

    /**
     * Only show notification options button if relevant sink is enabled.
     */
    const notificationOptionsEnabled = NOTIFICATION_SINKS.some(
        (sink) => sink.options && isSinkEnabled(sink.id, globalSettings, config),
    );

    const renderNotificationGroups = (notifications: INotificationGroup[]) => {
        return notifications.map((group) => (
            <span key={group.title}>
                <SettingsSection>
                    <Typography.Title level={5}>{group.title}</Typography.Title>
                    {group.notifications.map((notif) => (
                        <Setting key={notif.type}>
                            <NotificationTypeDescription>{notif.description}</NotificationTypeDescription>
                            <SettingValues>
                                {NOTIFICATION_SINKS.map((sink) => (
                                    <NotificationSettingValue
                                        sink={sink}
                                        notificationType={notif.type}
                                        existingNotificationSettings={formattedNotificationSettings}
                                        refetch={refetch}
                                        globalSettings={globalSettings as GlobalSettings}
                                        key={sink.id}
                                    />
                                ))}
                                {(notificationOptionsEnabled && (
                                    <NotificationTypeOptionsButton
                                        onClick={() => openNotificationOptions(notif.type)}
                                    />
                                )) || <OptionsPlaceholder />}
                            </SettingValues>
                        </Setting>
                    ))}
                </SettingsSection>
            </span>
        ));
    };

    return (
        <>
            {loading && (
                <Message type="loading" content="Loading notification settings..." style={{ marginTop: '10%' }} />
            )}
            {error && <Alert type="error" message={error?.message || `Failed to load notification settings!`} />}
            <StyledCard>
                <SettingsHeader>
                    <SettingsTitle>Send a notification when...</SettingsTitle>
                    <NotificationSinkHeaders>
                        {NOTIFICATION_SINKS.map((sink) => (
                            <NotificationSinkHeader key={sink.id}>
                                {sink.img && <Image preview={false} src={sink.img} width={12} />}
                                <NotificationSinkName strong>{sink.name}</NotificationSinkName>
                            </NotificationSinkHeader>
                        ))}
                        <OptionsPlaceholder />
                    </NotificationSinkHeaders>
                </SettingsHeader>
                <Divider />
                {renderNotificationGroups(RECOMMENDED_PLATFORM_NOTIFICATIONS)}
                <Divider />

                <GlobalNotificationsBanner>
                    <InfoIcon />
                    Subscribing to the below platform events could create a lot of noise in the channel you&apos;ve
                    selected.
                </GlobalNotificationsBanner>
                {renderNotificationGroups(NON_RECOMMENDED_PLATFORM_NOTIFICATIONS)}
            </StyledCard>
            {focusedNotificationType && (
                <PlatformNotificationOptionsModal
                    initialState={getDefaultNotificationTypeOptions(focusedNotificationType)}
                    visible={showNotificationOptions}
                    sinks={enabledSinks}
                    onDone={(ops) =>
                        updateNotificationTypeOptions(focusedNotificationType as NotificationScenarioType, ops)
                    }
                    onClose={() => closeNotificationOptions()}
                />
            )}
        </>
    );
};
