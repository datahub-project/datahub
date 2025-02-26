import React, { useMemo, useState } from 'react';
import styled from 'styled-components';
import { Divider, Image, Alert } from 'antd';
import { InfoCircleFilled } from '@ant-design/icons';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { colors } from '@src/alchemy-components';
import { Message } from '../../../shared/Message';
import { NotificationTypeOptionsModal } from '../../notifications/NotificationTypeOptionsModal';
import { RECOMMENDED_PLATFORM_NOTIFICATIONS, NON_RECOMMENDED_PLATFORM_NOTIFICATIONS } from '../types';
import { useUpdateGlobalNotificationSettingsMutation } from '../../../../graphql/settings.generated';
import {
    GlobalSettings,
    NotificationScenarioType,
    NotificationSetting,
    StringMapEntry,
} from '../../../../types.generated';
import { isSinkEnabled } from '../../utils';
import { buildGlobalNotificationSettingsMap } from './utils';
import { useAppConfig } from '../../../useAppConfig';
import { NOTIFICATION_SINKS, NotificationTypeOptions } from '../../notifications/types';
import {
    EMAIL_ADDRESS_PARAM_NAME,
    SLACK_CHANNEL_PARAM_NAME,
    updateNotificationTypeParams,
} from '../../notifications/utils';
import {
    NotificationSinkHeader,
    NotificationSinkHeaders,
    NotificationSinkName,
    OptionsPlaceholder,
    ScenarioSettingsContainer,
    ScenarioSettingsHeader,
    ScenarioSettingsTitle,
} from '../../notifications/styledComponents';
import { NotificationSettingsGroup } from '../../notifications/NotificationsSettingsGroup';

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
    color: ${colors.violet[500]};
    margin-right: 8px;
`;

type Props = {
    loading: boolean;
    error: any;
    refetch: () => void;
    globalSettings?: Partial<GlobalSettings>;
};

export const PlatformNotificationsScenarioSettings = ({ globalSettings, loading, error, refetch }: Props) => {
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

    const [updateGlobalNotificationSettingsMutation] = useUpdateGlobalNotificationSettingsMutation();

    const globalNotificationSettings = globalSettings?.notificationSettings;

    // Extract the notification settings and format them for easier access.
    const formattedNotificationSettings = useMemo(
        () => buildGlobalNotificationSettingsMap(globalNotificationSettings),
        [globalNotificationSettings],
    );

    const updateGlobalNotficationSettings = (settings: NotificationSetting[]) => {
        return updateGlobalNotificationSettingsMutation({
            variables: {
                input: {
                    settings,
                },
            },
        });
    };

    const getDefaultNotificationTypeOptions = (type: NotificationScenarioType) => {
        const currSlackChannel = formattedNotificationSettings.get(type)?.params?.get(SLACK_CHANNEL_PARAM_NAME) || null;
        const currEmail = formattedNotificationSettings.get(type)?.params?.get(EMAIL_ADDRESS_PARAM_NAME) || null;
        return {
            slackChannel: currSlackChannel,
            email: currEmail,
        };
    };

    const updateNotificationTypeOptions = (type: NotificationScenarioType, options: NotificationTypeOptions) => {
        const newParams: StringMapEntry[] = [];

        newParams.push({ key: SLACK_CHANNEL_PARAM_NAME, value: options.slackChannel });
        newParams.push({ key: EMAIL_ADDRESS_PARAM_NAME, value: options.email });

        updateNotificationTypeParams(
            type,
            newParams,
            refetch,
            updateGlobalNotficationSettings,
            globalNotificationSettings?.settings || [],
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

    return (
        <>
            {loading && (
                <Message type="loading" content="Loading notification settings..." style={{ marginTop: '10%' }} />
            )}
            {error && <Alert type="error" message={error?.message || `Failed to load notification settings!`} />}
            <ScenarioSettingsContainer>
                <ScenarioSettingsHeader>
                    <ScenarioSettingsTitle>Send a notification when...</ScenarioSettingsTitle>
                    <NotificationSinkHeaders>
                        {NOTIFICATION_SINKS.map((sink) => (
                            <NotificationSinkHeader key={sink.id}>
                                {sink.img && <Image preview={false} src={sink.img} width={12} />}
                                <NotificationSinkName>{sink.name}</NotificationSinkName>
                            </NotificationSinkHeader>
                        ))}
                        <OptionsPlaceholder />
                    </NotificationSinkHeaders>
                </ScenarioSettingsHeader>
                <Divider />
                <NotificationSettingsGroup
                    notifications={RECOMMENDED_PLATFORM_NOTIFICATIONS}
                    formattedNotificationSettings={formattedNotificationSettings}
                    originalSettings={globalSettings?.notificationSettings?.settings || []}
                    updateNotficationSettings={updateGlobalNotficationSettings}
                    refetch={refetch}
                    notificationOptionsEnabled={notificationOptionsEnabled}
                    openNotificationOptions={(type) => openNotificationOptions(type)}
                    isSinkEnabled={sink => isSinkEnabled(sink.id, globalSettings, config)}
                />
                <Divider />
                <GlobalNotificationsBanner>
                    <InfoIcon />
                    Subscribing to the below platform events could create a lot of noise in the channel you&apos;ve
                    selected.
                </GlobalNotificationsBanner>
                <NotificationSettingsGroup
                    notifications={NON_RECOMMENDED_PLATFORM_NOTIFICATIONS}
                    formattedNotificationSettings={formattedNotificationSettings}
                    originalSettings={globalSettings?.notificationSettings?.settings || []}
                    updateNotficationSettings={updateGlobalNotficationSettings}
                    refetch={refetch}
                    notificationOptionsEnabled={notificationOptionsEnabled}
                    openNotificationOptions={(type) => openNotificationOptions(type)}
                    isSinkEnabled={sink => isSinkEnabled(sink.id, globalSettings, config)}
                />
            </ScenarioSettingsContainer>
            {focusedNotificationType && (
                <NotificationTypeOptionsModal
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
