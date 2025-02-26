import React, { useMemo, useState } from 'react';
import { Image, Alert } from 'antd';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';
import { Message } from '../../../shared/Message';
import { NotificationTypeOptionsModal } from '../../notifications/NotificationTypeOptionsModal';
import {
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '../../../../graphql/settings.generated';
import {
    EntityType,
    NotificationScenarioType,
    NotificationSetting,
    NotificationSettings,
    StringMapEntry,
} from '../../../../types.generated';
import {
    updateNotificationTypeParams,
    SLACK_CHANNEL_PARAM_NAME,
    EMAIL_ADDRESS_PARAM_NAME,
    buildNotificationSettingsMap,
} from '../../notifications/utils';
import { useAppConfig } from '../../../useAppConfig';
import { USER_NOTIFICATION_GROUPS, GROUP_NOTIFICATION_GROUPS } from './types';
import { isSinkEnabled } from './utils';
import { NOTIFICATION_SINKS, NotificationTypeOptions } from '../../notifications/types';
import {
    ScenarioSettingsContainer,
    NotificationSinkHeader,
    NotificationSinkHeaders,
    NotificationSinkName,
    OptionsPlaceholder,
    ScenarioSettingsHeader,
    ScenarioSettingsTitle,
    ThinDivider,
} from '../../notifications/styledComponents';
import { NotificationSettingsGroup } from '../../notifications/NotificationsSettingsGroup';

type Props = {
    actorType: EntityType.CorpUser | EntityType.CorpGroup;
    actorUrn: string;
    loading: boolean;
    error: any;
    refetch: () => void;
    actorNotificationSettings?: Partial<NotificationSettings>;
};

export const ActorNotificationScenarioSettings = ({
    actorType,
    actorUrn,
    actorNotificationSettings,
    loading,
    error,
    refetch,
}: Props) => {
    const { config } = useAppConfig();
    const { globalSettings } = useGlobalSettingsContext();

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

    const [updateUserNotificationSettings] = useUpdateUserNotificationSettingsMutation();
    const [updateGroupNotificationSettings] = useUpdateGroupNotificationSettingsMutation();

    // Extract the notification settings and format them for easier access.
    const formattedNotificationSettings = useMemo(
        () => buildNotificationSettingsMap(actorNotificationSettings?.settings),
        [actorNotificationSettings],
    );

    const getDefaultNotificationTypeOptions = (type: NotificationScenarioType) => {
        const currSlackChannel = formattedNotificationSettings.get(type)?.params?.get(SLACK_CHANNEL_PARAM_NAME) || null;
        const currEmail = formattedNotificationSettings.get(type)?.params?.get(EMAIL_ADDRESS_PARAM_NAME) || null;
        return {
            slackChannel: currSlackChannel,
            email: currEmail,
        };
    };

    const updateActorNotificationSettings = (settings: NotificationSetting[]) => {
        if (actorType === EntityType.CorpUser) {
            const variables = {
                input: {
                    notificationSettings: {
                        settings,
                    },
                },
            };
            return updateUserNotificationSettings({ variables });
        }
        const variables = {
            input: {
                groupUrn: actorUrn,
                notificationSettings: {
                    settings,
                },
            },
        };
        return updateGroupNotificationSettings({ variables });
    };

    const updateNotificationTypeOptions = (type: NotificationScenarioType, options: NotificationTypeOptions) => {
        const newParams: StringMapEntry[] = [];

        newParams.push({ key: SLACK_CHANNEL_PARAM_NAME, value: options.slackChannel });
        newParams.push({ key: EMAIL_ADDRESS_PARAM_NAME, value: options.email });

        updateNotificationTypeParams(
            type,
            newParams,
            refetch,
            updateActorNotificationSettings,
            actorNotificationSettings?.settings || [],
        );

        closeNotificationOptions();
    };

    /**
     * A list of the enabled notification sinks. Sinks are destinations
     * to which notifications are routed.
     */
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) =>
        isSinkEnabled(sink.id, actorNotificationSettings, globalSettings, config),
    );

    /**
     * Only show notification options button if relevant sink is enabled.
     */
    const notificationOptionsEnabled = NOTIFICATION_SINKS.some(
        (sink) => sink.options && isSinkEnabled(sink.id, actorNotificationSettings, globalSettings, config),
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
                <ThinDivider />
                <NotificationSettingsGroup
                    notifications={
                        actorType === EntityType.CorpUser ? USER_NOTIFICATION_GROUPS : GROUP_NOTIFICATION_GROUPS
                    }
                    formattedNotificationSettings={formattedNotificationSettings}
                    originalSettings={actorNotificationSettings?.settings || []}
                    updateNotficationSettings={updateActorNotificationSettings}
                    refetch={refetch}
                    notificationOptionsEnabled={notificationOptionsEnabled}
                    openNotificationOptions={(type) => openNotificationOptions(type)}
                    isSinkEnabled={(sink) => isSinkEnabled(sink.id, actorNotificationSettings, globalSettings, config) || false}
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
