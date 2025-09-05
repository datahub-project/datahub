import { Alert, Image } from 'antd';
import React, { useMemo, useState } from 'react';

import { NotificationTypeOptionsModal } from '@app/settingsV2/notifications/NotificationTypeOptionsModal';
import { NotificationSettingsGroup } from '@app/settingsV2/notifications/NotificationsSettingsGroup';
import {
    NotificationSinkHeader,
    NotificationSinkHeaders,
    NotificationSinkName,
    OptionsPlaceholder,
    ScenarioSettingsContainer,
    ScenarioSettingsHeader,
    ScenarioSettingsTitle,
    ThinDivider,
} from '@app/settingsV2/notifications/styledComponents';
import { NOTIFICATION_SINKS, NotificationTypeOptions } from '@app/settingsV2/notifications/types';
import {
    EMAIL_ADDRESS_PARAM_NAME,
    SLACK_CHANNEL_PARAM_NAME,
    buildNotificationSettingsMap,
    updateNotificationTypeParams,
} from '@app/settingsV2/notifications/utils';
import useGroupNotificationGroups from '@app/settingsV2/personal/notifications/hooks/useGroupNotificationGroups';
import useUserNotificationGroups from '@app/settingsV2/personal/notifications/hooks/useUserNotificationGroups';
import { isSinkEnabled } from '@app/settingsV2/personal/notifications/utils';
import { Message } from '@app/shared/Message';
import { useAppConfig } from '@app/useAppConfig';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';

import {
    useUpdateGroupNotificationSettingsMutation,
    useUpdateUserNotificationSettingsMutation,
} from '@graphql/settings.generated';
import {
    EntityType,
    NotificationScenarioType,
    NotificationSetting,
    NotificationSettings,
    StringMapEntry,
} from '@types';

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

    const userNotificationGroups = useUserNotificationGroups(config);
    const groupNotificationGroups = useGroupNotificationGroups(config);

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
     * A list of the visible notification sinks. All sinks are shown for discoverability,
     * except Teams which is completely hidden when the feature flag is disabled.
     */
    const visibleSinks = NOTIFICATION_SINKS.filter((sink) => {
        // Only hide Teams when feature flag is disabled - show all others for discoverability
        if (sink.id === 'microsoft-teams') {
            return config?.featureFlags?.teamsNotificationsEnabled || false;
        }
        return true;
    });

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
                        {visibleSinks.map((sink) => (
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
                    notifications={actorType === EntityType.CorpUser ? userNotificationGroups : groupNotificationGroups}
                    formattedNotificationSettings={formattedNotificationSettings}
                    originalSettings={actorNotificationSettings?.settings || []}
                    updateNotficationSettings={updateActorNotificationSettings}
                    refetch={refetch}
                    notificationOptionsEnabled={notificationOptionsEnabled}
                    openNotificationOptions={(type) => openNotificationOptions(type)}
                    enabledSinks={visibleSinks}
                    isSinkEnabled={(sink) =>
                        isSinkEnabled(sink.id, actorNotificationSettings, globalSettings, config) || false
                    }
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
