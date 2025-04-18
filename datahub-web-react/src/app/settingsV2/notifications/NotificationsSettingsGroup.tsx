import React from 'react';

import { NotificationSettingValue } from '@app/settingsV2/notifications/NotificationSettingValue';
import { NotificationTypeOptionsButton } from '@app/settingsV2/notifications/NotificationTypeOptionButton';
import {
    NotificationTypeDescription,
    OptionsPlaceholder,
    ScenarioSetting,
    ScenarioSettingValues,
    ScenarioSettingsSection,
    ScenarioSettingsSectionTitle,
} from '@app/settingsV2/notifications/styledComponents';
import { NOTIFICATION_SINKS, NotificationGroup, NotificationSink } from '@app/settingsV2/notifications/types';
import { NotificationScenarioType, NotificationSetting } from '@src/types.generated';

type Props = {
    notifications: NotificationGroup[];
    formattedNotificationSettings: Map<NotificationScenarioType, any>;
    originalSettings: NotificationSetting[];
    updateNotficationSettings: (settings: NotificationSetting[]) => void;
    refetch: () => void;
    notificationOptionsEnabled: boolean;
    openNotificationOptions: (type: NotificationScenarioType) => void;
    isSinkEnabled: (sink: NotificationSink) => boolean;
};

export const NotificationSettingsGroup = ({
    notifications,
    formattedNotificationSettings,
    originalSettings,
    updateNotficationSettings,
    refetch,
    notificationOptionsEnabled,
    openNotificationOptions,
    isSinkEnabled,
}: Props) => {
    return (
        <>
            {notifications.map((group) => (
                <span key={group.title}>
                    <ScenarioSettingsSection>
                        <ScenarioSettingsSectionTitle>{group.title}</ScenarioSettingsSectionTitle>
                        {group.notifications.map((notif) => (
                            <ScenarioSetting key={notif.type}>
                                <NotificationTypeDescription>{notif.description}</NotificationTypeDescription>
                                <ScenarioSettingValues>
                                    {NOTIFICATION_SINKS.map((sink) => (
                                        <NotificationSettingValue
                                            sink={sink}
                                            disabled={!isSinkEnabled(sink)}
                                            notificationType={notif.type}
                                            existingNotificationSettings={formattedNotificationSettings}
                                            originalSettings={originalSettings}
                                            updateNotificationSettings={updateNotficationSettings}
                                            refetch={refetch}
                                            key={sink.id}
                                        />
                                    ))}
                                    {(notificationOptionsEnabled && (
                                        <NotificationTypeOptionsButton
                                            onClick={() => openNotificationOptions(notif.type)}
                                        />
                                    )) || <OptionsPlaceholder />}
                                </ScenarioSettingValues>
                            </ScenarioSetting>
                        ))}
                    </ScenarioSettingsSection>
                </span>
            ))}
        </>
    );
};
