import { message } from 'antd';
import { useEffect, useMemo, useState } from 'react';

import { useUpdateUserNotificationSettingsMutation } from '@graphql/settings.generated';
import { NotificationScenarioType, NotificationSetting, NotificationSettingValue, NotificationSettings } from '@types';

interface UseMarketingOptInProps {
    actorNotificationSettings?: Partial<NotificationSettings>;
    refetchNotificationSettings?: () => void;
    onError?: (error: any) => void;
    onSuccess?: () => void;
}

export const useMarketingOptIn = ({
    actorNotificationSettings,
    refetchNotificationSettings,
    onError,
    onSuccess,
}: UseMarketingOptInProps) => {
    const [isUpdatingMarketingSettings, setIsUpdatingMarketingSettings] = useState<boolean>(false);
    const [updateUserNotificationSettings] = useUpdateUserNotificationSettingsMutation();

    // Memoize the community updates setting to avoid unnecessary re-computations
    const communityUpdatesSetting = useMemo(() => {
        if (!actorNotificationSettings?.settings) return null;

        return (
            actorNotificationSettings.settings.find(
                (setting: NotificationSetting) => setting.type === NotificationScenarioType.DataHubCommunityUpdates,
            ) || null
        );
    }, [actorNotificationSettings?.settings]);

    const [marketingOptIn, setMarketingOptIn] = useState<boolean>(true);

    // Update marketingOptIn when the community updates setting changes
    useEffect(() => {
        if (!isUpdatingMarketingSettings) {
            // Auto-subscribe by default (true)
            if (!actorNotificationSettings?.settings || !communityUpdatesSetting) {
                setMarketingOptIn(true);
            } else {
                // Use actual setting value if available
                setMarketingOptIn(communityUpdatesSetting.value === NotificationSettingValue.Enabled);
            }
        }
    }, [communityUpdatesSetting, actorNotificationSettings?.settings, isUpdatingMarketingSettings]);

    const processedSettings = () => {
        const currentSettings = actorNotificationSettings?.settings || [];

        const otherSettings = currentSettings
            .filter((setting: NotificationSetting) => setting.type !== NotificationScenarioType.DataHubCommunityUpdates)
            .map((setting: NotificationSetting) => {
                // Create a clean setting object, explicitly excluding Apollo fields
                const cleanSetting: any = {
                    type: setting.type,
                    value: setting.value,
                };

                // Clean params array by removing __typename and other Apollo fields
                if (setting.params) {
                    cleanSetting.params = setting.params.map((param: any) => {
                        // Only include key and value, excluding __typename and other Apollo fields
                        const { key, value } = param;
                        return { key, value };
                    });
                }

                return cleanSetting;
            });

        return otherSettings;
    };

    const handleMarketingOptInChange = (checked: boolean) => {
        setMarketingOptIn(checked);
        setIsUpdatingMarketingSettings(true);

        const newSettings = [
            ...processedSettings(),
            {
                type: NotificationScenarioType.DataHubCommunityUpdates,
                value: checked ? NotificationSettingValue.Enabled : NotificationSettingValue.Disabled,
            },
        ];

        updateUserNotificationSettings({
            variables: {
                input: {
                    notificationSettings: {
                        settings: newSettings,
                    },
                },
            },
        })
            .then(() => {
                if (refetchNotificationSettings) {
                    refetchNotificationSettings();
                }

                // Show success toast using the same pattern as other notification settings
                message.destroy();
                message.success({
                    content: 'Preferences saved. This may take a few minutes to reflect in the system.',
                });

                onSuccess?.();
                setIsUpdatingMarketingSettings(false);
            })
            .catch((error) => {
                const errorMessage = `Failed to update marketing preferences: ${error.message || 'Unknown error'}`;
                console.error(errorMessage, {
                    error,
                    checked,
                    settingsCount: newSettings.length,
                    communityUpdatesExists: !!communityUpdatesSetting,
                });

                // Show toast notification using the same pattern as other notification settings
                message.destroy();
                if (error instanceof Error) {
                    message.error({
                        content: `Failed to update marketing preferences: \n ${error.message || ''}`,
                        duration: 3,
                    });
                }

                // Revert the state on error
                setMarketingOptIn(!checked);
                setIsUpdatingMarketingSettings(false);
                onError?.(error);
            });
    };

    return {
        marketingOptIn,
        isUpdatingMarketingSettings,
        handleMarketingOptInChange,
    };
};
