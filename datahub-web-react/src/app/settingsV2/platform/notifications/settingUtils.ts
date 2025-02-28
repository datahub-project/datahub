import { message } from 'antd';
import {
    GlobalNotificationSettings,
    NotificationScenarioType,
    NotificationSettingValue,
    StringMapEntry,
} from '../../../../types.generated';
import { EMAIL_SINK, FormattedNotificationSetting, SLACK_SINK } from '../types';
import { UpdateGlobalNotificationSettingsMutationFn } from '../../../../graphql/settings.generated';

export const SLACK_CHANNEL_PARAM_NAME = `${SLACK_SINK.id}.channel`;
export const EMAIL_ADDRESS_PARAM_NAME = `${EMAIL_SINK.id}.address`;

// TODO: Move these utilities to a global file.
export const paramsMapToArray = (params: Map<string, string>) => {
    const paramsArray = Array<StringMapEntry>();
    params.forEach((value, key) => {
        paramsArray.push({
            key,
            value,
        });
    });
    return paramsArray;
};

export const paramsArrayToMap = (params: Array<StringMapEntry>) => {
    const paramsMap = new Map();
    params.forEach((entry) => {
        paramsMap.set(entry.key, entry.value);
    });
    return paramsMap;
};

export const isParamPresent = (params: Map<string, string>, key: string, value: string): boolean => {
    return params.get(key) === value;
};

export const isSinkNotificationTypeEnabled = (sinkId, setting?: FormattedNotificationSetting | null) => {
    return (
        setting?.value === NotificationSettingValue.Enabled &&
        isParamPresent(setting.params, `${sinkId}.enabled`, 'true')
    );
};

/**
 * Simply converts the backend GraphQL array-based settings into a map for easier access.
 */
export const buildNotificationSettingsMap = (
    settings?: GlobalNotificationSettings | null,
): Map<NotificationScenarioType, FormattedNotificationSetting> => {
    const notificationSettings = new Map();
    settings?.settings?.forEach((setting) => {
        notificationSettings.set(setting.type, {
            type: setting.type,
            value: setting.value,
            params: paramsArrayToMap(setting.params || []),
        });
    });
    return notificationSettings;
};

export const updateSinkNotificationType = (
    type: NotificationScenarioType,
    value: NotificationSettingValue,
    params: Map<string, string>,
    refetch: () => void,
    updateGlobalNotificationSettings: UpdateGlobalNotificationSettingsMutationFn,
) => {
    updateGlobalNotificationSettings({
        variables: {
            input: {
                settings: [
                    {
                        value,
                        type,
                        params: paramsMapToArray(params),
                    },
                ],
            },
        },
    })
        .then(() => {
            refetch();
            message.destroy();
            message.success({ content: 'Preferences saved. This may take a few minutes to reflect in the system.' });
        })
        .catch((e: unknown) => {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update settings. An unknown error occurred.`, duration: 3 });
            }
        });
};

export const updateNotificationTypeParams = (
    type: NotificationScenarioType,
    params: Array<StringMapEntry>,
    refetch: () => void,
    updateGlobalNotificationSettings: UpdateGlobalNotificationSettingsMutationFn,
    existingNotificationSettings?: Partial<GlobalNotificationSettings>,
) => {
    const maybeCurrentTypeSettings = existingNotificationSettings?.settings?.find((setting) => setting.type === type);

    let currentTypeSettings;
    if (maybeCurrentTypeSettings) {
        const currentParams = paramsArrayToMap(maybeCurrentTypeSettings?.params || []);
        currentTypeSettings = {
            type,
            value: maybeCurrentTypeSettings.value,
            params: currentParams,
        };
    } else {
        currentTypeSettings = {
            type,
            value: NotificationSettingValue.Enabled,
            params: new Map(),
        };
    }

    params.forEach((value) => {
        if (value.value) {
            currentTypeSettings.params.set(value.key, value.value);
        } else {
            currentTypeSettings.params.delete(value.key);
        }
    });

    // Finally, update the current options for the notification type.
    updateSinkNotificationType(
        currentTypeSettings.type,
        currentTypeSettings.value,
        currentTypeSettings.params,
        refetch,
        updateGlobalNotificationSettings,
    );
};

export const removeNotificationTypeParams = (
    type: NotificationScenarioType,
    params: Array<string>,
    refetch: () => void,
    updateGlobalNotificationSettings: UpdateGlobalNotificationSettingsMutationFn,
    existingNotificationSettings?: Partial<GlobalNotificationSettings>,
) => {
    const maybeCurrentTypeSettings = existingNotificationSettings?.settings?.find((setting) => setting.type === type);

    let currentTypeSettings;
    if (maybeCurrentTypeSettings) {
        const currentParams = paramsArrayToMap(maybeCurrentTypeSettings?.params || []);
        currentTypeSettings = {
            type,
            value: maybeCurrentTypeSettings.value,
            params: currentParams,
        };
    } else {
        currentTypeSettings = {
            type,
            value: NotificationSettingValue.Enabled,
            params: new Map(),
        };
    }

    // Remove each param
    params.forEach((value) => {
        currentTypeSettings.params.remove(value);
    });

    // Finally, update the current options for the notification type.
    updateSinkNotificationType(
        currentTypeSettings.type,
        currentTypeSettings.value,
        currentTypeSettings.params,
        refetch,
        updateGlobalNotificationSettings,
    );
};

export const updateSinkNotificationTypeEnabled = (
    sinkId,
    type: NotificationScenarioType,
    enabled: boolean,
    refetch: () => void,
    updateGlobalNotificationSettings: UpdateGlobalNotificationSettingsMutationFn,
    existingNotificationSettings?: Partial<GlobalNotificationSettings>,
) => {
    const newParam = {
        key: `${sinkId}.enabled`,
        value: enabled ? 'true' : 'false',
    };
    updateNotificationTypeParams(
        type,
        [newParam],
        refetch,
        updateGlobalNotificationSettings,
        existingNotificationSettings,
    );
};
