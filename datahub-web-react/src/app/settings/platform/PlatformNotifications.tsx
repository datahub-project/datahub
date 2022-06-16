import React, { useMemo, useState } from 'react';
import { Divider, Typography, Card, Image, Checkbox, Tooltip, Alert, message } from 'antd';
import styled from 'styled-components';
import { NotificationTypeOptionsButton } from './NotificationTypeOptionButton';
import { PlatformNotificationOptionsModal } from './PlatformNotificationOptionsModal';
import {
    FormattedNotificationSetting,
    NOTIFICATION_GROUPS,
    NOTIFICATION_SINKS,
    PlatformNotificationOptions,
    SLACK_SINK,
} from './types';
import {
    useGetGlobalSettingsQuery,
    useUpdateGlobalNotificationSettingsMutation,
} from '../../../graphql/settings.generated';
import {
    GlobalNotificationSettings,
    GlobalSettings,
    NotificationScenarioType,
    NotificationSettingValue,
    StringMapEntry,
} from '../../../types.generated';
import { Message } from '../../shared/Message';

const Page = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const ContentContainer = styled.div`
    padding-top: 40px;
    padding-right: 40px;
    padding-left: 40px;
    padding-bottom: 40px;
    width: 80%;
`;

const SettingsHeader = styled.div`
    width: 100%;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SettingsTitle = styled(Typography.Text)`
    font-size: 18px;
`;

const SettingsSection = styled.div``;

const OptionsPlaceholder = styled.div`
    width: 66px;
`;

const Setting = styled.div`
    padding-left: 12px;
    padding-top: 6px;
    padding-bottom: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
`;

const SettingValues = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

const SettingValue = styled.div`
    width: 64px;
    display: flex;
    justify-content: center;
    align-items: center;
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

const SLACK_CHANNEL_PARAM_NAME = `${SLACK_SINK.id}.channel`;

const isSinkEnabled = (sinkId, settings?: GlobalSettings | null) => {
    switch (sinkId) {
        case SLACK_SINK.id: {
            return settings?.integrationSettings?.slackSettings?.enabled || false;
        }
        default:
            return false;
    }
};

const paramsMapToArray = (params: Map<string, string>) => {
    const paramsArray = Array<StringMapEntry>();
    params.forEach((value, key) => {
        paramsArray.push({
            key,
            value,
        });
    });
    return paramsArray;
};

const paramsArrayToMap = (params: Array<StringMapEntry>) => {
    const paramsMap = new Map();
    params.forEach((entry) => {
        paramsMap.set(entry.key, entry.value);
    });
    return paramsMap;
};

const hasParam = (params: Map<string, string>, key: string, value: string): boolean => {
    return params.get(key) === value;
};

const isSinkNotificationTypeEnabled = (sinkId, setting?: FormattedNotificationSetting | null) => {
    return (
        setting?.value === NotificationSettingValue.Enabled && !hasParam(setting.params, `${sinkId}.enabled`, 'false')
    );
};

const buildFormattedNotificationSettings = (
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

export const PlatformNotifications = () => {
    // Determine which notification sinks are enabled
    const [showNotificationOptions, setShowNotificationOptions] = useState(false);
    const [focusedNotificationType, setFocusedNotificationType] =
        useState<NotificationScenarioType | undefined>(undefined);

    const { data, loading, error, refetch } = useGetGlobalSettingsQuery();
    const [updateGlobalNotificationSettings] = useUpdateGlobalNotificationSettingsMutation();
    const globalSettings = data?.globalSettings;
    const globalNotificationSettings = globalSettings?.notificationSettings;
    const formattedNotificationSettings = useMemo(
        () => buildFormattedNotificationSettings(globalNotificationSettings),
        [globalNotificationSettings],
    );

    const openNotificationOptions = (type: NotificationScenarioType) => {
        setShowNotificationOptions(true);
        setFocusedNotificationType(type);
    };

    const closeNotificationOptions = () => {
        setShowNotificationOptions(false);
        setFocusedNotificationType(undefined);
    };

    const updateSinkNotificationType = (
        type: NotificationScenarioType,
        value: NotificationSettingValue,
        params: Map<string, string>,
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
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to update settings: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const addNotificationTypeParams = (type: NotificationScenarioType, params: Array<StringMapEntry>) => {
        const maybeCurrentTypeSettings = globalNotificationSettings?.settings?.find((setting) => setting.type === type);

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

        // Add each param
        params.forEach((value) => {
            currentTypeSettings.params.set(value.key, value.value);
        });

        // Finally, update the current options for the notification type.
        updateSinkNotificationType(currentTypeSettings.type, currentTypeSettings.value, currentTypeSettings.params);
    };

    const removeNotificationTypeParams = (type: NotificationScenarioType, params: Array<string>) => {
        const maybeCurrentTypeSettings = globalNotificationSettings?.settings?.find((setting) => setting.type === type);

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
        updateSinkNotificationType(currentTypeSettings.type, currentTypeSettings.value, currentTypeSettings.params);
    };

    const updateSinkNotificationTypeEnabled = (sinkId, type: NotificationScenarioType, enabled: boolean) => {
        const newParam = {
            key: `${sinkId}.enabled`,
            value: enabled ? 'true' : 'false',
        };
        addNotificationTypeParams(type, [newParam]);
    };

    const updateNotificationTypeOptions = (type: NotificationScenarioType, options: PlatformNotificationOptions) => {
        // Insert updated parameters into param map.
        if (options.slackChannel) {
            if (options.slackChannel.length) {
                // Insert slack channel parameter:
                addNotificationTypeParams(type, [{ key: SLACK_CHANNEL_PARAM_NAME, value: options.slackChannel }]);
            } else {
                removeNotificationTypeParams(type, [SLACK_CHANNEL_PARAM_NAME]);
            }
        }
        closeNotificationOptions();
    };

    const getDefaultNotificationTypeOptions = (type: NotificationScenarioType) => {
        const currSlackChannel = formattedNotificationSettings.get(type)?.params?.get(SLACK_CHANNEL_PARAM_NAME);
        return {
            slackChannel: currSlackChannel,
        };
    };

    /**
     * A list of the enabled notification sinks. Sinks are destinations
     * to which notifications are routed.
     */
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, data?.globalSettings));

    /**
     * Only show notification options button if relevant sink is enabled.
     */
    const notificationOptionsEnabled = NOTIFICATION_SINKS.some(
        (sink) => sink.options && isSinkEnabled(sink.id, data?.globalSettings),
    );

    return (
        <Page>
            <ContentContainer>
                {loading && (
                    <Message type="loading" content="Loading notification settings..." style={{ marginTop: '10%' }} />
                )}
                {error && <Alert type="error" message={error?.message || `Failed to load notification settings!`} />}
                <Typography.Title level={3}>Notifications</Typography.Title>
                <Typography.Text type="secondary">Select when and where global notifications are sent</Typography.Text>
                <Divider />
                <Card>
                    <SettingsHeader>
                        <SettingsTitle>Send a notification when...</SettingsTitle>
                        <NotificationSinkHeaders>
                            <NotificationSinkHeader>
                                {NOTIFICATION_SINKS.map((sink) => (
                                    <span key={sink.id}>
                                        <Image preview={false} src={sink.img} width={12} />
                                        <NotificationSinkName strong>{sink.name}</NotificationSinkName>
                                    </span>
                                ))}
                            </NotificationSinkHeader>
                            <OptionsPlaceholder />
                        </NotificationSinkHeaders>
                    </SettingsHeader>
                    <Divider />
                    {NOTIFICATION_GROUPS.map((group, index) => (
                        <span key={group.title}>
                            <SettingsSection>
                                <Typography.Title level={5}>{group.title}</Typography.Title>
                                {group.notifications.map((notif) => (
                                    <Setting key={notif.type}>
                                        <Typography.Text>{notif.description}</Typography.Text>
                                        <SettingValues>
                                            {NOTIFICATION_SINKS.map((sink) => (
                                                <SettingValue key={`${notif.type}-${sink.id}`}>
                                                    {isSinkEnabled(sink.id, data?.globalSettings) ? (
                                                        <Checkbox
                                                            checked={isSinkNotificationTypeEnabled(
                                                                sink.id,
                                                                formattedNotificationSettings.get(notif.type),
                                                            )}
                                                            onChange={(e) =>
                                                                updateSinkNotificationTypeEnabled(
                                                                    sink.id,
                                                                    notif.type as NotificationScenarioType,
                                                                    e.target.checked,
                                                                )
                                                            }
                                                        />
                                                    ) : (
                                                        <Tooltip
                                                            title={`${sink.name} integration is currently disabled! You can enable it inside Integrations settings.`}
                                                        >
                                                            <Checkbox checked disabled />
                                                        </Tooltip>
                                                    )}
                                                </SettingValue>
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
                            {index < NOTIFICATION_GROUPS.length - 1 && <Divider />}
                        </span>
                    ))}
                </Card>
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
            </ContentContainer>
        </Page>
    );
};
