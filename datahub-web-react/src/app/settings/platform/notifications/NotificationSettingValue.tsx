import { Tooltip } from '@components';
import { Checkbox } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import {
    isSinkNotificationTypeEnabled,
    updateSinkNotificationTypeEnabled,
} from '@app/settings/platform/notifications/settingUtils';
import { EMAIL_SINK, FormattedNotificationSetting, NotificationSink } from '@app/settings/platform/types';
import { isSinkEnabled } from '@app/settings/utils';
import { useAppConfig } from '@app/useAppConfig';

import { useUpdateGlobalNotificationSettingsMutation } from '@graphql/settings.generated';
import { GlobalNotificationSettings, GlobalSettings, NotificationScenarioType } from '@types';

const SettingValue = styled.div`
    width: 64px;
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    sink: NotificationSink;
    notificationType: NotificationScenarioType;
    existingNotificationSettings: Map<NotificationScenarioType, FormattedNotificationSetting>;
    refetch: () => void;
    globalSettings?: GlobalSettings;
};

export const NotificationSettingValue = ({
    sink,
    notificationType,
    existingNotificationSettings,
    refetch,
    globalSettings,
}: Props) => {
    const { config } = useAppConfig();
    const [updateGlobalNotificationSettings] = useUpdateGlobalNotificationSettingsMutation();
    const globalNotificationSettings = globalSettings?.notificationSettings as GlobalNotificationSettings;

    const [selected, setSelected] = useState(() =>
        isSinkNotificationTypeEnabled(sink.id, existingNotificationSettings.get(notificationType)),
    );

    useEffect(() => {
        // This effect will run on mount and whenever any of the dependencies change.
        const newSelected = isSinkNotificationTypeEnabled(sink.id, existingNotificationSettings.get(notificationType));
        setSelected(newSelected);
    }, [sink, notificationType, existingNotificationSettings]);

    return (
        <SettingValue key={`${notificationType}-${sink.id}`}>
            {isSinkEnabled(sink.id, globalSettings, config) ? (
                <Checkbox
                    checked={selected}
                    onChange={(e) => {
                        setSelected(e.target.checked); // Immediately mark as selected.
                        updateSinkNotificationTypeEnabled(
                            sink.id,
                            notificationType,
                            e.target.checked,
                            refetch,
                            updateGlobalNotificationSettings,
                            globalNotificationSettings,
                        );
                    }}
                />
            ) : (
                <Tooltip
                    title={`${sink.name} notifications are currently disabled! ${
                        sink.id === EMAIL_SINK.id
                            ? 'Contact your Acryl representative for more details.'
                            : `You can enable it inside Integrations settings.`
                    }`}
                >
                    <Checkbox disabled />
                </Tooltip>
            )}
        </SettingValue>
    );
};
