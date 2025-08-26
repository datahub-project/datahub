import { Tooltip } from '@components';
import { Checkbox } from 'antd';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { FormattedNotificationSetting, NotificationSink } from '@app/settingsV2/notifications/types';
import { isSinkNotificationTypeEnabled, updateSinkNotificationTypeEnabled } from '@app/settingsV2/notifications/utils';
import { EMAIL_SINK } from '@src/app/settings/platform/types';

import { NotificationScenarioType, NotificationSetting } from '@types';

const SettingValue = styled.div`
    width: 64px;
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    sink: NotificationSink;
    disabled: boolean;
    notificationType: NotificationScenarioType;
    existingNotificationSettings: Map<NotificationScenarioType, FormattedNotificationSetting>;
    refetch: () => void;
    updateNotificationSettings: (settings: NotificationSetting[]) => any;
    originalSettings?: NotificationSetting[];
    enabledByDefault?: boolean;
};

export const NotificationSettingValue = ({
    sink,
    disabled,
    notificationType,
    existingNotificationSettings,
    refetch,
    updateNotificationSettings,
    originalSettings,
    enabledByDefault,
}: Props) => {
    const [selected, setSelected] = useState(() =>
        isSinkNotificationTypeEnabled(sink.id, existingNotificationSettings.get(notificationType), enabledByDefault),
    );

    useEffect(() => {
        // This effect will run on mount and whenever any of the dependencies change.
        const newSelected = isSinkNotificationTypeEnabled(
            sink.id,
            existingNotificationSettings.get(notificationType),
            enabledByDefault,
        );
        setSelected(newSelected);
    }, [sink, notificationType, existingNotificationSettings, enabledByDefault]);

    return (
        <SettingValue key={`${notificationType}-${sink.id}`}>
            {!disabled ? (
                <Checkbox
                    data-testid={`notification-type-${sink.id}-${notificationType.toLowerCase()}`}
                    checked={selected}
                    onChange={(e) => {
                        setSelected(e.target.checked); // Immediately mark as selected.
                        updateSinkNotificationTypeEnabled(
                            sink.id,
                            notificationType,
                            e.target.checked,
                            refetch,
                            updateNotificationSettings,
                            originalSettings,
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
