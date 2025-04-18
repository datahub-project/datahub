import React, { useEffect, useState } from 'react';
import { Checkbox } from 'antd';
import { Tooltip } from '@components';
import { EMAIL_SINK } from '@src/app/settings/platform/types';
import styled from 'styled-components';
import { NotificationScenarioType, NotificationSetting } from '../../../types.generated';
import { FormattedNotificationSetting, NotificationSink } from './types';
import { isSinkNotificationTypeEnabled, updateSinkNotificationTypeEnabled } from './utils';

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
};

export const NotificationSettingValue = ({
    sink,
    disabled,
    notificationType,
    existingNotificationSettings,
    refetch,
    updateNotificationSettings,
    originalSettings,
}: Props) => {
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
