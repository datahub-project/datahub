import React, { useEffect, useState } from 'react';
import { Checkbox, Tooltip } from 'antd';
import styled from 'styled-components';

import { GlobalNotificationSettings, GlobalSettings, NotificationScenarioType } from '../../../../types.generated';
import { isSinkEnabled } from '../../utils';
import { isSinkNotificationTypeEnabled, updateSinkNotificationTypeEnabled } from './settingUtils';
import { FormattedNotificationSetting, NotificationSink } from '../types';
import { useUpdateGlobalNotificationSettingsMutation } from '../../../../graphql/settings.generated';

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
            {isSinkEnabled(sink.id, globalSettings) ? (
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
                    title={`${sink.name} integration is currently disabled! You can enable it inside Integrations settings.`}
                >
                    <Checkbox disabled />
                </Tooltip>
            )}
        </SettingValue>
    );
};
