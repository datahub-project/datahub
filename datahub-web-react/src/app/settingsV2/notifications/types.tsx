import { NotificationScenarioType, NotificationSettingValue, NotificationSinkType } from '@src/types.generated';
import { SLACK_INTEGRATION } from '../platform/types';

/**
 * Types shared between Platform & Actor notifications.
 */
export type NotificationSink = {
    id: string;
    name: string;
    img?: any;
    options: boolean;
};

export const SLACK_SINK = {
    type: NotificationSinkType.Slack,
    id: SLACK_INTEGRATION.id,
    name: SLACK_INTEGRATION.name,
    img: SLACK_INTEGRATION.img,
    options: true,
};

export const EMAIL_SINK = {
    type: NotificationSinkType.Email,
    id: 'email',
    name: 'Email',
    img: undefined,
    options: true,
};

export const NOTIFICATION_SINKS = [SLACK_SINK, EMAIL_SINK];

export type FormattedNotificationSetting = {
    type: NotificationScenarioType;
    value: NotificationSettingValue;
    params: Map<string, string>;
};

export type NotificationTypeOptions = {
    slackChannel: string | null;
    email: string | null;
};

export type NotificationGroup = {
    title: string;
    notifications: {
        type: NotificationScenarioType;
        description: string;
    }[];
};
