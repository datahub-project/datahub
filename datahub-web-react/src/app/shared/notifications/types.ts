import { SlackNotificationSettings } from '@src/types.generated';

export type TestNotificationConfig = TestNotificationConfigSlack; /* | TestNotificationConfigXXX */

type TestNotificationConfigSlack = TestNotificationConfigBase & {
    // differentiator key present across all TestNotificationConfig types
    integration: 'slack';
    // properties specific to this config type
    destinationSettings: SlackNotificationSettings;
};

type TestNotificationConfigBase = {
    connectionUrn: string;
};

export type NotificationConnectionTestResult = {
    status: string;
    report: NotificationConnectionTestStructuredReport;
};
export type NotificationConnectionTestStructuredReport = [
    {
        timestamp?: number;
        error?: string;
        message?: string;
        warning?: string;
    },
];

export type ExtraContextForErrorMessage = {
    destinationName: string;
};
