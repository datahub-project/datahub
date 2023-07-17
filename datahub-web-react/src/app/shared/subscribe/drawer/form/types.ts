import { Key } from 'react';
import { NotificationSinkType } from '../../../../../types.generated';

export type State = {
    enabled: boolean;
    checkedKeys: Array<Key>;
    subscribeToUpstream: boolean;
    notificationSinkTypes: Array<NotificationSinkType>;
    slack: {
        enabled: boolean;
        saveAsDefault: boolean;
        customSlackSink?: string;
    };
};

export type Action = { type: 'toggleSlack'; payload: boolean };
