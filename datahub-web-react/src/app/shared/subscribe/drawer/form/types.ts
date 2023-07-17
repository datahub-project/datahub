import { Key } from 'react';
import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';

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

export type Action =
    | {
          type: 'initialize';
          payload: { slackSinkEnabled: boolean; entityType: EntityType; subscription?: DataHubSubscription };
      }
    | { type: 'toggleSlack'; payload: boolean };
