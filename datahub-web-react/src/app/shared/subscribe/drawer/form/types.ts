import { Key } from 'react';
import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';

// todo - declare a FormState type
// in our context we have both a base, and a modified FormState
// modiifed is a straight copy, with overrides, and a boolean for edited?
// maybe we just keep a bool and can reset it

// what about the gql values, do we even need that in state?

export type State = {
    edited: boolean;
    isPersonal: boolean;
    checkedKeys: Array<Key>;
    subscribeToUpstream: boolean;
    notificationSinkTypes: Array<NotificationSinkType>;
    slack: {
        enabled: boolean;
        channelSelection: 'settings' | 'subscription';
        settings: {
            channel?: string;
        };
        subscription: {
            channel?: string;
            saveAsDefault: boolean;
        };
    };
};

export type Action =
    | {
          type: 'initialize';
          payload: {
              slackSinkEnabled: boolean;
              entityType: EntityType;
              subscription?: DataHubSubscription;
              subscriptionChannel?: string;
              settingsChannel?: string;
          };
      }
    | { type: 'setSlackEnabled'; payload: boolean }
    | { type: 'setSubscriptionChannel'; payload?: string }
    | { type: 'setSaveAsDefault'; payload: boolean }
    | { type: 'setCheckedKeys'; payload: Array<Key> }
    | { type: 'setSubscribeToUpstream'; payload: boolean }
    | { type: 'setChannelSelection'; payload: State['slack']['channelSelection'] };
