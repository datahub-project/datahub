import { useMemo } from 'react';

import { useDrawerState } from '@app/shared/subscribe/drawer/state/context';
import { ChannelSelections, State } from '@app/shared/subscribe/drawer/state/types';

import { NotificationSinkType } from '@types';

export function useDrawerSelector<T>(selector: (state: State) => T) {
    const state = useDrawerState();
    return useMemo(() => selector(state), [selector, state]);
}

export function selectIsEdited(state: State) {
    return state.edited;
}

export function selectSlack(state: State) {
    return state.slack;
}

export function selectEmail(state: State) {
    return state.email;
}

export function selectSlackSubscription(state: State) {
    return selectSlack(state).subscription;
}

export function selectEmailSubscription(state: State) {
    return selectEmail(state).subscription;
}

export function selectSettings(state: State) {
    return state.settings;
}

export function selectNotificationSinkTypes(state: State) {
    return state.notificationSinkTypes;
}

export function selectNotificationTypes(state: State) {
    return state.notificationTypes;
}

export function selectIsPersonal(state: State) {
    return state.isPersonal;
}

export function selectSlackSubscriptionChannel(state: State) {
    return selectSlackSubscription(state).channel;
}

export function selectSlackSettingsChannel(state: State) {
    return selectSettings(state).slack.channel;
}

export function selectSlackChannelSelection(state: State) {
    return selectSlack(state).channelSelection;
}

export function selectIsSlackSubscriptionChannelSelection(state: State) {
    return selectSlackChannelSelection(state) === ChannelSelections.SUBSCRIPTION;
}

export function selectIsSettingsChannelSelection(state: State) {
    return selectSlackChannelSelection(state) === ChannelSelections.SETTINGS;
}

export function selectHasSlackChannel(state: State) {
    return selectIsSlackSubscriptionChannelSelection(state) && !!selectSlackSubscriptionChannel(state);
}

export function selectSlackSaveAsDefault(state: State) {
    return selectSlackSubscription(state).saveAsDefault;
}

export function selectEmailSubscriptionChannel(state: State) {
    return selectEmailSubscription(state).channel;
}

export function selectEmailSettingsChannel(state: State) {
    return selectSettings(state).email.channel;
}

export function selectEmailChannelSelection(state: State) {
    return selectEmail(state).channelSelection;
}

export function selectIsEmailSubscriptionChannelSelection(state: State) {
    return selectEmailChannelSelection(state) === ChannelSelections.SUBSCRIPTION;
}

export function selectIsEmailSettingsChannelSelection(state: State) {
    return selectEmailChannelSelection(state) === ChannelSelections.SETTINGS;
}

export function selectHasEmail(state: State) {
    return selectIsEmailSubscriptionChannelSelection(state) && !!selectEmailSubscriptionChannel(state);
}

export function selectEmailSaveAsDefault(state: State) {
    return selectEmailSubscription(state).saveAsDefault;
}

export function selectIsSlackEnabled(state: State) {
    return selectSlack(state).enabled;
}

export function selectIsEmailEnabled(state: State) {
    return selectEmail(state).enabled;
}

export function selectCheckedKeys(state: State) {
    return selectNotificationTypes(state).checkedKeys;
}

export function selectExpandedKeys(state: State) {
    return selectNotificationTypes(state).expandedKeys;
}

export function selectKeysWithFilteringCleared(state: State) {
    return selectNotificationTypes(state).keysWithAllFilteringCleared;
}

// if the user's slack sink is disabled but they're enabling it for this susbcription, turn it back on for them
export function selectShouldTurnOnSlackInSettings(state: State) {
    const isSlackEnabled = selectIsSlackEnabled(state);
    const settings = selectSettings(state);
    return isSlackEnabled && !settings.sinkTypes?.includes(NotificationSinkType.Slack);
}

// if the user's email sink is disabled but they're enabling it for this susbcription, turn it back on for them
export function selectShouldTurnOnEmailInSettings(state: State) {
    const isEmailEnabled = selectIsEmailEnabled(state);
    const settings = selectSettings(state);
    return isEmailEnabled && !settings.sinkTypes?.includes(NotificationSinkType.Email);
}

export function selectShouldShowUpdateSlackSettingsWarning(state: State) {
    const shouldTurnOnSlackInSettings = selectShouldTurnOnSlackInSettings(state);
    return shouldTurnOnSlackInSettings;
}

export function selectShouldShowUpdateEmailSettingsWarning(state: State) {
    const shouldTurnOnEmailInSettings = selectShouldTurnOnEmailInSettings(state);
    return shouldTurnOnEmailInSettings;
}

export function selectHasEnabledSink(state: State) {
    return selectNotificationSinkTypes(state).length > 0;
}

export function selectHasNotificationType(state: State) {
    return selectCheckedKeys(state).length > 0;
}
