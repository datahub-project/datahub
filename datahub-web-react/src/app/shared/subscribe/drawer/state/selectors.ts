import { useMemo } from 'react';
import { NotificationSinkType } from '../../../../../types.generated';
import { useDrawerState } from './context';
import { ChannelSelections, State } from './types';

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

export function selectSlackSubscription(state: State) {
    return selectSlack(state).subscription;
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

export function selectSubscriptionSlackChannel(state: State) {
    return selectSlackSubscription(state).channel;
}

export function selectSettingsSlackChannel(state: State) {
    return selectSettings(state).slack.channel;
}

export function selectChannelSelection(state: State) {
    return selectSlack(state).channelSelection;
}

export function selectIsSubscriptionChannelSelection(state: State) {
    return selectChannelSelection(state) === ChannelSelections.SUBSCRIPTION;
}

export function selectIsSettingsChannelSelection(state: State) {
    return selectChannelSelection(state) === ChannelSelections.SETTINGS;
}

export function selectHasSlackChannel(state: State) {
    return selectIsSubscriptionChannelSelection(state) && !!selectSubscriptionSlackChannel(state);
}

export function selectSlackSaveAsDefault(state: State) {
    return selectSlackSubscription(state).saveAsDefault;
}

export function selectIsSlackEnabled(state: State) {
    return selectSlack(state).enabled;
}

export function selectCheckedKeys(state: State) {
    return selectNotificationTypes(state).checkedKeys;
}

export function selectExpandedKeys(state: State) {
    return selectNotificationTypes(state).expandedKeys;
}

// if the user's slack sink is disabled but they're enabling it for this susbcription, turn it back on for them
export function selectShouldTurnOnSlackInSettings(state: State) {
    const isSlackEnabled = selectIsSlackEnabled(state);
    const settings = selectSettings(state);
    return isSlackEnabled && !settings.sinkTypes?.includes(NotificationSinkType.Slack);
}

export function selectShouldShowUpdateSlackSettingsWarning(state: State) {
    const shouldTurnOnSlackInSettings = selectShouldTurnOnSlackInSettings(state);
    const settingsSlackChannel = selectSettingsSlackChannel(state);
    return shouldTurnOnSlackInSettings && !!settingsSlackChannel;
}

export function selectHasEnabledSink(state: State) {
    return selectNotificationSinkTypes(state).length > 0;
}

export function selectHasNotificationType(state: State) {
    return selectCheckedKeys(state).length > 0;
}
