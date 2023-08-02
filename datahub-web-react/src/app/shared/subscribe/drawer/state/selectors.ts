import { useMemo } from 'react';
import { NotificationSinkType } from '../../../../../types.generated';
import { useDrawerState } from './context';
import { State } from './types';

export function useDrawerSelector<T>(selector: (state: State) => T) {
    const state = useDrawerState();
    return useMemo(() => selector(state), [selector, state]);
}

function selectSlack(state: State) {
    return state.slack;
}

function selectSlackSubscription(state: State) {
    return selectSlack(state).subscription;
}

function selectSettings(state: State) {
    return state.settings;
}

function selectNotificationSinkTypes(state: State) {
    return state.notificationSinkTypes;
}

function selectNotificationTypes(state: State) {
    return state.notificationTypes;
}

export function selectSlackChannel(state: State) {
    return selectSlackSubscription(state).channel;
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
    const settings = selectSettings(state);
    return shouldTurnOnSlackInSettings && !!settings.slack.channel;
}

export function selectHasEnabledSink(state: State) {
    return selectNotificationSinkTypes(state).length > 0;
}
