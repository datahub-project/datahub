import { NotificationSinkType } from '../../../../../types.generated';
import { State } from './types';

// if the user's slack sink is disabled but they're enabling it for this susbcription, turn it back on for them
export function shouldTurnOnSlackInSettings(state: State) {
    return state.slack.enabled && !state.settings.sinkTypes?.includes(NotificationSinkType.Slack);
}

export function shouldShowUpdateSlackSettingsWarning(state: State) {
    return shouldTurnOnSlackInSettings(state) && !!state.slack.settings.channel;
}
