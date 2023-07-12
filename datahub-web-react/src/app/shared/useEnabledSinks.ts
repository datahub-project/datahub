import { useGetGlobalSettingsQuery } from '../../graphql/settings.generated';
import { NOTIFICATION_SINKS, SLACK_SINK } from '../settings/platform/types';
import { isSinkEnabled } from '../settings/utils';

// todo - put this everywhere we can, ie grep for useGetGlobalSettingsQuery
const useEnabledSinks = () => {
    const { data: globalSettings } = useGetGlobalSettingsQuery();
    // todo - is the sink id capitalized or not?
    console.log(globalSettings);
    const enabledSinks = NOTIFICATION_SINKS.filter((sink) => isSinkEnabled(sink.id, globalSettings?.globalSettings));
    const slackSinkEnabled = enabledSinks.some((sink) => sink.id === SLACK_SINK.id);

    return { slackSinkEnabled } as const;
};

export default useEnabledSinks;
