import mixpanel from '@analytics/mixpanel';
import analyticsConfig from '../../../conf/analytics';

const mixpanelConfigs = analyticsConfig.mixpanel;
const isEnabled: boolean = mixpanelConfigs || false;
const token = isEnabled ? mixpanelConfigs.token : undefined;

export const getMixpanelPlugin = (t: string) => {
    // Mixpanel does not really have a page view event.
    // (Technically they have $mp_web_page_view as per
    // https://github.com/mixpanel/mixpanel-js/blob/41e7958af75263a1dc00e960952c27ca655579b2/src/mixpanel-core.js#L1269
    // but this isn't meant for user generation).
    //
    // However, the "analytics" package that we use has a built-in page view event
    // that we hook into. By default, it uses the page path as the event name, but
    // this really doesn't work for us and instead just pollutes the event stream
    // with tons of junk event names.
    // See https://github.com/DavidWells/analytics/blob/master/packages/analytics-plugin-mixpanel/src/browser.js#L141-L147
    //
    // Here we're overriding the pageEvent name to be PageViewEvent.
    // This is redundant, since we already have a PageViewEvent and also
    // have HomePageViewEvent, EntityViewEvent, EntitySectionViewEvent, etc.
    // But this gets the job done for now.
    return mixpanel({ token: t, pageEvent: 'PageViewEvent' });
};

export default {
    isEnabled,
    plugin: isEnabled && getMixpanelPlugin(token),
};
