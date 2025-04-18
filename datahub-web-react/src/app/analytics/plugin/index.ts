import amplitude from '@app/analytics/plugin/amplitude';
import datahub from '@app/analytics/plugin/datahub';
import googleAnalytics from '@app/analytics/plugin/googleAnalytics';
import logger from '@app/analytics/plugin/logger';
import mixpanel from '@app/analytics/plugin/mixpanel';

export default [googleAnalytics, mixpanel, amplitude, datahub, logger];
