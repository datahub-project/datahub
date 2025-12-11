/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import amplitude from '@app/analytics/plugin/amplitude';
import datahub from '@app/analytics/plugin/datahub';
import googleAnalytics from '@app/analytics/plugin/googleAnalytics';
import logger from '@app/analytics/plugin/logger';
import mixpanel from '@app/analytics/plugin/mixpanel';

export default [googleAnalytics, mixpanel, amplitude, datahub, logger];
