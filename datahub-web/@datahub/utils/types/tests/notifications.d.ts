import { TestContext } from 'ember-test-helpers';
import Notifications from '@datahub/utils/services/notifications';

/**
 * Test context interface definition for notifications tests
 * @export
 * @extends {TestContext}
 */
export interface INotificationsTestContext extends TestContext {
  service: Notifications;
}
